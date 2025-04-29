package internal

import (
	"database/sql"
	"io/fs"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/boyter/scc/v3/processor"
	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-billy/v5/util"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"

	// TODO: Try to get arrow support working
	_ "github.com/marcboeker/go-duckdb/v2"
)

func initDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", "codescene.db")
	if err != nil {
		return nil, err
	}

	createTablesStmt := `
				CREATE TABLE IF NOT EXISTS commits (
					hash TEXT PRIMARY KEY UNIQUE,
					contributor TEXT NOT NULL,
					author_date INTEGER NOT NULL,
					project TEXT NOT NULL
				);
				CREATE TABLE IF NOT EXISTS filestates (
					id INTEGER PRIMARY KEY,
					commit_hash TEXT NOT NULL REFERENCES commits(hash),
					path TEXT NOT NULL,
					language TEXT NOT NULL,
					sloc INTEGER NOT NULL,
					cloc INTEGER NOT NULL,
					blank INTEGER NOT NULL,
					lines_added INTEGER NOT NULL,
					lines_removed INTEGER NOT NULL,
					complexity INTEGER NOT NULL,
					blame_authors TEXT[] NOT NULL,
					blame_dates INTEGER[] NOT NULL
				);` // TODO: Add array for blame
	_, err = db.Exec(createTablesStmt)
	if err != nil {
		return nil, err
	}

	return db, nil
}

type FileState struct {
	CommitId int64
	Commit   *object.Commit
	*processor.FileJob
}

func Analyze() error {
	db, err := initDB()
	if err != nil {
		return err
	}
	defer db.Close()

	repoUrl := "https://github.com/go-git/go-billy"
	filesystem := memfs.New()
	r, err := git.Clone(memory.NewStorage(), filesystem, &git.CloneOptions{
		URL: repoUrl,
	})

	if err != nil {
		return err
	}

	ref, err := r.Head()

	if err != nil {
		return err
	}

	cIter, err := r.Log(&git.LogOptions{From: ref.Hash()})

	if err != nil {
		return err
	}

	worktree, err := r.Worktree()
	if err != nil {
		return err
	}

	files := make(chan FileState, runtime.NumCPU())

	cIter.ForEach(func(c *object.Commit) error {
		res, err := db.Exec("INSERT INTO commits (hash, contributor, author_date, project) VALUES (?, ?, ?, ?)",
			c.Hash.String(),
			c.Author.Name,
			c.Author.When.Unix(),
			repoUrl,
		)

		if err != nil && strings.Contains(err.Error(), "Duplicate key") {
			return nil
		} else if err != nil {
			return err
		}

		commitId, err := res.LastInsertId()
		if err != nil {
			return err
		}

		if err := worktree.Checkout(&git.CheckoutOptions{
			Hash: c.Hash,
		}); err != nil {
			return err
		}

		processor.ProcessConstants()

		go findFiles(filesystem, commitId, c, files)

		if err = process(filesystem, files); err != nil {
			return err
		}

		return nil
	})

	close(files)

	return nil
}

func findFiles(filesystem billy.Filesystem, commitId int64, commit *object.Commit, files chan FileState) error {
	err := util.Walk(filesystem, ".", func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			// Not interested in directories
			return nil
		}
		if err != nil {
			// Also not interested in handling errors passed to the function specifically
			return nil
		}
		f := newFileJob(path, info)
		if f == nil {
			return err
		}
		files <- FileState{
			CommitId: commitId,
			Commit:   commit,
			FileJob:  f,
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

var LargeByteCount int64 = 1000000

func newFileJob(path string, info fs.FileInfo) *processor.FileJob {
	if info.Size() >= LargeByteCount {
		return nil
	}

	if info.Mode()&os.ModeSymlink == os.ModeSymlink {
		return nil
	}

	if !info.Mode().IsRegular() {
		return nil
	}

	language, extension := processor.DetectLanguage(path)

	if len(language) != 0 {
		for _, l := range language {
			processor.LoadLanguageFeature(l)
		}

		for _, l := range language {
			if l == "ignore" || l == "gitignore" {
				return nil
			}
		}

		return &processor.FileJob{
			Location:          path,
			Filename:          path,
			Extension:         extension,
			PossibleLanguages: language,
			Bytes:             info.Size(),
			Content:           make([]byte, info.Size()),
		}
	}

	return nil
}

func process(filesystem billy.Filesystem, files chan FileState) error {
	var wg sync.WaitGroup

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			for file := range files {
				loc := file.Location
				f, err := filesystem.Open(loc)
				if err != nil {
					continue
				}
				_, err = f.Read(file.Content)
				if err != nil {
					continue
				}
				processFile(file)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return nil
}

func processFile(file FileState) {
	file.Language = processor.DetermineLanguage(file.Filename, file.Language, file.PossibleLanguages, file.Content)
	if file.Language == processor.SheBang {

		cutoff := 200

		// To avoid runtime panic check if the content we are cutting is smaller than 200
		if len(file.Content) < cutoff {
			cutoff = len(file.Content)
		}

		lang, err := processor.DetectSheBang(string(file.Content[:cutoff]))
		if err != nil {
			return
		}

		file.Language = lang
		processor.LoadLanguageFeature(lang)
	}

	processor.CountStats(file.FileJob)

	if file.Binary {
		return
	}

	// TODO: add to duckdb
	// TODO: Could improve performance by configuring gc
}
