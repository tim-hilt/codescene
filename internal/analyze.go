package internal

import (
	"context"
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
	"github.com/rs/zerolog/log"

	// TODO: Try to get arrow support working
	"github.com/marcboeker/go-duckdb/v2"
)

func initDB() (*sql.DB, *duckdb.Appender, error) {
	c, err := duckdb.NewConnector("codescene.db", nil)
	if err != nil {
		return nil, nil, err
	}

	con, err := c.Connect(context.Background())
	if err != nil {
		return nil, nil, err
	}

	db := sql.OpenDB(c)
	if _, err := db.Exec(`CREATE TABLE users (name VARCHAR, age INTEGER)`); err != nil {
		return nil, nil, err
	}

	createTablesStmt := `
				CREATE TABLE IF NOT EXISTS commits (
					hash TEXT PRIMARY KEY UNIQUE,
					contributor TEXT NOT NULL,
					author_date INTEGER NOT NULL,
					project TEXT NOT NULL
				);
				CREATE TABLE IF NOT EXISTS filestates (
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
				);`
	_, err = db.Exec(createTablesStmt)
	if err != nil {
		return nil, nil, err
	}

	a, err := duckdb.NewAppenderFromConn(con, "", "filestates")
	if err != nil {
		return nil, nil, err
	}

	return db, a, nil
}

type FileState struct {
	CommitId int64
	Commit   *object.Commit
	*processor.FileJob
}

func Analyze() error {
	db, appender, err := initDB()
	if err != nil {
		return err
	}

	// TODO: Add "force" option to just reprocess all commits -> Might be useful in case anyone rebases
	repoUrl := "https://github.com/go-git/go-billy" // HACK: Hardcoded for now
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
	processor.ProcessConstants()

	cIter.ForEach(func(c *object.Commit) error {
		// start := time.Now()
		hash := c.Hash.String()
		res, err := db.Exec("INSERT INTO commits (hash, contributor, author_date, project) VALUES (?, ?, ?, ?)",
			hash,
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

		// PERF: We could reuse the channel, if we don't close it. We could use contexts to signal the `process` function to stop
		files := make(chan FileState, runtime.NumCPU())
		go findFiles(filesystem, commitId, c, files)

		if err = process(filesystem, appender, files); err != nil {
			return err
		}

		// log.Info().Str("hash", hash).Dur("duration", time.Since(start)).Msg("Finished processing commit")
		return nil
	})

	if err = appender.Flush(); err != nil {
		return err
	}

	if err = appender.Close(); err != nil {
		return err
	}

	return nil
}

func findFiles(filesystem billy.Filesystem, commitId int64, commit *object.Commit, files chan FileState) error {
	defer close(files)

	err := util.Walk(filesystem, ".", func(path string, info fs.FileInfo, err error) error {
		// start := time.Now()
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
		// log.Info().Dur("duration", time.Since(start)).Str("file", path).Msg("Found file")
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

func process(filesystem billy.Filesystem, appender *duckdb.Appender, files chan FileState) error {
	var wg sync.WaitGroup

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			for file := range files {
				// start := time.Now()
				loc := file.Location
				f, err := filesystem.Open(loc)
				if err != nil {
					continue
				}
				_, err = f.Read(file.Content)
				if err != nil {
					continue
				}
				processFile(file, appender)
				// log.Info().Dur("duration", time.Since(start)).Str("file", loc).Msg("Processed file")
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return nil
}

var mu sync.Mutex

func processFile(file FileState, appender *duckdb.Appender) {
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

	stats, err := statsToMap(file.Commit)
	if err != nil {
		log.Error().Err(err).Msg("Error getting stats")
		return
	}

	linesAdded, linesDeleted := 0, 0
	if stat, ok := stats[file.Location]; ok {
		linesAdded = stat.Addition
		linesDeleted = stat.Deletion
	}

	blameAuthors, blameDates, err := blameToLists(file.Commit, file.Location)
	if err != nil {
		log.Error().Err(err).Msg("Error getting blame")
		return
	}

	mu.Lock()
	err = appender.AppendRow(
		file.Commit.Hash.String(),
		file.Location,
		file.Language,
		int32(file.Code),
		int32(file.Comment),
		int32(file.Blank),
		int32(linesAdded),
		int32(linesDeleted),
		int32(file.Complexity),
		blameAuthors,
		blameDates,
	)
	mu.Unlock()

	if err != nil {
		log.Error().Err(err).Msg("Error appending row")
	}
	// PERF: Could improve performance by configuring gc
	// PERF: Could improve performance by passing around more pointers instead of values, esp. for FileState
}

func statsToMap(commit *object.Commit) (map[string]object.FileStat, error) {
	stats, err := commit.Stats()
	if err != nil {
		return nil, err
	}

	statsMap := make(map[string]object.FileStat, len(stats))
	for _, stat := range stats {
		statsMap[stat.Name] = stat
	}

	return statsMap, nil
}

func blameToLists(commit *object.Commit, fileName string) ([]string, []int32, error) {
	blame, err := git.Blame(commit, fileName)
	if err != nil {
		return nil, nil, err
	}

	authors := make([]string, len(blame.Lines))
	dates := make([]int32, len(blame.Lines))

	for i, line := range blame.Lines {
		authors[i] = line.AuthorName
		dates[i] = int32(line.Date.Unix())
	}

	return authors, dates, nil
}

// TODO: Log how long processing and analyzing commit took
