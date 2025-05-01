package internal

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/boyter/scc/v3/processor"
	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-billy/v5/util"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"
	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/rs/zerolog/log"
	// TODO: Try to get arrow support working
)

func initDB(repo string, force bool) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "codescene.db")
	if err != nil {
		return nil, err
	}

	// TODO: Do I need a primary key for the filestates?
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

	if _, err = db.Exec(createTablesStmt); err != nil {
		return nil, err
	}

	if !force {
		return db, nil
	}

	deleteFileStatesStmt := `
    DELETE FROM filestates
    WHERE commit_hash IN (
        SELECT hash
        FROM commits
        WHERE project = ?
    );`
	if _, err := db.Exec(deleteFileStatesStmt, repo); err != nil {
		return nil, err
	}

	deleteCommitsStmt := `
    DELETE FROM commits
    WHERE project = ?;`
	if _, err := db.Exec(deleteCommitsStmt, repo); err != nil {
		return nil, err
	}

	return db, nil
}

type FileState struct {
	Commit *object.Commit
	*processor.FileJob
}

func Analyze(repo string, force bool) error {
	db, err := initDB(repo, force)
	if err != nil {
		return err
	}
	defer db.Close()

	filesystem := memfs.New()
	r, err := git.Clone(memory.NewStorage(), filesystem, &git.CloneOptions{
		URL: repo,
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

	output := make(chan FileState, runtime.NumCPU())
	input := make(chan FileState, runtime.NumCPU())

	go func() {
		cIter.ForEach(func(c *object.Commit) error {
			log.Info().Str("hash", c.Hash.String()).Msg("Processing commit")
			hash := c.Hash.String()
			mu.Lock()
			_, err := db.Exec("INSERT INTO commits (hash, contributor, author_date, project) VALUES (?, ?, ?, ?)",
				hash,
				c.Author.Name,
				c.Author.When.Unix(),
				repo,
			)
			mu.Unlock()

			if err != nil && strings.Contains(err.Error(), "Duplicate key") {
				return nil
			} else if err != nil {
				return err
			}

			if err := worktree.Checkout(&git.CheckoutOptions{
				Hash: c.Hash,
			}); err != nil {
				return err
			}

			ctx, cancelCtx := context.WithCancel(context.Background())
			go findFiles(cancelCtx, filesystem, c, input)

			process(ctx, filesystem, input, output)

			// log.Info().Str("hash", hash).Dur("duration", time.Since(start)).Msg("Finished processing commit")
			return nil
		})
		close(output)
	}()

	filestates, err := collectFileStates(output)
	if err != nil {
		return err
	}
	if err := insert(db, filestates); err != nil {
		return err
	}

	return nil
}

func findFiles(cancelCtx context.CancelFunc, filesystem billy.Filesystem, commit *object.Commit, files chan FileState) error {
	defer cancelCtx()

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
			Commit:  commit,
			FileJob: f,
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

func process(ctx context.Context, filesystem billy.Filesystem, input, output chan FileState) {
	var wg sync.WaitGroup

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case file := <-input:
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
					processFile(file, output)
					// log.Info().Dur("duration", time.Since(start)).Str("file", loc).Msg("Processed file")
				}
			}
		}()
	}

	wg.Wait()
}

var mu sync.Mutex

func processFile(file FileState, output chan FileState) {
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

	output <- file
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

func blameToLists(commit *object.Commit, fileName string) (string, string, error) {
	blame, err := git.Blame(commit, fileName)
	if err != nil {
		return "", "", err
	}

	authors, dates := "[", "["

	for _, line := range blame.Lines {
		authors += "'" + line.AuthorName + "', "
		dates += strconv.FormatInt(line.Date.Unix(), 10) + ", "
	}

	authors = strings.TrimSuffix(authors, ", ") + "]"
	dates = strings.TrimSuffix(dates, ", ") + "]"

	return authors, dates, nil
}

func collectFileStates(output chan FileState) ([]interface{}, error) {
	var values []interface{}
	for file := range output {
		stats, err := statsToMap(file.Commit)
		if err != nil {
			log.Error().Err(err).Msg("Error getting stats")
			return nil, err
		}

		linesAdded, linesDeleted := 0, 0
		if stat, ok := stats[file.Location]; ok {
			linesAdded = stat.Addition
			linesDeleted = stat.Deletion
		}

		blameAuthors, blameDates, err := blameToLists(file.Commit, file.Location)
		if err != nil {
			log.Error().Err(err).Msg("Error getting blame")
			return nil, err
		}
		values = append(values, file.Commit.Hash.String(), file.Location, file.Language, file.Code, file.Comment, file.Blank, linesAdded, linesDeleted, file.Complexity, blameAuthors, blameDates)
	}
	return values, nil
}

func insert(db *sql.DB, values []interface{}) error {
	log.Info().Msg("Inserting into database")
	stmt := "INSERT INTO filestates (commit_hash, path, language, sloc, cloc, blank, lines_added, lines_removed, complexity, blame_authors, blame_dates) VALUES "

	numCols := 11
	numRows := len(values) / numCols

	for i := 0; i < numRows; i++ {
		stmt += "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), "
	}
	stmt = strings.TrimSuffix(stmt, ", ")

	if _, err := db.Exec(stmt, values...); err != nil {
		return err
	}

	return nil
}
