package internal

import (
	"database/sql"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/boyter/scc/v3/processor"
	"github.com/marcboeker/go-duckdb/v2"

	"github.com/rs/zerolog/log"

	// TODO: Try to get arrow support working
	"github.com/tim-hilt/codescene/internal/database"
)

type FileState struct {
	Commit Commit
	*processor.FileJob
}

var mut sync.Mutex

func sanitizeRepo(repo string) (string, error) {
	u, err := url.Parse(repo)
	if err != nil {
		return "", err
	}

	if u.Host == "" {
		// Assume github.com, if host not provided
		u.Host = "github.com"
	}

	u.Path = strings.TrimSuffix(strings.TrimPrefix(u.Path, "/"), "/")

	if len(strings.Split(u.Path, "/")) != 2 {
		return "", errors.New("provide repo in the format <user>/<repo>")
	}

	repo = u.Host + "/" + u.Path

	return repo, nil
}

func Analyze(db *sql.DB, appender *duckdb.Appender, repo string, force bool, commitCompletedCallback func(curr, total int)) error {

	repo, err := sanitizeRepo(repo)
	if err != nil {
		return err
	}

	if force {
		log.Info().Str("repo", repo).Msg("Force re-analyzing repository, deleting old data")

		if err := database.Clean(db, repo); err != nil {
			return err
		}
	}

	log.Info().Str("repo", repo).Msg("Cloning repository")
	tmp, err := os.MkdirTemp("", "")
	defer os.RemoveAll(tmp)

	if err != nil {
		return err
	}

	if err = gitClone("https://"+repo, tmp); err != nil {
		return err
	}

	commits, err := gitLog(tmp)

	if err != nil {
		return err
	}

	log.Info().Str("repo", repo).Msg("Injecting new commits")

	for _, commit := range commits {
		mut.Lock()
		_, err := db.Exec("INSERT INTO commits (hash, contributor, author_date, project) VALUES (?, ?, ?, ?)",
			commit.Hash,
			commit.Author,
			commit.Date,
			repo,
		)
		mut.Unlock()

		if err != nil && strings.Contains(err.Error(), "Duplicate key") {
			continue
		} else if err != nil {
			return err
		}
	}

	log.Info().Str("repo", repo).Int("newCommits", len(commits)).Msg("New commits injected")

	processor.ProcessConstants()

	errs := make(chan error)

	go func() {
		defer close(errs)

		for i, c := range commits {
			if err := gitCheckout(tmp, c.Hash); err != nil {
				errs <- err
				return
			}

			input := make(chan FileState, runtime.NumCPU())
			go findFiles(tmp, c, input, errs)

			process(appender, input, errs)
			commitCompletedCallback(i+1, len(commits))
		}
	}()

	for err := range errs {
		return err
	}

	if err = appender.Flush(); err != nil {
		return err
	}

	return nil
}

func findFiles(tmpDir string, commit Commit, files chan FileState, errs chan error) {
	err := filepath.Walk(tmpDir, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			// Not interested in directories
			return nil
		}
		if err != nil {
			return err
		}
		f := newFileJob(path, info)
		if f == nil {
			return nil
		}
		files <- FileState{
			Commit:  commit,
			FileJob: f,
		}
		return nil
	})
	if err != nil {
		errs <- err
	}

	close(files)
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

func process(appender *duckdb.Appender, input chan FileState, errs chan error) {
	var wg sync.WaitGroup

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range input {
				loc := file.Location
				f, err := os.Open(loc)
				if err != nil {
					errs <- err
					return
				}
				_, err = f.Read(file.Content)
				if err != nil && err != io.EOF {
					errs <- err
					return
				}
				if err = processFile(appender, file); err != nil && err.Error() != "Missing #!" {
					errs <- err
					return
				}
			}
		}()
	}

	wg.Wait()
}

func processFile(appender *duckdb.Appender, file FileState) error {
	file.Language = processor.DetermineLanguage(file.Filename, file.Language, file.PossibleLanguages, file.Content)
	if file.Language == processor.SheBang {

		cutoff := 200

		// To avoid runtime panic check if the content we are cutting is smaller than 200
		if len(file.Content) < cutoff {
			cutoff = len(file.Content)
		}

		lang, err := processor.DetectSheBang(string(file.Content[:cutoff]))
		if err != nil {
			return err
		}

		file.Language = lang
		processor.LoadLanguageFeature(lang)
	}

	processor.CountStats(file.FileJob)

	if file.Binary {
		// Stop analysis, but not an error
		return nil
	}

	mut.Lock()
	defer mut.Unlock()
	if err := appender.AppendRow(file.Commit.Hash, file.Location, file.Language, int32(file.Code), int32(file.Comment), int32(file.Blank), int32(file.Complexity)); err != nil {
		return err
	}
	// PERF: Could improve performance by configuring gc
	// PERF: Could improve performance by passing around more pointers instead of values, esp. for FileState
	return nil
}

type FileChange struct {
	Path         string
	LinesAdded   int64
	LinesRemoved int64
	RenameFrom   string
}

type Commit struct {
	Hash        string
	Author      string
	Date        string
	FileChanges []FileChange
}

func gitClone(repo, destination string) error {
	cmd := exec.Command("git", "clone", "--single-branch", "--no-tags", repo, destination)
	return cmd.Run()
}

func gitLog(repo string) ([]Commit, error) {
	cmd := exec.Command("git", "log", "--no-merges", "--diff-filter=ACMRTUXB", "--numstat", "--pretty=format:'%H;%cI;%an'")
	cmd.Dir = repo

	stdout, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	if len(stdout) == 0 {
		return []Commit{}, nil
	}

	commitsString := string(stdout)
	commitStrings := strings.Split(commitsString, "\n\n")
	commits := make([]Commit, len(commitStrings))

	for i, commitString := range commitStrings {
		commit, err := parseCommit(commitString)
		if err != nil {
			return nil, err
		}
		commits[i] = commit
	}

	return commits, nil
}

func parseCommit(commitString string) (Commit, error) {
	lines := strings.Split(commitString, "\n")
	fileChanges := make([]FileChange, len(lines)-1)

	for j, line := range lines[1:] {
		fileChange := strings.Split(line, "\t")
		if fileChange[0] == "-" && fileChange[1] == "-" || line == "" {
			// Don't record binary files, skip empty lines
			continue
		}
		linesAdded, err := strconv.ParseInt(fileChange[0], 10, 64)
		if err != nil {
			return Commit{}, err
		}
		linesRemoved, err := strconv.ParseInt(fileChange[1], 10, 64)
		if err != nil {
			return Commit{}, err
		}
		renameFrom, path := getRenamedPaths(fileChange[2])
		fileChanges[j] = FileChange{
			Path:         path,
			LinesAdded:   linesAdded,
			LinesRemoved: linesRemoved,
			RenameFrom:   renameFrom,
		}
	}
	// TODO: Find out, why git surrounds pretty format with \'...\'
	commitData := strings.Split(strings.Trim(lines[0], "'"), ";")
	commit := Commit{
		Hash:        commitData[0],
		Author:      commitData[2],
		Date:        commitData[1],
		FileChanges: fileChanges,
	}
	return commit, nil
}

func getRenamedPaths(input string) (string, string) {
	re := regexp.MustCompile(`\{([^}]+) => ([^}]+)\}`)
	match := re.FindStringSubmatch(input)
	if match == nil {
		return input, input
	}
	before, after := match[1], match[2]
	replaced1 := strings.Replace(input, match[0], before, 1)
	replaced2 := strings.Replace(input, match[0], after, 1)

	return replaced1, replaced2
}

func gitCheckout(repo, hash string) error {
	cmd := exec.Command("git", "checkout", hash)
	cmd.Dir = repo
	return cmd.Run()
}
