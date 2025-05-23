package internal

import (
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
	"time"

	"github.com/boyter/scc/v3/processor"

	"github.com/rs/zerolog/log"

	// TODO: Try to get arrow support working
	"github.com/tim-hilt/codescene/internal/database"
)

type FileState struct {
	Commit
	*processor.FileJob
	Stats *map[string]FileChange
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

func Analyze(db *database.DB, repo string, force bool, commitCompletedCallback func(curr, total int)) error {

	repo, err := sanitizeRepo(repo)
	if err != nil {
		return err
	}

	if force {
		log.Info().Str("repo", repo).Msg("Force re-analyzing repository, deleting old data")

		if err := db.Clean(repo); err != nil {
			return err
		}
	}

	tmp, err := os.MkdirTemp("", "")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	start := time.Now()
	err = gitClone("https://"+repo, tmp)
	if err != nil {
		return err
	}
	log.Info().Dur("duration", time.Since(start)).Msg("cloning finished")

	commits, err := gitLog(tmp)
	if err != nil {
		return err
	}

	log.Info().Str("repo", repo).Int("commits", len(commits)).Msg("Injecting new commits")
	errs := make(chan error)

	processor.ProcessConstants()

	go func() {
		defer close(errs)
		for i, commit := range commits {
			mut.Lock()
			// db.CommitsAppender.AppendRow(commit.Hash.String(), commit.Author.Name, commit.Author.When.Format(time.RFC3339), repo)
			// TODO: This could be sped up by using an appender or csv loader
			_, err := db.Exec("INSERT INTO commits (hash, contributor, author_date, project, message) VALUES (?, ?, ?, ?, ?)",
				commit.Hash,
				commit.Author,
				commit.Date,
				repo,
				commit.Message,
			)
			mut.Unlock()

			if err != nil && strings.Contains(err.Error(), "Duplicate key") {
				continue
			}

			if len(commit.FileChanges) == 0 {
				// Don't record, if no change
				continue
			}

			err = gitCheckout(tmp, commit.Hash)
			if err != nil {
				errs <- err
				return
			}

			input := make(chan FileState, runtime.NumCPU())
			go findFiles(tmp, commit, input, errs)

			process(db, input, errs)
			commitCompletedCallback(i+1, len(commits))
		}
	}()

	for err := range errs {
		return err
	}

	if err = db.CommitsAppender.Flush(); err != nil {
		return err
	}

	if err = db.FilestatesAppender.Flush(); err != nil {
		return err
	}

	return nil
}

func findFiles(repoPath string, commit Commit, files chan FileState, errs chan error) {
	defer close(files)

	stats, err := statsToMap(commit)
	if err != nil {
		errs <- err
		return
	}

	if err := filepath.Walk(repoPath, func(path string, info fs.FileInfo, err error) error {
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
			Stats:   &stats,
		}
		return nil
	}); err != nil {
		errs <- err
	}
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

func process(db *database.DB, input chan FileState, errs chan error) {
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
				if err = processFile(db, file); err != nil && err.Error() != "Missing #!" {
					errs <- err
					return
				}
			}
		}()
	}

	wg.Wait()
}

func processFile(db *database.DB, file FileState) error {
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

	stat := (*file.Stats)[file.Location]

	mut.Lock()
	defer mut.Unlock()
	if err := db.FilestatesAppender.AppendRow(file.Commit.Hash, file.Location, file.Language, int32(file.Code), int32(file.Comment), int32(file.Blank), int32(file.Complexity), int32(stat.LinesAdded), int32(stat.LinesRemoved)); err != nil {
		return err
	}
	// PERF: Could improve performance by configuring gc
	// PERF: Could improve performance by passing around more pointers instead of values, esp. for FileState
	return nil
}

func statsToMap(commit Commit) (map[string]FileChange, error) {
	stats := commit.FileChanges

	statsMap := make(map[string]FileChange, len(stats))
	for _, stat := range stats {
		statsMap[stat.Path] = stat
	}

	return statsMap, nil
}

func gitClone(repo, destination string) error {
	cmd := exec.Command("git", "clone", "--single-branch", "--no-tags", repo, destination)
	return cmd.Run()
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
	Message     string
	Date        string
	FileChanges []FileChange
}

const commitSeparator = "COMMIT_START"

func gitLog(repo string) ([]Commit, error) {
	cmd := exec.Command("git", "log", "--numstat", "--pretty=format:"+commitSeparator+"%H;%cI;%an;%s")
	cmd.Dir = repo

	stdout, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	if len(stdout) == 0 {
		return []Commit{}, nil
	}

	commitsString := string(stdout)
	commitStrings := strings.Split(commitsString, commitSeparator)[1:]
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

	commitData := strings.Split(strings.Trim(lines[0], "'"), ";")
	fileChanges := make([]FileChange, len(lines)-1)
	// TODO: Verify if commiter timezone is included + added correctly in DuckDB
	commit := Commit{
		Hash:        commitData[0],
		Author:      commitData[2],
		Message:     commitData[3],
		Date:        commitData[1],
		FileChanges: fileChanges,
	}

	if len(fileChanges) == 0 {
		// Commit without file changes
		return commit, nil
	}

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
	cmd := exec.Command("git", "checkout", "-f", hash)
	cmd.Dir = repo
	return cmd.Run()
}
