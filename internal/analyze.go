package internal

import (
	"encoding/csv"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/boyter/scc/v3/processor"

	"github.com/rs/zerolog/log"

	// TODO: Try to get arrow support working
	"github.com/tim-hilt/codescene/internal/database"
)

/**
 * 2. Rewrite gitLog, so that it returns a []FileState -> Which is the type, that will be persisted -> Requires rewriting FileState type
 * 3. Reduce nested goroutines - have one fan-out call and bump concurrency up if needed
 * 4. Introduce git submodule
 * 5. Find out what is causing the amount of duplicate data (should be around 850.000 rows, not 10.9M rows)
 * 6. Find out why progress is blocked
 */

type FileState struct {
	commitHash   string
	path         string
	renameFrom   string
	linesAdded   int64
	linesDeleted int64
	language     string
	sloc         int64
	cloc         int64
	blank        int64
	complexity   int64
}

var (
	ErrRepoFormat = errors.New("provide repo in the format <user>/<repo>")
)

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
		return "", ErrRepoFormat
	}

	repo = u.Host + "/" + u.Path

	return repo, nil
}

func Analyze(db *database.DB, repo string, force bool, commitCompletedCallback func(curr, total uint64)) error {

	repo, err := sanitizeRepo(repo)
	if err != nil {
		return err
	}

	if force {
		log.Info().Str("repo", repo).Msg("Force re-analyzing repository, deleting old data")
		go db.Clean(repo)
	}

	// TODO: Find newest commit and clone only from the day of the commit
	// TODO: Don't checkout - clone only .git folder

	repoPath, err := os.MkdirTemp("", "")
	if err != nil {
		return err
	}
	defer os.RemoveAll(repoPath)
	start := time.Now()
	err = gitClone("https://"+repo, repoPath)
	if err != nil {
		return err
	}
	log.Info().Dur("duration", time.Since(start)).Str("repository", repo).Msg("cloning finished")

	commits, filestates, err := gitLog(repoPath)
	if err != nil {
		return err
	}

	log.Info().Str("repo", repo).Int("commits", len(commits)).Msg("Injecting new commits")

	// TODO: This could be done concurrently
	// Should this be a function in the database package?
	for _, commit := range commits {
		date, err := time.Parse(time.RFC3339, commit.Date)
		if err != nil {
			return err
		}
		err = db.CommitsAppender.AppendRow(commit.Hash, commit.Author, date, repo, commit.Message)
		if err != nil {
			return err
		}
	}

	processor.ProcessConstants()

	file, err := os.Create("filestates.csv")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())

	writer := csv.NewWriter(file)

	if err := writer.Write([]string{
		"commit_hash",
		"path",
		"rename_from",
		"language",
		"sloc",
		"cloc",
		"blank",
		"complexity",
		"lines_added",
		"lines_deleted",
	}); err != nil {
		return err
	}

	for i, filestate := range filestates {
		path := filepath.Join(repoPath, filestate.path)
		content, err := gitShow(repoPath, filestate.commitHash, filestate.path)

		if err == os.ErrNotExist {
			continue
		}

		if err != nil {
			return err
		}

		// TODO: I'm pretty sure, not all needed properties of FileJob are set here
		f := newFileJob(repoPath, path, content)
		if f == nil {
			continue
		}

		if err := processFile(f); err != nil && err.Error() != "Missing #!" {
			return err
		}

		filestate.language = f.Language
		filestate.sloc = f.Code
		filestate.cloc = f.Comment
		filestate.blank = f.Blank
		filestate.complexity = f.Complexity

		// TODO: Check how much slower an appender would be here
		mut.Lock()
		writer.Write([]string{
			filestate.commitHash,
			filestate.path,
			filestate.renameFrom,
			filestate.language,
			strconv.FormatInt(filestate.linesAdded, 10),
			strconv.FormatInt(filestate.linesDeleted, 10),
			strconv.FormatInt(filestate.sloc, 10),
			strconv.FormatInt(filestate.cloc, 10),
			strconv.FormatInt(filestate.blank, 10),
			strconv.FormatInt(filestate.complexity, 10),
		})
		mut.Unlock()

		commitCompletedCallback(uint64(i), uint64(len(filestates)))
	}

	if err = db.CommitsAppender.Flush(); err != nil {
		return err
	}

	writer.Flush()
	// TODO: Fill missing filestates -> Claude

	if err := db.ImportCSV(file.Name()); err != nil {
		return err
	}

	return nil
}

var (
	mut            sync.Mutex
	LargeByteCount int = 1000000
)

func newFileJob(repoPath, path string, content []byte) *processor.FileJob {
	if len(content) >= LargeByteCount {
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
			Filename:          strings.TrimPrefix(path, repoPath+"/"),
			Extension:         extension,
			PossibleLanguages: language,
			Bytes:             int64(len(content)),
			Content:           content,
		}
	}

	return nil
}

func processFile(file *processor.FileJob) error {
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

	processor.CountStats(file)

	if file.Binary {
		// Stop analysis, but not an error
		return nil
	}
	// PERF: Could improve performance by passing around more pointers instead of values, esp. for FileState
	return nil
}

func gitClone(repo, destination string) error {
	cmd := exec.Command("git", "clone", "--single-branch", "--no-tags", repo, destination)
	return cmd.Run()
}

type Commit struct {
	Hash    string
	Author  string
	Message string
	Date    string
}

const commitSeparator = "COMMIT_START"

func gitLog(repo string) ([]Commit, []FileState, error) {
	cmd := exec.Command("git", "log", "--reverse", "--numstat", "--pretty=format:"+commitSeparator+"%H;%cI;%an;%s")
	cmd.Dir = repo

	stdout, err := cmd.Output()
	if err != nil {
		return nil, nil, err
	}

	if len(stdout) == 0 {
		return nil, nil, os.ErrNotExist
	}

	commitsString := string(stdout)
	commitStrings := strings.Split(commitsString, commitSeparator)[1:]
	commits := make([]Commit, len(commitStrings))
	var filestates []FileState

	for i, commitString := range commitStrings {
		commit, fs, err := parseCommit(commitString)
		if err != nil {
			return nil, nil, err
		}
		commits[i] = commit
		filestates = append(filestates, fs...)
	}

	return commits, filestates, nil
}

func parseCommit(commitString string) (Commit, []FileState, error) {
	commitString = strings.TrimSpace(commitString)
	lines := strings.Split(commitString, "\n")

	commitData := strings.Split(strings.Trim(lines[0], "'"), ";")

	// TODO: Verify if commiter timezone is included + added correctly in DuckDB
	commit := Commit{
		Hash:    commitData[0],
		Author:  commitData[2],
		Message: commitData[3],
		Date:    commitData[1],
	}

	var filestates []FileState

	for _, line := range lines[1:] {
		fileChange := strings.Split(line, "\t")
		if fileChange[0] == "-" && fileChange[1] == "-" || line == "" {
			// Don't record binary files, skip empty lines
			continue
		}
		linesAdded, err := strconv.ParseInt(fileChange[0], 10, 32)
		if err != nil {
			return Commit{}, nil, err
		}
		linesDeleted, err := strconv.ParseInt(fileChange[1], 10, 32)
		if err != nil {
			return Commit{}, nil, err
		}
		renameFrom, path := getRenamedPaths(fileChange[2])
		filestates = append(filestates, FileState{
			commitHash:   commitData[0],
			path:         path,
			renameFrom:   renameFrom,
			linesAdded:   linesAdded,
			linesDeleted: linesDeleted,
		})
	}

	return commit, filestates, nil
}

func getRenamedPaths(path string) (string, string) {
	// Case 1: {... => ...} within a path segment
	reBrace := regexp.MustCompile(`\{([^{}]*) => ([^{}]*)\}`)
	if reBrace.MatchString(path) {
		match := reBrace.FindStringSubmatch(path)
		prefix := path[:strings.Index(path, "{")]
		suffix := path[strings.LastIndex(path, "}")+1:]
		oldPath := prefix + match[1] + suffix
		newPath := prefix + match[2] + suffix
		return oldPath, newPath
	}

	// Case 2: Full-path rename with => separator
	reArrow := regexp.MustCompile(`^(.*) => (.*)$`)
	if reArrow.MatchString(path) {
		match := reArrow.FindStringSubmatch(path)
		return strings.TrimSpace(match[1]), strings.TrimSpace(match[2])
	}

	return path, path
}

func gitShow(repo, hash, file string) ([]byte, error) {
	cmd := exec.Command("git", "show", fmt.Sprintf("%s:%s", hash, file))
	cmd.Dir = repo

	stdout, err := cmd.Output()

	if err != nil {
		// TODO: Infer correct error based off of stderr
		return nil, os.ErrNotExist
	}

	return stdout, nil
}
