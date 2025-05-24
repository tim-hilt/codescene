package internal

import (
	"encoding/csv"
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
	"sync/atomic"
	"time"

	"github.com/boyter/scc/v3/processor"

	"github.com/rs/zerolog/log"

	// TODO: Try to get arrow support working
	"github.com/tim-hilt/codescene/internal/database"
)

type FileState struct {
	Commit
	*processor.FileJob
	Stats *FileChange
}

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

func Analyze(db *database.DB, repo string, force bool, commitCompletedCallback func(curr, total uint64)) error {

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

	// TODO: Find newest commit and clone only from the day of the commit

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
	log.Info().Dur("duration", time.Since(start)).Msg("cloning finished")

	commits, err := gitLog(repoPath)
	if err != nil {
		return err
	}

	log.Info().Str("repo", repo).Int("commits", len(commits)).Msg("Injecting new commits")
	errs := make(chan error)

	processor.ProcessConstants()

	fw, err := NewFilestatesWriter()
	if err != nil {
		return err
	}
	defer fw.Close()

	go func() {
		defer close(errs)
		var i atomic.Uint64

		for _, commit := range commits {
			date, err := time.Parse(time.RFC3339, commit.Date)
			if err != nil {
				errs <- err
				return
			}

			// TODO: Append new commits upfront, so that Analyze can be run repeatedly
			err = db.CommitsAppender.AppendRow(commit.Hash, commit.Author, date, repo, commit.Message)

			if err != nil && strings.Contains(err.Error(), "Duplicate key") {
				continue
			}

			if err != nil {
				errs <- err
				return
			}

			err = gitCheckout(repoPath, commit.Hash)
			if err != nil {
				errs <- err
				return
			}

			input := make(chan FileState, runtime.NumCPU())
			removedFiles := make(chan string)
			changedFiles := make(chan FileState, runtime.NumCPU())

			go findFiles(repoPath, commit, input, removedFiles, errs)
			go process(input, changedFiles, errs)
			if err := fw.Collect(commit.Hash, changedFiles, removedFiles); err != nil {
				errs <- err
				return
			}

			commitCompletedCallback(i.Add(1), uint64(len(commits)))
		}
	}()

	for err := range errs {
		return err
	}

	if err := fw.Import(db); err != nil {
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

type FilestatesWriter struct {
	*csv.Writer
	file     *os.File
	previous [][]string
}

func NewFilestatesWriter() (*FilestatesWriter, error) {
	file, err := os.Create("filestates.csv")
	if err != nil {
		return nil, err
	}
	writer := csv.NewWriter(file)

	fw := &FilestatesWriter{writer, file, [][]string{}}

	if err := fw.Write([]string{"commit_hash", "path", "language", "sloc", "cloc", "blank", "complexity", "lines_added", "lines_deleted"}); err != nil {
		return nil, err
	}

	return fw, nil
}

func (fw *FilestatesWriter) Collect(hash string, changedFiles chan FileState, removedFiles chan string) error {
	var (
		rfs []string
		cfs []FileState
	)

	for {
		var changedFilesClosed, removedFilesClosed bool
		for !changedFilesClosed || !removedFilesClosed {
			select {
			case val, ok := <-changedFiles:
				if !ok {
					changedFilesClosed = true
					changedFiles = nil // disable this case
					continue
				}
				cfs = append(cfs, val)
			case val, ok := <-removedFiles:
				if !ok {
					removedFilesClosed = true
					removedFiles = nil // disable this case
					continue
				}
				rfs = append(rfs, val)
			}
		}
		break
	}

	var current [][]string

	for _, row := range fw.previous {
		for _, cf := range cfs {
			if cf.Filename == row[1] {
				// If file is changed in commit
				continue
			}

			if cf.Stats.RenameFrom == row[1] {
				// If changed file was renamed from previously registered state
				continue
			}
		}

		for _, rf := range rfs {
			if rf == row[1] {
				// If file was removed in commit
				continue
			}
		}

		row[0] = hash
		current = append(current, row)
	}

	for _, cf := range cfs {
		current = append(current, []string{
			cf.Commit.Hash,
			cf.Filename,
			cf.Language,
			strconv.FormatInt(cf.Code, 10),
			strconv.FormatInt(cf.Comment, 10),
			strconv.FormatInt(cf.Blank, 10),
			strconv.FormatInt(cf.Complexity, 10),
			strconv.FormatInt(cf.Stats.LinesAdded, 10),
			strconv.FormatInt(cf.Stats.LinesRemoved, 10),
		})
	}

	if err := fw.WriteAll(current); err != nil {
		return err
	}

	fw.previous = current

	return nil
}

func (fw *FilestatesWriter) Import(db *database.DB) error {
	fw.Flush()
	if err := db.ImportCSV(fw.file.Name()); err != nil {
		return err
	}
	return nil
}

func (fw *FilestatesWriter) Close() {
	fw.file.Close()
	os.Remove(fw.file.Name())
}

func findFiles(repoPath string, commit Commit, input chan FileState, removedFiles chan string, errs chan error) {
	defer close(input)
	defer close(removedFiles)

	for _, fileChange := range commit.FileChanges {
		path := filepath.Join(repoPath, fileChange.Path)
		info, err := os.Lstat(path)

		if err != nil && strings.Contains(err.Error(), "no such file") && fileChange.LinesRemoved > 0 {
			// File was removed in this commit
			removedFiles <- path
			continue
		}

		if err != nil {
			errs <- err
			return
		}
		f := newFileJob(repoPath, path, info)
		if f == nil {
			continue
		}
		input <- FileState{
			Commit:  commit,
			FileJob: f,
			Stats:   &fileChange,
		}
	}
}

var LargeByteCount int64 = 1000000

func newFileJob(repoPath, path string, info fs.FileInfo) *processor.FileJob {
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
			Filename:          strings.TrimPrefix(path, repoPath+"/"),
			Extension:         extension,
			PossibleLanguages: language,
			Bytes:             info.Size(),
			Content:           make([]byte, info.Size()),
		}
	}

	return nil
}

func process(input, changedFiles chan FileState, errs chan error) {
	defer close(changedFiles)
	var wg sync.WaitGroup

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range input {
				content, err := os.ReadFile(file.Location)
				if err != nil && err != io.EOF {
					errs <- err
					return
				}
				copy(file.Content, content)
				if err = processFile(file, changedFiles); err != nil && err.Error() != "Missing #!" {
					errs <- err
					return
				}
			}
		}()
	}

	wg.Wait()
}

func processFile(file FileState, changedFiles chan FileState) error {
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
	changedFiles <- file
	// PERF: Could improve performance by passing around more pointers instead of values, esp. for FileState
	return nil
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
	cmd := exec.Command("git", "log", "--reverse", "--numstat", "--pretty=format:"+commitSeparator+"%H;%cI;%an;%s")
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
	commitString = strings.TrimSpace(commitString)
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

	return commit, nil
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

func gitCheckout(repo, hash string) error {
	cmd := exec.Command("git", "checkout", "-f", hash)
	cmd.Dir = repo
	return cmd.Run()
}
