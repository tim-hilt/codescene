package git

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/tim-hilt/codescene/internal/database"
)

var (
	ErrNoNewCommits = errors.New("no new commits")

	Concurrency = runtime.NumCPU()
)

type Repository struct {
	Path string
	repo string
}

func Clone(repo string, shallowSince time.Time) (Repository, error) {
	shallowSince = shallowSince.Add(1 * time.Second)

	destination, err := os.MkdirTemp("", "")
	if err != nil {
		return Repository{}, err
	}

	cmd := exec.Command(
		"git",
		"clone",
		"--single-branch",
		"--no-tags",
		"--no-checkout",
		"--shallow-since="+shallowSince.Format(time.RFC3339),
		"https://"+repo,
		destination,
	)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err = cmd.Run()

	if strings.Contains(stderr.String(), "error processing shallow info: 4") {
		return Repository{}, ErrNoNewCommits
	}

	if err != nil {
		return Repository{}, err
	}

	return Repository{destination, repo}, nil
}

func (r Repository) Log(errs chan error) (chan database.Commit, int, chan database.FileState, int, error) {
	cmd := exec.Command("git", "log", "--reverse", "--pretty=format:%H;%aI;%an;%s")
	cmd.Dir = r.Path

	stdout, err := cmd.Output()
	if err != nil {
		return nil, -1, nil, -1, err
	}

	if len(stdout) == 0 {
		return nil, -1, nil, -1, os.ErrNotExist
	}

	commitsString := string(stdout)
	commitStrings := strings.Split(strings.TrimSpace(commitsString), "\n")

	cs := make(chan database.Commit)
	fs := make(chan database.FileState, Concurrency)

	go func() {
		defer close(cs)
		defer close(fs)

		for i, commitString := range commitStrings {
			commit, err := parseCommit(commitString)
			if err != nil {
				errs <- err
				return
			}
			commit.Project = r.repo
			cs <- commit

			var previousHash string
			if i == 0 {
				// empty tree hash
				previousHash = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
			} else {
				previousHash = strings.SplitN(commitStrings[i-1], ";", 2)[0] // TODO: This could be a bit more elegant
			}

			cmd := exec.Command("git", "diff", "--no-renames", "--numstat", previousHash, commit.Hash)
			cmd.Dir = r.Path

			stdout, err := cmd.Output()

			if err != nil {
				errs <- err
				return
			}

			if len(stdout) == 0 {
				continue
			}

			filechanges := strings.Split(strings.TrimSpace(string(stdout)), "\n")
			filestates, err := parseFilestates(filechanges)

			if err != nil {
				errs <- err
				return
			}

			for _, filestate := range filestates {
				filestate.CommitHash = commit.Hash
				fs <- filestate // TODO: Monitor fullness of channel!
			}
		}
	}()

	return cs, len(commitStrings), fs, len(fs), nil // TODO: Get rid of this hack
}

func (r Repository) Show(hash, file string) ([]byte, error) {
	cmd := exec.Command("git", "show", fmt.Sprintf("%s:%s", hash, file))
	cmd.Dir = r.Path

	var (
		stderr bytes.Buffer
		stdout bytes.Buffer
	)

	cmd.Stderr = &stderr
	cmd.Stdout = &stdout
	err := cmd.Run()

	if strings.Contains(stderr.String(), "does not exist") {
		return nil, os.ErrNotExist
	}

	if err != nil {
		return nil, err
	}

	return stdout.Bytes(), nil
}

func (r Repository) Files(hash string) ([]string, error) {
	cmd := exec.Command("git", "ls-tree", "-r", "--name-only", hash)
	cmd.Dir = r.Path

	stdout, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	files := strings.Split(strings.TrimSpace(string(stdout)), "\n")

	return files, nil
}

func (r Repository) Close() error {
	return os.RemoveAll(r.Path)
}
