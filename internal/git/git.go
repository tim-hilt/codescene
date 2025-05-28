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

const commitSeparator = "COMMIT_START"

func (r Repository) Log() (chan database.Commit, int, chan database.FileState, int, error) {
	cmd := exec.Command("git", "log", "--reverse", "--numstat", "--pretty=format:"+commitSeparator+"%H;%aI;%an;%s")
	cmd.Dir = r.Path

	stdout, err := cmd.Output()
	if err != nil {
		return nil, -1, nil, -1, err
	}

	if len(stdout) == 0 {
		return nil, -1, nil, -1, os.ErrNotExist
	}

	commitsString := string(stdout)
	commitStrings := strings.Split(commitsString, commitSeparator)[1:]
	commits := make([]database.Commit, len(commitStrings))
	var filestates []database.FileState

	for i, commitString := range commitStrings {
		commit, fs, err := parseCommit(commitString)
		if err != nil {
			return nil, -1, nil, -1, err
		}
		commit.Project = r.repo
		commits[i] = commit
		filestates = append(filestates, fs...)
	}

	cs := make(chan database.Commit)
	fs := make(chan database.FileState, Concurrency)

	go func() {
		defer close(cs)
		for _, commit := range commits {
			cs <- commit
		}
	}()

	go func() {
		defer close(fs)
		for _, filestate := range filestates {
			fs <- filestate
		}
	}()

	return cs, len(commitStrings), fs, len(filestates), nil
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

func (r Repository) Close() error {
	return os.RemoveAll(r.Path)
}
