package internal

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/boyter/scc/v3/processor"

	"github.com/rs/zerolog/log"

	"github.com/tim-hilt/codescene/internal/database"
	"github.com/tim-hilt/codescene/internal/git"
)

var (
	ErrRepoFormat = errors.New("provide repo in the format <user>/<repo>")

	LargeByteCount = 1000000
	Concurrency    = runtime.NumCPU()
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

func Analyze(db *database.DB, repo string, force bool, filestateProcessedCallback func(curr, total int)) error {
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

	newestCommitAt, err := db.GetNewestCommitDate(repo)
	if err != nil {
		return err
	}

	repository, err := git.Clone(repo, newestCommitAt)

	if err == git.ErrNoNewCommits {
		log.Info().Str("repository", repo).Msg("no new commits")
		return nil
	}

	if err != nil {
		return err
	}
	defer repository.Close()

	commits, numCommits, filestates, numFilestates, err := repository.Log()
	if err != nil {
		return err
	}

	log.Info().Str("repo", repo).Int("commits", numCommits).Msg("Injecting new commits")

	errs := make(chan error)

	go db.PersistCommits(commits, errs)

	processor.ProcessConstants()

	removedFiles := make(map[string][]string)
	output := processFilestates(repository, filestates, &removedFiles, errs)
	go db.PersistFileStates(output, numFilestates, filestateProcessedCallback, errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}

	if err := db.Flush(); err != nil {
		return err
	}

	if err := db.FillFilestates(repo, removedFiles); err != nil {
		return err
	}

	return nil
}

func processFilestates(repository git.Repository, input chan database.FileState, removedFiles *map[string][]string, errs chan error) chan database.FileState {
	output := make(chan database.FileState)

	go func() {
		defer close(errs)
		defer close(output)
		var (
			wg  sync.WaitGroup
			mut sync.Mutex
		)

		for i := 0; i < Concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for filestate := range input {
					filestate.Location = filepath.Join(repository.Path, filestate.Filename)
					content, err := repository.Show(filestate.CommitHash, filestate.Filename)

					if err == os.ErrNotExist {
						mut.Lock()
						if _, exists := (*removedFiles)[filestate.CommitHash]; !exists {
							(*removedFiles)[filestate.CommitHash] = []string{}
						}
						(*removedFiles)[filestate.CommitHash] = append((*removedFiles)[filestate.CommitHash], filestate.Filename)
						mut.Unlock()
						continue
					}

					if err != nil {
						errs <- err
						return
					}

					newFileJob(content, &filestate)
					if filestate.FileJob == nil {
						continue
					}

					if err := processFile(&filestate); err != nil && err.Error() != "Missing #!" {
						errs <- err
						return
					}

					output <- filestate
				}
			}()
		}

		wg.Wait()
	}()

	return output
}

func newFileJob(content []byte, filestate *database.FileState) {
	if len(content) >= LargeByteCount {
		filestate.FileJob = nil
		return
	}

	language, extension := processor.DetectLanguage(filestate.Filename)

	if len(language) != 0 {
		for _, l := range language {
			processor.LoadLanguageFeature(l)
		}

		for _, l := range language {
			if l == "ignore" || l == "gitignore" {
				filestate.FileJob = nil
				return
			}
		}

		filestate.Extension = extension
		filestate.PossibleLanguages = language
		filestate.Bytes = int64(len(content))
		filestate.Content = content
	}
}

func processFile(filestate *database.FileState) error {
	filestate.Language = processor.DetermineLanguage(filestate.Filename, filestate.Language, filestate.PossibleLanguages, filestate.Content)
	if filestate.Language == processor.SheBang {

		cutoff := 200

		// To avoid runtime panic check if the content we are cutting is smaller than 200
		if len(filestate.Content) < cutoff {
			cutoff = len(filestate.Content)
		}

		lang, err := processor.DetectSheBang(string(filestate.Content[:cutoff]))
		if err != nil {
			return err
		}

		filestate.Language = lang
		processor.LoadLanguageFeature(lang)
	}

	processor.CountStats(filestate.FileJob)

	return nil
}
