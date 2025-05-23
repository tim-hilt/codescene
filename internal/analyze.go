package internal

import (
	"errors"
	"io"
	"io/fs"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/boyter/scc/v3/processor"
	"github.com/go-git/go-billy/v5"

	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-billy/v5/util"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/rs/zerolog/log"

	// TODO: Try to get arrow support working
	"github.com/tim-hilt/codescene/internal/database"
)

type FileState struct {
	*object.Commit
	*processor.FileJob
	Stats *map[string]object.FileStat
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

	globalFilesystem := memfs.New()
	start := time.Now()
	globalRepo, err := git.Clone(memory.NewStorage(), globalFilesystem, &git.CloneOptions{
		URL:          "https://" + repo,
		Tags:         git.NoTags,
		SingleBranch: true,
	})
	log.Info().Dur("duration", time.Since(start)).Msg("cloning finished")

	if err != nil {
		return err
	}

	ref, err := globalRepo.Head()
	if err != nil {
		return err
	}

	cIter, err := globalRepo.Log(&git.LogOptions{
		From: ref.Hash(),
	})

	if err != nil {
		return err
	}

	commits := make(chan *object.Commit, runtime.NumCPU())

	log.Info().Str("repo", repo).Msg("Injecting new commits")
	errs := make(chan error)

	go func() {
		defer close(errs)
		defer close(commits)
		if err = cIter.ForEach(func(c *object.Commit) error {
			commits <- c
			return nil
		}); err != nil {
			errs <- err
			return
		}
	}()

	// TODO: Correct logs
	log.Info().Str("repo", repo).Int("newCommits", len(commits)).Msg("New commits injected")

	processor.ProcessConstants()

	var wg sync.WaitGroup
	for i := range runtime.NumCPU() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := globalRepo
			filesystem := globalFilesystem
			if i != 0 {
				// Seperate clone for goroutine 1 - NumCPU
				filesystem = memfs.New()
				start := time.Now()
				r, err = git.Clone(memory.NewStorage(), filesystem, &git.CloneOptions{
					URL:          "https://github.com/go-git/go-git",
					SingleBranch: true,
					Tags:         git.NoTags,
				})
				if err != nil {
					errs <- err
					return
				}
				log.Info().Dur("duration", time.Since(start)).Msg("cloning finished")
			}

			worktree, err := r.Worktree()
			if err != nil {
				errs <- err
				return
			}

			for commit := range commits {
				mut.Lock()
				// db.CommitsAppender.AppendRow(commit.Hash.String(), commit.Author.Name, commit.Author.When.Format(time.RFC3339), repo)
				// TODO: This could be sped up by using an appender or csv loader
				_, err := db.Exec("INSERT INTO commits (hash, contributor, author_date, project) VALUES (?, ?, ?, ?)",
					commit.Hash.String(),
					commit.Author.Name,
					commit.Author.When.Format(time.RFC3339),
					repo,
				)
				mut.Unlock()

				if err != nil && strings.Contains(err.Error(), "Duplicate key") {
					continue
				}

				if err != nil {
					errs <- err
					return
				}

				if err := worktree.Checkout(&git.CheckoutOptions{Hash: commit.Hash, Force: true}); err != nil {
					errs <- err
					return
				}

				input := make(chan FileState, runtime.NumCPU())
				go findFiles(filesystem, commit, input, errs)

				process(db, filesystem, input, errs)
				commitCompletedCallback(i+1, len(commits))
			}
		}()
	}

	for err := range errs {
		return err
	}
	wg.Wait()

	if err = db.CommitsAppender.Flush(); err != nil {
		return err
	}

	if err = db.FilestatesAppender.Flush(); err != nil {
		return err
	}

	return nil
}

func findFiles(filesystem billy.Filesystem, commit *object.Commit, files chan FileState, errs chan error) {
	defer close(files)

	stats, err := statsToMap(commit)
	if err != nil {
		errs <- err
		return
	}

	if err := util.Walk(filesystem, ".", func(path string, info fs.FileInfo, err error) error {
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

func process(db *database.DB, filesystem billy.Filesystem, input chan FileState, errs chan error) {
	var wg sync.WaitGroup

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range input {
				loc := file.Location
				f, err := filesystem.Open(loc)
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
	if err := db.FilestatesAppender.AppendRow(file.Commit.Hash.String(), file.Location, file.Language, int32(file.Code), int32(file.Comment), int32(file.Blank), int32(file.Complexity), int32(stat.Addition), int32(stat.Deletion)); err != nil {
		return err
	}
	// PERF: Could improve performance by configuring gc
	// PERF: Could improve performance by passing around more pointers instead of values, esp. for FileState
	return nil
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
