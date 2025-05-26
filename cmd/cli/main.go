package main

import (
	"flag"
	"os"
	"runtime/trace"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tim-hilt/codescene/internal"
	"github.com/tim-hilt/codescene/internal/database"
)

func parseFlags() ([]string, bool) {
	force := flag.Bool("f", false, "force re-analyzing of repo")
	flag.Parse()
	repos := flag.Args()
	return repos, *force
}

func commitCompletedCallback(curr, total uint64) {
	log.Info().Uint64("current", curr).Uint64("total", total).Msg("Processed commits")
}

func main() {
	f, _ := os.Create("trace.out")
	trace.Start(f)
	defer trace.Stop()

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	repos, force := parseFlags()
	if len(repos) == 0 {
		log.Fatal().Msg("No repository specified")
		return
	}

	db, err := database.Init()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize database")
		return
	}
	defer db.Close()

	for _, repo := range repos {
		start := time.Now()
		if err := internal.Analyze(db, repo, force, commitCompletedCallback); err != nil {
			log.Err(err).Msg("Failed to analyze")
			return
		}
		log.Info().Dur("duration", time.Since(start)).Str("repo", repo).Msg("Analysis completed")
	}
}
