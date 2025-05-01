package main

import (
	"flag"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tim-hilt/codescene/internal"
)

func parseFlags() (string, bool) {
	force := flag.Bool("f", false, "force re-analyzing of repo")
	flag.Parse()
	repo := flag.Arg(0)
	return repo, *force
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	repo, force := parseFlags()
	if repo == "" {
		log.Fatal().Msg("No repository specified")
		return
	}

	start := time.Now()
	if err := internal.Analyze(repo, force); err != nil {
		log.Err(err).Msg("Failed to analyze")
		return
	}
	log.Info().Dur("duration", time.Since(start)).Msg("Analysis completed")
}
