package main

import (
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tim-hilt/codescene/internal"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.DurationFieldInteger = true
	zerolog.DurationFieldUnit = time.Microsecond

	start := time.Now()
	if err := internal.Analyze(); err != nil {
		log.Err(err).Msg("Failed to analyze")
		return
	}
	log.Info().Dur("duration", time.Since(start)).Msg("Analysis completed")
}
