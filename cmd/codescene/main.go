package main

import (
	"github.com/rs/zerolog"
	"github.com/tim-hilt/codescene/internal"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	internal.Analyze()
}
