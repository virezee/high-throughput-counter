package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"high-throughput-counter/internal/bootstrap"
)

func main() {
	time.Local = time.UTC
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	processLikes, cleanup := bootstrap.NewWorker(
		ctx,
		"postgres://root:root@localhost:5432/high-throughput-counter",
		"localhost:6379",
		"localhost:9092",
	)
	defer cleanup()
	err := processLikes(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error().Err(err).Send()
	}
}
