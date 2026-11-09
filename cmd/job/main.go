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

const reconcileInterval = time.Minute

func main() {
	time.Local = time.UTC
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	reconcileLikes, cleanup := bootstrap.NewJob(
		ctx,
		"postgres://root:root@localhost:5432/high-throughput-counter",
		"localhost:6379",
	)
	defer cleanup()
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		err := reconcileLikes(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Send()
		}
	}
}
