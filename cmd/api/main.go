package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"high-throughput-counter/internal/bootstrap"
)

const readHeaderTimeout = 5 * time.Second

func main() {
	time.Local = time.UTC
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	app, stopStreams, cleanup := bootstrap.NewApp("localhost:6379", "localhost:9092")
	defer cleanup()
	srv := &http.Server{
		Addr:              ":3000",
		Handler:           app,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	srv.RegisterOnShutdown(stopStreams)
	go func() {
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Send()
		}
	}()
	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := srv.Shutdown(sctx)
	if err != nil {
		log.Error().Err(err).Send()
	}
}
