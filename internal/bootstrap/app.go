package bootstrap

import (
	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"

	"high-throughput-counter/internal/adapter/gateway/messaging/kafka"
	"high-throughput-counter/internal/adapter/gateway/repository/dragonfly"
	"high-throughput-counter/internal/adapter/transport/http/router"
	"high-throughput-counter/internal/application/usecase"

	httphandler "high-throughput-counter/internal/adapter/transport/http/handler"
)

func NewApp(dragonflyAddr, kafkaAddr string) (app *chi.Mux, stopStreams, cleanup func()) {
	rdb := redis.NewClient(&redis.Options{Addr: dragonflyAddr})
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr))
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	counterRepo := dragonfly.NewCounter(rdb)
	producer := kafka.NewProducer(cl, "likes", "checkouts")
	likeUC := usecase.NewLikeUseCase(producer, counterRepo, nil)
	shutdown := make(chan struct{})
	likeHandler := httphandler.NewLikeHandler(likeUC, shutdown)
	r := chi.NewRouter()
	router.RegisterLike(r, likeHandler)
	router.RegisterOps(r, httphandler.NewOpsHandler())
	stopStreams = func() {
		close(shutdown)
	}
	cleanup = func() {
		cl.Close()
		err := rdb.Close()
		if err != nil {
			log.Error().Err(err).Send()
		}
	}
	return r, stopStreams, cleanup
}
