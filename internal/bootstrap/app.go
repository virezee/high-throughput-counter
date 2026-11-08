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

func NewApp() (app *chi.Mux, cleanup func()) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	cl, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	counterRepo := dragonfly.NewCounter(rdb)
	producer := kafka.NewProducer(cl, "likes", "checkouts")
	likeUC := usecase.NewLikeUseCase(producer, counterRepo, nil)
	likeHandler := httphandler.NewLikeHandler(likeUC)
	r := chi.NewRouter()
	router.RegisterLike(r, likeHandler)
	cleanup = func() {
		cl.Close()
		err := rdb.Close()
		if err != nil {
			log.Error().Err(err).Send()
		}
	}
	return r, cleanup
}
