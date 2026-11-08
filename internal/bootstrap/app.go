package bootstrap

import (
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"

	"high-throughput-counter/internal/adapter/gateway/messaging/kafka"
	"high-throughput-counter/internal/adapter/transport/http/router"
	"high-throughput-counter/internal/application/usecase"

	httphandler "high-throughput-counter/internal/adapter/transport/http/handler"
)

func NewApp() (*chi.Mux, func()) {
	cl, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	producer := kafka.NewProducer(cl, "likes", "checkouts")
	likeUC := usecase.NewLikeUseCase(producer)
	likeHandler := httphandler.NewLikeHandler(likeUC)
	r := chi.NewRouter()
	router.RegisterLike(r, likeHandler)
	return r, cl.Close
}
