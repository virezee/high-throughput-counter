package bootstrap

import (
	"context"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"

	"high-throughput-counter/internal/adapter/gateway/messaging/kafka"
	"high-throughput-counter/internal/adapter/gateway/repository/dragonfly"
	"high-throughput-counter/internal/adapter/gateway/repository/postgres"
	"high-throughput-counter/internal/adapter/transport/http/router"
	"high-throughput-counter/internal/application/usecase"

	httphandler "high-throughput-counter/internal/adapter/transport/http/handler"
)

func NewApp(
	ctx context.Context,
	postgresDSN, dragonflyAddr, kafkaAddr string,
) (app *chi.Mux, stopStreams, cleanup func()) {
	pool, err := pgxpool.New(ctx, postgresDSN)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	rdb := redis.NewClient(&redis.Options{Addr: dragonflyAddr})
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr))
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	counterRepo := dragonfly.NewCounter(rdb)
	stockRepo := dragonfly.NewStock(rdb)
	orderRepo := postgres.NewOrderRepository(pool)
	producer := kafka.NewProducer(cl, "likes", "checkouts")
	likeUC := usecase.NewLikeUseCase(producer, counterRepo, nil)
	flashSaleUC := usecase.NewFlashSaleUseCase(producer, stockRepo, orderRepo)
	shutdown := make(chan struct{})
	likeHandler := httphandler.NewLikeHandler(likeUC, shutdown)
	flashSaleHandler := httphandler.NewFlashSaleHandler(flashSaleUC)
	r := chi.NewRouter()
	router.RegisterLike(r, likeHandler)
	router.RegisterFlashSale(r, flashSaleHandler)
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
		pool.Close()
	}
	return r, stopStreams, cleanup
}
