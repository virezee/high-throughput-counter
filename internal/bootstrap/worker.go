package bootstrap

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"

	"high-throughput-counter/internal/adapter/gateway/repository/dragonfly"
	"high-throughput-counter/internal/adapter/gateway/repository/postgres"
	"high-throughput-counter/internal/adapter/transport/messaging/kafka"
	"high-throughput-counter/internal/application/usecase"
)

func NewWorker(ctx context.Context) (processLikes func(context.Context) error, cleanup func()) {
	pool, err := pgxpool.New(ctx, "postgres://root:root@localhost:5432/high-throughput-counter")
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("like-worker"),
		kgo.ConsumeTopics("likes"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	likeRepo := postgres.NewRepository(pool)
	counterRepo := dragonfly.NewCounter(rdb)
	likeUC := usecase.NewLikeUseCase(nil, counterRepo, likeRepo)
	consumer := kafka.NewConsumer(cl, likeUC)
	cleanup = func() {
		cl.Close()
		err := rdb.Close()
		if err != nil {
			log.Error().Err(err).Send()
		}
		pool.Close()
	}
	return consumer.ProcessLikes, cleanup
}
