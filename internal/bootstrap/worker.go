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

func NewWorker(
	ctx context.Context,
	postgresDSN, dragonflyAddr, kafkaAddr string,
) (process func(context.Context) error, cleanup func()) {
	pool, err := pgxpool.New(ctx, postgresDSN)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	rdb := redis.NewClient(&redis.Options{Addr: dragonflyAddr})
	likeCl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAddr),
		kgo.ConsumerGroup("like-worker"),
		kgo.ConsumeTopics("likes"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	flashSaleCl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAddr),
		kgo.ConsumerGroup("checkout-worker"),
		kgo.ConsumeTopics("checkouts"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	likeRepo := postgres.NewRepository(pool)
	counterRepo := dragonfly.NewCounter(rdb)
	orderRepo := postgres.NewOrderRepository(pool)
	stockRepo := dragonfly.NewStock(rdb)
	likeUC := usecase.NewLikeUseCase(nil, counterRepo, likeRepo)
	flashSaleUC := usecase.NewFlashSaleUseCase(nil, stockRepo, orderRepo)
	consumer := kafka.NewConsumer(likeCl, flashSaleCl, likeUC, flashSaleUC)
	process = func(ctx context.Context) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		errCh := make(chan error, 2)
		go func() {
			errCh <- consumer.ProcessLikes(ctx)
		}()
		go func() {
			errCh <- consumer.ProcessOrders(ctx)
		}()
		err := <-errCh
		cancel()
		<-errCh
		return err
	}
	cleanup = func() {
		likeCl.Close()
		flashSaleCl.Close()
		err := rdb.Close()
		if err != nil {
			log.Error().Err(err).Send()
		}
		pool.Close()
	}
	return process, cleanup
}
