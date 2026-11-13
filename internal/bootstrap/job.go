package bootstrap

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"

	"high-throughput-counter/internal/adapter/gateway/repository/dragonfly"
	"high-throughput-counter/internal/adapter/gateway/repository/postgres"
	"high-throughput-counter/internal/application/usecase"
)

func NewJob(
	ctx context.Context,
	postgresDSN, dragonflyAddr string,
) (reconcile func(context.Context) error, cleanup func()) {
	pool, err := pgxpool.New(ctx, postgresDSN)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	rdb := redis.NewClient(&redis.Options{Addr: dragonflyAddr})
	likeRepo := postgres.NewRepository(pool)
	counterRepo := dragonfly.NewCounter(rdb)
	orderRepo := postgres.NewOrderRepository(pool)
	stockRepo := dragonfly.NewStock(rdb)
	likeUC := usecase.NewLikeUseCase(nil, counterRepo, likeRepo)
	flashSaleUC := usecase.NewFlashSaleUseCase(nil, stockRepo, orderRepo)
	reconcile = func(ctx context.Context) error {
		return errors.Join(likeUC.ReconcileLikes(ctx), flashSaleUC.ReconcileStock(ctx))
	}
	cleanup = func() {
		err := rdb.Close()
		if err != nil {
			log.Error().Err(err).Send()
		}
		pool.Close()
	}
	return reconcile, cleanup
}
