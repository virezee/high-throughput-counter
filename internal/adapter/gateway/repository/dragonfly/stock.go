package dragonfly

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"high-throughput-counter/internal/domain/flashsale"

	_ "embed"
)

const (
	decremented    int64 = 1
	soldOut        int64 = 0
	alreadyOrdered int64 = -1
)

//go:embed lua/decrement_stock.lua
var decrementStockLua string
var decrementScript = redis.NewScript(decrementStockLua)

type stockRepository struct {
	rdb *redis.Client
}

func NewStock(rdb *redis.Client) *stockRepository {
	return &stockRepository{rdb}
}

func (s *stockRepository) Decrement(ctx context.Context, saleID, userID uuid.UUID) error {
	keys := []string{stockKey(saleID), buyersKey(saleID)}
	result, err := decrementScript.Run(ctx, s.rdb, keys, userID.String()).Int64()
	if err != nil {
		return fmt.Errorf("dragonfly decrement stock: %w", err)
	}
	switch result {
	case decremented:
		return nil
	case soldOut:
		return flashsale.ErrSoldOut
	case alreadyOrdered:
		return flashsale.ErrAlreadyOrdered
	default:
		return fmt.Errorf("dragonfly decrement stock: unexpected result %d", result)
	}
}

func (s *stockRepository) Count(ctx context.Context, saleID uuid.UUID) (int64, error) {
	stock, err := s.rdb.Get(ctx, stockKey(saleID)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("dragonfly count stock: %w", err)
	}
	return stock, nil
}

func (s *stockRepository) Set(ctx context.Context, saleID uuid.UUID, n int64) error {
	if err := s.rdb.Set(ctx, stockKey(saleID), n, 0).Err(); err != nil {
		return fmt.Errorf("dragonfly set stock: %w", err)
	}
	return nil
}

func stockKey(saleID uuid.UUID) string {
	return "flash_sale:" + saleID.String() + ":stock"
}

func buyersKey(saleID uuid.UUID) string {
	return "flash_sale:" + saleID.String() + ":buyers"
}
