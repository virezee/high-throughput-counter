package flashsale

import (
	"context"

	"github.com/google/uuid"
)

type OrderRepository interface {
	SaveAll(ctx context.Context, orders []Order) error
	FindByID(ctx context.Context, id uuid.UUID) (Order, error)
	CountRemainingStock(ctx context.Context) (map[uuid.UUID]int64, error)
}
type StockRepository interface {
	Decrement(ctx context.Context, saleID, userID uuid.UUID) error
	Count(ctx context.Context, saleID uuid.UUID) (int64, error)
	Set(ctx context.Context, saleID uuid.UUID, n int64) error
}
