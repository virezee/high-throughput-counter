package inbound

import (
	"context"

	"github.com/google/uuid"

	"high-throughput-counter/internal/domain/flashsale"
)

type FlashSaleUseCase interface {
	RemainingStock(ctx context.Context, saleID uuid.UUID) (int64, error)
	Checkout(ctx context.Context, saleID, userID uuid.UUID) (uuid.UUID, error)
	FindOrder(ctx context.Context, orderID uuid.UUID) (flashsale.Order, error)
	ProcessOrders(ctx context.Context, orders []flashsale.Order) error
	ReconcileStock(ctx context.Context) error
}
