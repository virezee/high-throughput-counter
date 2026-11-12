package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"high-throughput-counter/internal/application/port/outbound"
	"high-throughput-counter/internal/domain/flashsale"
)

type flashSaleUseCase struct {
	publisher outbound.OrderPublisher
	stockRepo flashsale.StockRepository
	orderRepo flashsale.OrderRepository
}

func NewFlashSaleUseCase(
	publisher outbound.OrderPublisher,
	stockRepo flashsale.StockRepository,
	orderRepo flashsale.OrderRepository,
) *flashSaleUseCase {
	return &flashSaleUseCase{publisher, stockRepo, orderRepo}
}

func (uc *flashSaleUseCase) RemainingStock(ctx context.Context, saleID uuid.UUID) (int64, error) {
	return uc.stockRepo.Count(ctx, saleID)
}

func (uc *flashSaleUseCase) Checkout(
	ctx context.Context,
	saleID, userID uuid.UUID,
) (uuid.UUID, error) {
	err := uc.stockRepo.Decrement(ctx, saleID, userID)
	if err != nil {
		return uuid.Nil, err
	}
	orderID, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, fmt.Errorf("new order id: %w", err)
	}
	order := flashsale.Order{
		ID:          orderID,
		FlashSaleID: saleID,
		UserID:      userID,
		CreatedAt:   time.Now(),
	}
	err = uc.publisher.PublishOrder(ctx, order)
	if err != nil {
		return uuid.Nil, err
	}
	return orderID, nil
}

func (uc *flashSaleUseCase) FindOrder(
	ctx context.Context,
	orderID uuid.UUID,
) (flashsale.Order, error) {
	return uc.orderRepo.FindByID(ctx, orderID)
}

func (uc *flashSaleUseCase) ProcessOrders(ctx context.Context, orders []flashsale.Order) error {
	return uc.orderRepo.SaveAll(ctx, orders)
}

func (uc *flashSaleUseCase) ReconcileStock(ctx context.Context) error {
	remaining, err := uc.orderRepo.CountRemainingStock(ctx)
	if err != nil {
		return err
	}
	for saleID, n := range remaining {
		err = uc.stockRepo.Set(ctx, saleID, n)
		if err != nil {
			return err
		}
	}
	return nil
}
