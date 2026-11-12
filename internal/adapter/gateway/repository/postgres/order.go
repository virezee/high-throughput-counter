package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"high-throughput-counter/internal/domain/flashsale"
)

const saveAllOrdersQuery = `
INSERT INTO orders (id, flash_sale_id, user_id, created_at)
SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::timestamptz[])
ON CONFLICT DO NOTHING`

const findOrderByIDQuery = `SELECT id, flash_sale_id, user_id, created_at FROM orders WHERE id = $1`

const countRemainingStockQuery = `
SELECT s.id, s.stock - count(o.id)
FROM flash_sales s
LEFT JOIN orders o ON o.flash_sale_id = s.id
GROUP BY s.id, s.stock`

type orderRepository struct {
	pool *pgxpool.Pool
}

func NewOrderRepository(pool *pgxpool.Pool) *orderRepository {
	return &orderRepository{pool}
}

func (r *orderRepository) SaveAll(ctx context.Context, orders []flashsale.Order) error {
	ids := make([]uuid.UUID, len(orders))
	saleIDs := make([]uuid.UUID, len(orders))
	userIDs := make([]uuid.UUID, len(orders))
	createdAts := make([]time.Time, len(orders))
	for i, o := range orders {
		ids[i] = o.ID
		saleIDs[i] = o.FlashSaleID
		userIDs[i] = o.UserID
		createdAts[i] = o.CreatedAt
	}
	_, err := r.pool.Exec(ctx, saveAllOrdersQuery, ids, saleIDs, userIDs, createdAts)
	if err != nil {
		return fmt.Errorf("postgres save all orders exec: %w", err)
	}
	return nil
}

func (r *orderRepository) FindByID(ctx context.Context, id uuid.UUID) (flashsale.Order, error) {
	var o flashsale.Order
	err := r.pool.QueryRow(ctx, findOrderByIDQuery, id).
		Scan(&o.ID, &o.FlashSaleID, &o.UserID, &o.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return flashsale.Order{}, flashsale.ErrOrderNotFound
	}
	if err != nil {
		return flashsale.Order{}, fmt.Errorf("postgres find order by id scan: %w", err)
	}
	return o, nil
}

func (r *orderRepository) CountRemainingStock(ctx context.Context) (map[uuid.UUID]int64, error) {
	rows, err := r.pool.Query(ctx, countRemainingStockQuery)
	if err != nil {
		return nil, fmt.Errorf("postgres count remaining stock query: %w", err)
	}
	defer rows.Close()
	remaining := make(map[uuid.UUID]int64)
	for rows.Next() {
		var saleID uuid.UUID
		var n int64
		err = rows.Scan(&saleID, &n)
		if err != nil {
			return nil, fmt.Errorf("postgres count remaining stock scan: %w", err)
		}
		remaining[saleID] = n
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("postgres count remaining stock rows: %w", err)
	}
	return remaining, nil
}
