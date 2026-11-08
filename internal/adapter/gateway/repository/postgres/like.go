package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"high-throughput-counter/internal/domain/like"
)

const saveBatchQuery = `
WITH inserted AS (
	INSERT INTO likes (post_id, user_id, created_at)
	SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::timestamptz[])
	ON CONFLICT DO NOTHING
	RETURNING post_id
)
SELECT post_id, count(*) FROM inserted GROUP BY post_id`

type likeRepository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *likeRepository {
	return &likeRepository{pool}
}

func (r *likeRepository) SaveBatch(
	ctx context.Context,
	likes []like.Like,
) (map[uuid.UUID]int64, error) {
	postIDs := make([]uuid.UUID, len(likes))
	userIDs := make([]uuid.UUID, len(likes))
	createdAts := make([]time.Time, len(likes))
	for i, l := range likes {
		postIDs[i] = l.PostID
		userIDs[i] = l.UserID
		createdAts[i] = l.CreatedAt
	}
	rows, err := r.pool.Query(ctx, saveBatchQuery, postIDs, userIDs, createdAts)
	if err != nil {
		return nil, fmt.Errorf("postgres save batch query: %w", err)
	}
	defer rows.Close()
	added := make(map[uuid.UUID]int64)
	for rows.Next() {
		var postID uuid.UUID
		var n int64
		err = rows.Scan(&postID, &n)
		if err != nil {
			return nil, fmt.Errorf("postgres save batch scan: %w", err)
		}
		added[postID] = n
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("postgres save batch rows: %w", err)
	}
	return added, nil
}
