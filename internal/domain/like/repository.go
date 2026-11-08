package like

import (
	"context"

	"github.com/google/uuid"
)

type CounterRepository interface {
	Increment(ctx context.Context, postID uuid.UUID, n int64) error
	Count(ctx context.Context, postID uuid.UUID) (int64, error)
	Set(ctx context.Context, postID uuid.UUID, n int64) error
}
type LikeRepository interface {
	SaveBatch(ctx context.Context, likes []Like) (map[uuid.UUID]int64, error)
	CountByPost(ctx context.Context) (map[uuid.UUID]int64, error)
}
