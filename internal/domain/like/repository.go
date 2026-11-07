package like

import (
	"context"

	"github.com/google/uuid"
)

type Counter interface {
	Increment(ctx context.Context, postID uuid.UUID, n int64) error
	Count(ctx context.Context, postID uuid.UUID) (int64, error)
}
