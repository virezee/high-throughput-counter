package outbound

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Publisher interface {
	Publish(ctx context.Context, postID, userID uuid.UUID, likedAt time.Time) error
}
