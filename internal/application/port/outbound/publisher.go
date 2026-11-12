package outbound

import (
	"context"
	"time"

	"github.com/google/uuid"

	"high-throughput-counter/internal/domain/flashsale"
)

type LikePublisher interface {
	PublishLike(ctx context.Context, postID, userID uuid.UUID, likedAt time.Time) error
}

type OrderPublisher interface {
	PublishOrder(ctx context.Context, order flashsale.Order) error
}
