package inbound

import (
	"context"

	"github.com/google/uuid"

	"high-throughput-counter/internal/domain/like"
)

type LikeUseCase interface {
	LikeCount(ctx context.Context, postID uuid.UUID) (int64, error)
	LikePost(ctx context.Context, postID, userID uuid.UUID) error
	ProcessLikes(ctx context.Context, likes []like.Like) error
}
