package inbound

import (
	"context"

	"github.com/google/uuid"
)

type LikeUseCase interface {
	LikePost(ctx context.Context, postID, userID uuid.UUID) (bool, error)
}
