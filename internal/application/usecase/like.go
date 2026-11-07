package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"

	"high-throughput-counter/internal/application/port/outbound"
)

type likeUseCase struct {
	publisher outbound.Publisher
}

func NewLikeUseCase(publisher outbound.Publisher) *likeUseCase {
	return &likeUseCase{publisher}
}

func (u *likeUseCase) LikePost(ctx context.Context, postID, userID uuid.UUID) error {
	return u.publisher.Publish(ctx, postID, userID, time.Now())
}
