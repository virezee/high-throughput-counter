package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"

	"high-throughput-counter/internal/application/port/outbound"
	"high-throughput-counter/internal/domain/like"
)

type likeUseCase struct {
	publisher   outbound.Publisher
	counterRepo like.CounterRepository
	likeRepo    like.LikeRepository
}

func NewLikeUseCase(
	publisher outbound.Publisher,
	counterRepo like.CounterRepository,
	likeRepo like.LikeRepository,
) *likeUseCase {
	return &likeUseCase{publisher, counterRepo, likeRepo}
}

func (uc *likeUseCase) LikePost(ctx context.Context, postID, userID uuid.UUID) error {
	return uc.publisher.Publish(ctx, postID, userID, time.Now())
}

func (uc *likeUseCase) ProcessLikes(ctx context.Context, likes []like.Like) error {
	added, err := uc.likeRepo.SaveBatch(ctx, likes)
	if err != nil {
		return err
	}
	for postID, n := range added {
		err = uc.counterRepo.Increment(ctx, postID, n)
		if err != nil {
			return err
		}
	}
	return nil
}
