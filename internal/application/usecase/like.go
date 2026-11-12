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

func (uc *likeUseCase) LikeCount(ctx context.Context, postID uuid.UUID) (int64, error) {
	return uc.counterRepo.Count(ctx, postID)
}

func (uc *likeUseCase) LikePost(ctx context.Context, postID, userID uuid.UUID) error {
	return uc.publisher.PublishLike(ctx, postID, userID, time.Now())
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

func (uc *likeUseCase) ReconcileLikes(ctx context.Context) error {
	counts, err := uc.likeRepo.CountByPost(ctx)
	if err != nil {
		return err
	}
	for postID, n := range counts {
		err = uc.counterRepo.Set(ctx, postID, n)
		if err != nil {
			return err
		}
	}
	return nil
}
