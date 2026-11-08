package kafka

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"

	"high-throughput-counter/internal/application/port/inbound"
	"high-throughput-counter/internal/domain/like"
)

type consumer struct {
	cl *kgo.Client
	uc inbound.LikeUseCase
}

func NewConsumer(cl *kgo.Client, uc inbound.LikeUseCase) *consumer {
	return &consumer{cl, uc}
}

func (c *consumer) ProcessLikes(ctx context.Context) error {
	for {
		fetches := c.cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		err := fetches.Err()
		if err != nil {
			return fmt.Errorf("kafka poll fetches: %w", err)
		}
		likes := make([]like.Like, 0, fetches.NumRecords())
		iter := fetches.RecordIter()
		for !iter.Done() {
			r := iter.Next()
			userID, err := uuid.FromBytes(r.Key)
			if err != nil {
				log.Warn().Err(err).Int64("offset", r.Offset).Msg("invalid user_id")
				continue
			}
			postID, err := uuid.FromBytes(r.Value)
			if err != nil {
				log.Warn().Err(err).Int64("offset", r.Offset).Msg("invalid post_id")
				continue
			}
			likes = append(likes, like.Like{
				PostID:    postID,
				UserID:    userID,
				CreatedAt: r.Timestamp,
			})
		}
		if len(likes) == 0 {
			continue
		}
		err = c.uc.ProcessLikes(ctx, likes)
		if err != nil {
			return fmt.Errorf("kafka process likes: %w", err)
		}
		err = c.cl.CommitUncommittedOffsets(ctx)
		if err != nil {
			return fmt.Errorf("kafka commit offsets: %w", err)
		}
	}
}
