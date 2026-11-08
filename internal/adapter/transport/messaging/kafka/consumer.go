package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"

	"high-throughput-counter/internal/application/port/inbound"
	"high-throughput-counter/internal/domain/like"
)

const processTimeout = 10 * time.Second

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
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fetches.IsClientClosed() {
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
			var userID, postID uuid.UUID
			userID, err = uuid.FromBytes(r.Key)
			if err != nil {
				log.Warn().Err(err).Int64("offset", r.Offset).Msg("invalid user_id")
				continue
			}
			postID, err = uuid.FromBytes(r.Value)
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
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), processTimeout)
		err = c.uc.ProcessLikes(pctx, likes)
		if err != nil {
			cancel()
			return err
		}
		err = c.cl.CommitUncommittedOffsets(pctx)
		cancel()
		if err != nil {
			return fmt.Errorf("kafka commit uncommitted offsets: %w", err)
		}
	}
}
