package kafka

import (
	"context"
	"errors"
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
		err := c.processBatch(ctx)
		if errors.Is(err, kgo.ErrClientClosed) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (c *consumer) processBatch(ctx context.Context) error {
	fetches := c.cl.PollFetches(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if fetches.IsClientClosed() {
		return kgo.ErrClientClosed
	}
	err := fetches.Err()
	if err != nil {
		return fmt.Errorf("kafka poll fetches: %w", err)
	}
	likes := toLikes(fetches)
	if len(likes) == 0 {
		return nil
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), processTimeout)
	defer cancel()
	err = c.uc.ProcessLikes(pctx, likes)
	if err != nil {
		return err
	}
	err = c.cl.CommitUncommittedOffsets(pctx)
	if err != nil {
		return fmt.Errorf("kafka commit uncommitted offsets: %w", err)
	}
	return nil
}

func toLikes(fetches kgo.Fetches) []like.Like {
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
	return likes
}
