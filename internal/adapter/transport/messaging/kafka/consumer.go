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
	"high-throughput-counter/internal/domain/flashsale"
	"high-throughput-counter/internal/domain/like"
)

const (
	processTimeout = 10 * time.Second
	uuidSize       = len(uuid.UUID{})
	orderValueSize = 2 * uuidSize
)

type consumer struct {
	likeCl, flashSaleCl *kgo.Client
	likeUC              inbound.LikeUseCase
	flashSaleUC         inbound.FlashSaleUseCase
}

func NewConsumer(
	likeCl, flashSaleCl *kgo.Client,
	likeUC inbound.LikeUseCase,
	flashSaleUC inbound.FlashSaleUseCase,
) *consumer {
	return &consumer{likeCl, flashSaleCl, likeUC, flashSaleUC}
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

func (c *consumer) ProcessOrders(ctx context.Context) error {
	for {
		err := c.processOrderBatch(ctx)
		if errors.Is(err, kgo.ErrClientClosed) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (c *consumer) processBatch(ctx context.Context) error {
	fetches := c.likeCl.PollFetches(ctx)
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
	err = c.likeUC.ProcessLikes(pctx, likes)
	if err != nil {
		return err
	}
	err = c.likeCl.CommitUncommittedOffsets(pctx)
	if err != nil {
		return fmt.Errorf("kafka commit uncommitted offsets: %w", err)
	}
	return nil
}

func (c *consumer) processOrderBatch(ctx context.Context) error {
	fetches := c.flashSaleCl.PollFetches(ctx)
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
	orders := toOrders(fetches)
	if len(orders) == 0 {
		return nil
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), processTimeout)
	defer cancel()
	err = c.flashSaleUC.ProcessOrders(pctx, orders)
	if err != nil {
		return err
	}
	err = c.flashSaleCl.CommitUncommittedOffsets(pctx)
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

func toOrders(fetches kgo.Fetches) []flashsale.Order {
	orders := make([]flashsale.Order, 0, fetches.NumRecords())
	iter := fetches.RecordIter()
	for !iter.Done() {
		r := iter.Next()
		userID, err := uuid.FromBytes(r.Key)
		if err != nil {
			log.Warn().Err(err).Int64("offset", r.Offset).Msg("invalid user_id")
			continue
		}
		if len(r.Value) != orderValueSize {
			log.Warn().Int("size", len(r.Value)).Int64("offset", r.Offset).Msg("invalid order")
			continue
		}
		orders = append(orders, flashsale.Order{
			ID:          uuid.UUID(r.Value[:uuidSize]),
			FlashSaleID: uuid.UUID(r.Value[uuidSize:]),
			UserID:      userID,
			CreatedAt:   r.Timestamp,
		})
	}
	return orders
}
