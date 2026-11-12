package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"high-throughput-counter/internal/domain/flashsale"
)

type producer struct {
	cl             *kgo.Client
	likeTopic      string
	flashSaleTopic string
}

func NewProducer(cl *kgo.Client, likeTopic, flashSaleTopic string) *producer {
	return &producer{cl, likeTopic, flashSaleTopic}
}

func (p *producer) PublishLike(
	ctx context.Context,
	postID, userID uuid.UUID,
	likedAt time.Time,
) error {
	record := &kgo.Record{
		Topic:     p.likeTopic,
		Key:       userID[:],
		Value:     postID[:],
		Timestamp: likedAt,
	}
	if err := p.cl.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("kafka publish: %w", err)
	}
	return nil
}

func (p *producer) PublishOrder(ctx context.Context, order flashsale.Order) error {
	record := &kgo.Record{
		Topic:     p.flashSaleTopic,
		Key:       order.UserID[:],
		Value:     append(order.ID[:], order.FlashSaleID[:]...),
		Timestamp: order.CreatedAt,
	}
	if err := p.cl.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("kafka publish: %w", err)
	}
	return nil
}
