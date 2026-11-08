package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type producer struct {
	cl             *kgo.Client
	likeTopic      string
	flashSaleTopic string
}

func NewProducer(cl *kgo.Client, likeTopic, flashSaleTopic string) *producer {
	return &producer{cl, likeTopic, flashSaleTopic}
}

func (p *producer) Publish(
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
