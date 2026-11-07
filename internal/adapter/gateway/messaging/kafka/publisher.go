package kafkaproducer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type kafkaPublisher struct {
	client *kgo.Client
	topic  string
}

func NewKafkaPublisher(client *kgo.Client, topic string) *kafkaPublisher {
	return &kafkaPublisher{client, topic}
}

type likeMessage struct {
	PostID  uuid.UUID `json:"post_id"`
	UserID  uuid.UUID `json:"user_id"`
	LikedAt time.Time `json:"liked_at"`
}

func (p *kafkaPublisher) Publish(
	ctx context.Context,
	postID, userID uuid.UUID,
	likedAt time.Time,
) error {
	value, err := json.Marshal(likeMessage{PostID: postID, UserID: userID, LikedAt: likedAt})
	if err != nil {
		return err
	}
	record := &kgo.Record{
		Topic: p.topic,
		Key:   append(postID[:], userID[:]...),
		Value: value,
	}
	return p.client.ProduceSync(ctx, record).FirstErr()
}
