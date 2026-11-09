//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"

	"high-throughput-counter/internal/domain/like"

	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

const (
	kafkaImage  = "confluentinc/confluent-local:7.5.0"
	testTopic   = "likes"
	testGroup   = "like-worker-test"
	waitTimeout = 30 * time.Second
)

type recordingLikeUseCase struct {
	batches chan []like.Like
}

func (u *recordingLikeUseCase) ProcessLikes(ctx context.Context, likes []like.Like) error {
	select {
	case u.batches <- likes:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*recordingLikeUseCase) LikeCount(context.Context, uuid.UUID) (int64, error) {
	return 0, nil
}

func (*recordingLikeUseCase) LikePost(context.Context, uuid.UUID, uuid.UUID) error {
	return nil
}

func (*recordingLikeUseCase) ReconcileLikes(context.Context) error {
	return nil
}

func startKafka(t *testing.T) []string {
	t.Helper()
	ctr, err := tckafka.Run(t.Context(), kafkaImage, tckafka.WithClusterID("test-cluster"))
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	brokers, err := ctr.Brokers(t.Context())
	require.NoError(t, err)
	return brokers
}

func newLike() like.Like {
	return like.Like{
		CreatedAt: time.UnixMilli(time.Now().UnixMilli()),
		PostID:    uuid.New(),
		UserID:    uuid.New(),
	}
}

func produce(t *testing.T, brokers []string, likes []like.Like) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.AllowAutoTopicCreation())
	require.NoError(t, err)
	defer cl.Close()
	for _, l := range likes {
		record := &kgo.Record{
			Topic:     testTopic,
			Key:       l.UserID[:],
			Value:     l.PostID[:],
			Timestamp: l.CreatedAt,
		}
		require.NoError(t, cl.ProduceSync(t.Context(), record).FirstErr())
	}
}

func runConsumer(t *testing.T, brokers []string, uc *recordingLikeUseCase) func() error {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(testGroup),
		kgo.ConsumeTopics(testTopic),
		kgo.DisableAutoCommit(),
	)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error)
	go func() {
		errCh <- NewConsumer(cl, uc).ProcessLikes(ctx)
	}()
	return func() error {
		cancel()
		runErr := <-errCh
		cl.Close()
		return runErr
	}
}

func collect(t *testing.T, uc *recordingLikeUseCase, want int) []like.Like {
	t.Helper()
	got := make([]like.Like, 0, want)
	timeout := time.After(waitTimeout)
	for len(got) < want {
		select {
		case batch := <-uc.batches:
			for _, l := range batch {
				l.CreatedAt = time.UnixMilli(l.CreatedAt.UnixMilli())
				got = append(got, l)
			}
		case <-timeout:
			require.FailNow(t, "timeout waiting for likes")
		}
	}
	return got
}

func TestProcessLikesCommitsProcessedOffsets(t *testing.T) {
	brokers := startKafka(t)
	uc := &recordingLikeUseCase{batches: make(chan []like.Like)}
	first := []like.Like{newLike(), newLike()}
	produce(t, brokers, first)
	stop := runConsumer(t, brokers, uc)
	assert.ElementsMatch(t, first, collect(t, uc, len(first)))
	require.ErrorIs(t, stop(), context.Canceled)
	second := []like.Like{newLike()}
	produce(t, brokers, second)
	stop = runConsumer(t, brokers, uc)
	assert.ElementsMatch(t, second, collect(t, uc, len(second)))
	require.ErrorIs(t, stop(), context.Canceled)
}
