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

	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

const (
	kafkaImage         = "confluentinc/confluent-local:7.5.0"
	testLikeTopic      = "likes"
	testFlashSaleTopic = "checkouts"
	pollTimeout        = 30 * time.Second
	wantRecords        = 1
)

func TestPublish(t *testing.T) {
	ctr, err := tckafka.Run(t.Context(), kafkaImage, tckafka.WithClusterID("test-cluster"))
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	brokers, err := ctr.Brokers(t.Context())
	require.NoError(t, err)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumeTopics(testLikeTopic),
	)
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	postID := uuid.New()
	userID := uuid.New()
	likedAt := time.Now()
	err = NewProducer(
		cl,
		testLikeTopic,
		testFlashSaleTopic,
	).Publish(t.Context(), postID, userID, likedAt)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), pollTimeout)
	defer cancel()
	fetches := cl.PollFetches(ctx)
	require.NoError(t, fetches.Err())
	records := fetches.Records()
	require.Len(t, records, wantRecords)
	assert.Equal(t, userID[:], records[0].Key)
	assert.Equal(t, postID[:], records[0].Value)
	assert.Equal(t, likedAt.UnixMilli(), records[0].Timestamp.UnixMilli())
}
