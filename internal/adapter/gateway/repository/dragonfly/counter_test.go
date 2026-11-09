//go:build integration

package dragonfly

import (
	"testing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	dragonflyImage   = "docker.dragonflydb.io/dragonflydb/dragonfly"
	dragonflyPort    = "6379/tcp"
	dragonflyThreads = "--proactor_threads=8"
)

const (
	firstIncrement  int64 = 3
	secondIncrement int64 = 4
	reconciledCount int64 = 10
)

func newCounter(t *testing.T) *counterRepository {
	t.Helper()
	ctr, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        dragonflyImage,
			Cmd:          []string{dragonflyThreads},
			ExposedPorts: []string{dragonflyPort},
			WaitingFor:   wait.ForListeningPort(dragonflyPort),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	addr, err := ctr.PortEndpoint(t.Context(), dragonflyPort, "")
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() {
		assert.NoError(t, rdb.Close())
	})
	return NewCounter(rdb)
}

func TestCountMissingKeyReturnsZero(t *testing.T) {
	counter := newCounter(t)
	count, err := counter.Count(t.Context(), uuid.New())
	require.NoError(t, err)
	assert.Zero(t, count)
}

func TestIncrementAddsToCount(t *testing.T) {
	counter := newCounter(t)
	postID := uuid.New()
	require.NoError(t, counter.Increment(t.Context(), postID, firstIncrement))
	require.NoError(t, counter.Increment(t.Context(), postID, secondIncrement))
	count, err := counter.Count(t.Context(), postID)
	require.NoError(t, err)
	assert.Equal(t, firstIncrement+secondIncrement, count)
}

func TestSetOverwritesCount(t *testing.T) {
	counter := newCounter(t)
	postID := uuid.New()
	require.NoError(t, counter.Increment(t.Context(), postID, firstIncrement))
	require.NoError(t, counter.Set(t.Context(), postID, reconciledCount))
	count, err := counter.Count(t.Context(), postID)
	require.NoError(t, err)
	assert.Equal(t, reconciledCount, count)
}
