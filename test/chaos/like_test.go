//go:build chaos

package chaos

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"high-throughput-counter/test/framework"
)

const (
	likesBefore         = 20
	likesDuring         = 20
	likesAfter          = 20
	likesWhileKafkaDown = 5
)

const unavailableTimeout = 2 * time.Second

func TestDragonflyDownWhileProcessing(t *testing.T) {
	in := framework.StartInfra(t)
	srv := framework.StartApp(t, in)
	worker := framework.StartWorker(t, in)
	pool := framework.NewPool(t, in)
	reconcileLikes := framework.StartJob(t, in)
	postID := uuid.New()
	framework.SendLikes(t, srv, postID, framework.NewUsers(likesBefore))
	framework.AssertCounts(t, srv, pool, map[uuid.UUID]int64{postID: likesBefore})
	framework.Pause(t, in.Dragonfly)
	framework.SendLikes(t, srv, postID, framework.NewUsers(likesDuring))
	err := worker.Wait(t)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.Canceled)
	framework.Unpause(t, in.Dragonfly)
	worker = framework.StartWorker(t, in)
	total := int64(likesBefore + likesDuring)
	framework.AssertStored(t, pool, postID, total)
	require.ErrorIs(t, worker.Stop(), context.Canceled)
	live, err := framework.LikeCount(t.Context(), srv, postID)
	require.NoError(t, err)
	assert.Less(t, live, total)
	require.NoError(t, reconcileLikes(t.Context()))
	framework.AssertCounts(t, srv, pool, map[uuid.UUID]int64{postID: total})
}

func TestPostgresDownWhileProcessing(t *testing.T) {
	in := framework.StartInfra(t)
	srv := framework.StartApp(t, in)
	worker := framework.StartWorker(t, in)
	pool := framework.NewPool(t, in)
	reconcileLikes := framework.StartJob(t, in)
	postID := uuid.New()
	framework.SendLikes(t, srv, postID, framework.NewUsers(likesBefore))
	framework.AssertCounts(t, srv, pool, map[uuid.UUID]int64{postID: likesBefore})
	framework.Pause(t, in.Postgres)
	framework.SendLikes(t, srv, postID, framework.NewUsers(likesDuring))
	err := worker.Wait(t)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.Canceled)
	framework.Unpause(t, in.Postgres)
	worker = framework.StartWorker(t, in)
	total := int64(likesBefore + likesDuring)
	framework.AssertStored(t, pool, postID, total)
	require.ErrorIs(t, worker.Stop(), context.Canceled)
	require.NoError(t, reconcileLikes(t.Context()))
	framework.AssertCounts(t, srv, pool, map[uuid.UUID]int64{postID: total})
}

func TestKafkaDownWhileReceiving(t *testing.T) {
	in := framework.StartInfra(t)
	srv := framework.StartApp(t, in)
	worker := framework.StartWorker(t, in)
	pool := framework.NewPool(t, in)
	reconcileLikes := framework.StartJob(t, in)
	postID := uuid.New()
	framework.Pause(t, in.Kafka)
	for _, userID := range framework.NewUsers(likesWhileKafkaDown) {
		ctx, cancel := context.WithTimeout(t.Context(), unavailableTimeout)
		status, err := framework.PostLike(ctx, srv, postID, userID)
		cancel()
		if err == nil {
			assert.NotEqual(t, http.StatusAccepted, status)
		}
	}
	framework.Unpause(t, in.Kafka)
	t.Logf("worker stopped: %v", worker.Stop())
	worker = framework.StartWorker(t, in)
	after := framework.NewUsers(likesAfter)
	framework.SendLikes(t, srv, postID, after)
	framework.AssertStoredUsers(t, pool, postID, after)
	require.ErrorIs(t, worker.Stop(), context.Canceled)
	stored, err := framework.StoredCount(t.Context(), pool, postID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, stored, int64(likesAfter))
	assert.LessOrEqual(t, stored, int64(likesWhileKafkaDown+likesAfter))
	require.NoError(t, reconcileLikes(t.Context()))
	framework.AssertCounts(t, srv, pool, map[uuid.UUID]int64{postID: stored})
}
