//go:build integration

package e2e

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"high-throughput-counter/test/framework"
)

const (
	post1Likes = 50
	post2Likes = 30
)

func TestLikesAreCountedOnce(t *testing.T) {
	in := framework.StartInfra(t)
	srv := framework.StartApp(t, in)
	framework.StartWorker(t, in)
	pool := framework.NewPool(t, in)
	reconcileLikes := framework.StartJob(t, in)
	post1, post2 := uuid.New(), uuid.New()
	users := framework.NewUsers(post1Likes)
	framework.SendLikes(t, srv, post1, users)
	framework.SendLikes(t, srv, post1, users)
	framework.SendLikes(t, srv, post2, users[:post2Likes])
	want := map[uuid.UUID]int64{post1: post1Likes, post2: post2Likes}
	framework.AssertCounts(t, srv, pool, want)
	framework.FlushDragonfly(t, in)
	live, err := framework.LikeCount(t.Context(), srv, post1)
	require.NoError(t, err)
	assert.Zero(t, live)
	require.NoError(t, reconcileLikes(t.Context()))
	framework.AssertCounts(t, srv, pool, want)
}
