//go:build integration

package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"high-throughput-counter/internal/domain/like"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	postgresImage = "postgres"
	migrationPath = "../../../../../migrations/000001_create_likes.up.sql"
)

const (
	post1Added int64 = 2
	post2Added int64 = 1
)

func newRepository(t *testing.T) *likeRepository {
	t.Helper()
	ctr, err := tcpostgres.Run(
		t.Context(),
		postgresImage,
		tcpostgres.WithInitScripts(migrationPath),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	dsn, err := ctr.ConnectionString(t.Context(), "sslmode=disable")
	require.NoError(t, err)
	pool, err := pgxpool.New(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return NewRepository(pool)
}

func TestSaveBatchCountsOnlyNewLikes(t *testing.T) {
	repo := newRepository(t)
	post1, post2 := uuid.New(), uuid.New()
	user1, user2 := uuid.New(), uuid.New()
	now := time.Now()
	added, err := repo.SaveBatch(t.Context(), []like.Like{
		{PostID: post1, UserID: user1, CreatedAt: now},
		{PostID: post1, UserID: user1, CreatedAt: now},
		{PostID: post1, UserID: user2, CreatedAt: now},
		{PostID: post2, UserID: user1, CreatedAt: now},
	})
	require.NoError(t, err)
	assert.Equal(t, map[uuid.UUID]int64{post1: post1Added, post2: post2Added}, added)
	added, err = repo.SaveBatch(t.Context(), []like.Like{
		{PostID: post1, UserID: user2, CreatedAt: now},
		{PostID: post2, UserID: user2, CreatedAt: now},
	})
	require.NoError(t, err)
	assert.Equal(t, map[uuid.UUID]int64{post2: post2Added}, added)
}

func TestCountByPost(t *testing.T) {
	repo := newRepository(t)
	post1, post2 := uuid.New(), uuid.New()
	user1, user2 := uuid.New(), uuid.New()
	now := time.Now()
	_, err := repo.SaveBatch(t.Context(), []like.Like{
		{PostID: post1, UserID: user1, CreatedAt: now},
		{PostID: post1, UserID: user2, CreatedAt: now},
		{PostID: post2, UserID: user1, CreatedAt: now},
	})
	require.NoError(t, err)
	counts, err := repo.CountByPost(t.Context())
	require.NoError(t, err)
	assert.Equal(t, map[uuid.UUID]int64{post1: post1Added, post2: post2Added}, counts)
}
