package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"high-throughput-counter/internal/domain/like"
)

const (
	post1Count int64 = 120
	post2Count int64 = 45
)

var (
	post1   = uuid.MustParse("0b6f4c3e-8f4a-4d1e-9c2a-000000000001")
	post2   = uuid.MustParse("0b6f4c3e-8f4a-4d1e-9c2a-000000000002")
	user1   = uuid.MustParse("0b6f4c3e-8f4a-4d1e-9c2a-000000000101")
	batch   = []like.Like{{PostID: post1, UserID: user1}}
	errFake = errors.New("fake error")
)

type fakePublisher struct {
	likedAt time.Time
	err     error
	postID  uuid.UUID
	userID  uuid.UUID
}

func (f *fakePublisher) PublishLike(
	_ context.Context,
	postID, userID uuid.UUID,
	likedAt time.Time,
) error {
	f.postID = postID
	f.userID = userID
	f.likedAt = likedAt
	return f.err
}

type fakeLikeRepo struct {
	added  map[uuid.UUID]int64
	counts map[uuid.UUID]int64
	err    error
}

func (f *fakeLikeRepo) SaveBatch(context.Context, []like.Like) (map[uuid.UUID]int64, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.added, nil
}

func (f *fakeLikeRepo) CountByPost(context.Context) (map[uuid.UUID]int64, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.counts, nil
}

type fakeCounterRepo struct {
	incremented map[uuid.UUID]int64
	set         map[uuid.UUID]int64
	err         error
	count       int64
}

func newFakeCounterRepo() *fakeCounterRepo {
	return &fakeCounterRepo{
		incremented: map[uuid.UUID]int64{},
		set:         map[uuid.UUID]int64{},
	}
}

func (f *fakeCounterRepo) Increment(_ context.Context, postID uuid.UUID, n int64) error {
	if f.err != nil {
		return f.err
	}
	f.incremented[postID] = n
	return nil
}

func (f *fakeCounterRepo) Count(context.Context, uuid.UUID) (int64, error) {
	return f.count, f.err
}

func (f *fakeCounterRepo) Set(_ context.Context, postID uuid.UUID, n int64) error {
	if f.err != nil {
		return f.err
	}
	f.set[postID] = n
	return nil
}

func TestLikeCount(t *testing.T) {
	counterRepo := newFakeCounterRepo()
	counterRepo.count = post1Count
	uc := NewLikeUseCase(nil, counterRepo, nil)
	count, err := uc.LikeCount(t.Context(), post1)
	require.NoError(t, err)
	assert.Equal(t, post1Count, count)
}

func TestLikePost(t *testing.T) {
	publisher := &fakePublisher{}
	uc := NewLikeUseCase(publisher, nil, nil)
	err := uc.LikePost(t.Context(), post1, user1)
	require.NoError(t, err)
	assert.Equal(t, post1, publisher.postID)
	assert.Equal(t, user1, publisher.userID)
	assert.False(t, publisher.likedAt.IsZero())
}

func TestProcessLikesIncrementsAddedPerPost(t *testing.T) {
	likeRepo := &fakeLikeRepo{added: map[uuid.UUID]int64{post1: post1Count, post2: post2Count}}
	counterRepo := newFakeCounterRepo()
	uc := NewLikeUseCase(nil, counterRepo, likeRepo)
	err := uc.ProcessLikes(t.Context(), batch)
	require.NoError(t, err)
	assert.Equal(t, likeRepo.added, counterRepo.incremented)
}

func TestProcessLikesStopsOnSaveBatchError(t *testing.T) {
	likeRepo := &fakeLikeRepo{err: errFake}
	counterRepo := newFakeCounterRepo()
	uc := NewLikeUseCase(nil, counterRepo, likeRepo)
	err := uc.ProcessLikes(t.Context(), batch)
	require.ErrorIs(t, err, errFake)
	assert.Empty(t, counterRepo.incremented)
}

func TestProcessLikesReturnsIncrementError(t *testing.T) {
	likeRepo := &fakeLikeRepo{added: map[uuid.UUID]int64{post1: post1Count}}
	counterRepo := newFakeCounterRepo()
	counterRepo.err = errFake
	uc := NewLikeUseCase(nil, counterRepo, likeRepo)
	err := uc.ProcessLikes(t.Context(), batch)
	require.ErrorIs(t, err, errFake)
}

func TestReconcileLikesSetsCountPerPost(t *testing.T) {
	likeRepo := &fakeLikeRepo{counts: map[uuid.UUID]int64{post1: post1Count, post2: post2Count}}
	counterRepo := newFakeCounterRepo()
	uc := NewLikeUseCase(nil, counterRepo, likeRepo)
	err := uc.ReconcileLikes(t.Context())
	require.NoError(t, err)
	assert.Equal(t, likeRepo.counts, counterRepo.set)
}

func TestReconcileLikesStopsOnCountByPostError(t *testing.T) {
	likeRepo := &fakeLikeRepo{err: errFake}
	counterRepo := newFakeCounterRepo()
	uc := NewLikeUseCase(nil, counterRepo, likeRepo)
	err := uc.ReconcileLikes(t.Context())
	require.ErrorIs(t, err, errFake)
	assert.Empty(t, counterRepo.set)
}

func TestReconcileLikesReturnsSetError(t *testing.T) {
	likeRepo := &fakeLikeRepo{counts: map[uuid.UUID]int64{post1: post1Count}}
	counterRepo := newFakeCounterRepo()
	counterRepo.err = errFake
	uc := NewLikeUseCase(nil, counterRepo, likeRepo)
	err := uc.ReconcileLikes(t.Context())
	require.ErrorIs(t, err, errFake)
}
