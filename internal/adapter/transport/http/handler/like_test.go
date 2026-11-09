package httphandler_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"high-throughput-counter/internal/adapter/transport/http/router"
	"high-throughput-counter/internal/domain/like"

	httphandler "high-throughput-counter/internal/adapter/transport/http/handler"
)

const (
	userIDHeader = "User-ID"
	invalidID    = "not-a-uuid"
)

const (
	firstCount int64 = 5
	nextCount  int64 = 8
)

var (
	testPostID = uuid.MustParse("0b6f4c3e-8f4a-4d1e-9c2a-000000000001")
	testUserID = uuid.MustParse("0b6f4c3e-8f4a-4d1e-9c2a-000000000101")
	errFake    = errors.New("fake error")
)

type fakeLikeUseCase struct {
	counts chan int64
	cancel context.CancelFunc
	err    error
	last   int64
	postID uuid.UUID
	userID uuid.UUID
}

func newCountingUseCase(cancel context.CancelFunc, counts ...int64) *fakeLikeUseCase {
	uc := &fakeLikeUseCase{counts: make(chan int64, len(counts)), cancel: cancel}
	for _, count := range counts {
		uc.counts <- count
	}
	return uc
}

func (f *fakeLikeUseCase) LikeCount(context.Context, uuid.UUID) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	select {
	case count := <-f.counts:
		f.last = count
	default:
		f.cancel()
	}
	return f.last, nil
}

func (f *fakeLikeUseCase) LikePost(_ context.Context, postID, userID uuid.UUID) error {
	f.postID = postID
	f.userID = userID
	return f.err
}

func (*fakeLikeUseCase) ProcessLikes(context.Context, []like.Like) error {
	return nil
}

func (*fakeLikeUseCase) ReconcileLikes(context.Context) error {
	return nil
}

func serve(
	uc *fakeLikeUseCase,
	shutdown <-chan struct{},
	req *http.Request,
) *httptest.ResponseRecorder {
	r := chi.NewRouter()
	router.RegisterLike(r, httphandler.NewLikeHandler(uc, shutdown))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func likePostRequest(ctx context.Context, id string) *http.Request {
	return httptest.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"/posts/"+id+"/like",
		http.NoBody,
	)
}

func likeCountRequest(ctx context.Context, id string) *http.Request {
	return httptest.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"/posts/"+id+"/likes",
		http.NoBody,
	)
}

func TestLikePostAccepted(t *testing.T) {
	uc := &fakeLikeUseCase{}
	req := likePostRequest(t.Context(), testPostID.String())
	req.Header.Set(userIDHeader, testUserID.String())
	rec := serve(uc, nil, req)
	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, testPostID, uc.postID)
	assert.Equal(t, testUserID, uc.userID)
}

func TestLikePostInvalidPostID(t *testing.T) {
	req := likePostRequest(t.Context(), invalidID)
	req.Header.Set(userIDHeader, testUserID.String())
	rec := serve(&fakeLikeUseCase{}, nil, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestLikePostInvalidUserID(t *testing.T) {
	req := likePostRequest(t.Context(), testPostID.String())
	req.Header.Set(userIDHeader, invalidID)
	rec := serve(&fakeLikeUseCase{}, nil, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestLikePostUseCaseError(t *testing.T) {
	req := likePostRequest(t.Context(), testPostID.String())
	req.Header.Set(userIDHeader, testUserID.String())
	rec := serve(&fakeLikeUseCase{err: errFake}, nil, req)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestLikeCountInvalidPostID(t *testing.T) {
	req := likeCountRequest(t.Context(), invalidID)
	rec := serve(&fakeLikeUseCase{}, nil, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestLikeCountSendsFirstCountAndStopsOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	uc := newCountingUseCase(cancel, firstCount)
	shutdown := make(chan struct{})
	close(shutdown)
	rec := serve(uc, shutdown, likeCountRequest(ctx, testPostID.String()))
	assert.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	assert.Equal(t, "data: 5\n\n", rec.Body.String())
}

func TestLikeCountSkipsUnchangedCount(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	uc := newCountingUseCase(cancel, firstCount, firstCount, nextCount)
	rec := serve(uc, nil, likeCountRequest(ctx, testPostID.String()))
	assert.Equal(t, "data: 5\n\ndata: 8\n\n", rec.Body.String())
}

func TestLikeCountStopsOnUseCaseError(t *testing.T) {
	req := likeCountRequest(t.Context(), testPostID.String())
	rec := serve(&fakeLikeUseCase{err: errFake}, nil, req)
	assert.Empty(t, rec.Body.String())
}
