package httphandler

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"

	"high-throughput-counter/internal/application/port/inbound"
)

const likeCountInterval = 50 * time.Millisecond

const noLikeCount int64 = -1

var sseActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "sse_active_connections",
	Help: "Number of open like count SSE streams.",
})

type LikeHandler struct {
	likeUC   inbound.LikeUseCase
	shutdown <-chan struct{}
}

func NewLikeHandler(likeUC inbound.LikeUseCase, shutdown <-chan struct{}) *LikeHandler {
	return &LikeHandler{likeUC, shutdown}
}

func (h *LikeHandler) LikeCount(w http.ResponseWriter, r *http.Request) {
	postID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	sseActiveConnections.Inc()
	defer sseActiveConnections.Dec()
	ticker := time.NewTicker(likeCountInterval)
	defer ticker.Stop()
	last := noLikeCount
	for {
		last, err = h.sendLikeCount(w, r, postID, last)
		if err != nil {
			log.Error().Err(err).Str("post_id", postID.String()).Send()
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-h.shutdown:
			return
		case <-ticker.C:
		}
	}
}

func (h *LikeHandler) LikePost(w http.ResponseWriter, r *http.Request) {
	postID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	userID, err := uuid.Parse(r.Header.Get("User-ID"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Debug().Str("post_id", postID.String()).Str("user_id", userID.String()).Send()
	err = h.likeUC.LikePost(r.Context(), postID, userID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (h *LikeHandler) sendLikeCount(
	w http.ResponseWriter,
	r *http.Request,
	postID uuid.UUID,
	last int64,
) (int64, error) {
	count, err := h.likeUC.LikeCount(r.Context(), postID)
	if err != nil {
		return last, err
	}
	if count == last {
		return last, nil
	}
	//nolint:gosec // count is int64 formatted with %d, digits only
	_, err = fmt.Fprintf(w, "data: %d\n\n", count)
	if err != nil {
		return last, fmt.Errorf("http write like count: %w", err)
	}
	err = http.NewResponseController(w).Flush()
	if err != nil {
		return last, fmt.Errorf("http flush like count: %w", err)
	}
	return count, nil
}
