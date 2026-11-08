package httphandler

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"high-throughput-counter/internal/application/port/inbound"
)

const likeCountInterval = 50 * time.Millisecond

type LikeHandler struct {
	likeUC inbound.LikeUseCase
}

func NewLikeHandler(likeUC inbound.LikeUseCase) *LikeHandler {
	return &LikeHandler{likeUC}
}

func (h *LikeHandler) LikeCount(w http.ResponseWriter, r *http.Request) {
	postID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ticker := time.NewTicker(likeCountInterval)
	defer ticker.Stop()
	var last int64
	sent := false
	for {
		last, sent, err = h.sendLikeCount(w, r, postID, last, sent)
		if err != nil {
			log.Error().Err(err).Str("post_id", postID.String()).Send()
			return
		}
		select {
		case <-r.Context().Done():
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
	sent bool,
) (int64, bool, error) {
	count, err := h.likeUC.LikeCount(r.Context(), postID)
	if err != nil {
		return last, sent, err
	}
	if sent && count == last {
		return last, sent, nil
	}
	_, err = fmt.Fprintf(w, "data: %d\n\n", count)
	if err != nil {
		return last, sent, fmt.Errorf("http write like count: %w", err)
	}
	err = http.NewResponseController(w).Flush()
	if err != nil {
		return last, sent, fmt.Errorf("http flush like count: %w", err)
	}
	return count, true, nil
}
