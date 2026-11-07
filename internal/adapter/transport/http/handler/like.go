package handler

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"high-throughput-counter/internal/application/port/inbound"
)

type LikeHandler struct {
	likeUC inbound.LikeUseCase
}

func NewLikeHandler(likeUC inbound.LikeUseCase) *LikeHandler {
	return &LikeHandler{likeUC}
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
	liked, err := h.likeUC.LikePost(r.Context(), postID, userID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if !liked {
		w.WriteHeader(http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
