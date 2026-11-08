package router

import (
	"github.com/go-chi/chi/v5"

	httphandler "high-throughput-counter/internal/adapter/transport/http/handler"
)

func RegisterLike(r chi.Router, h *httphandler.LikeHandler) {
	r.Post("/posts/{id}/like", h.LikePost)
	// r.Method("/posts/{id}/likes", h.Count)
}
