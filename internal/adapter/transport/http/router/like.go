package router

import (
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	httphandler "high-throughput-counter/internal/adapter/transport/http/handler"
)

const likeConcurrencyLimit = 380

func RegisterLike(r chi.Router, h *httphandler.LikeHandler) {
	r.Get("/posts/{id}/likes", h.LikeCount)
	r.With(middleware.Throttle(likeConcurrencyLimit)).Post("/posts/{id}/like", h.LikePost)
}
