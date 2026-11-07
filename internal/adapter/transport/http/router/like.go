package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type LikeHandlers struct {
	Create http.Handler
	Count  http.Handler
}

func RegisterLike(r chi.Router, h LikeHandlers) {
	r.Method(http.MethodPost, "/posts/{id}/like", h.Create)
	r.Method(http.MethodGet, "/posts/{id}/likes", h.Count)
}
