package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type OpsHandlers struct {
	Healthz http.Handler
	Metrics http.Handler
}

func RegisterOps(r chi.Router, h OpsHandlers) {
	r.Method(http.MethodGet, "/healthz", h.Healthz)
	r.Method(http.MethodGet, "/metrics", h.Metrics)
}
