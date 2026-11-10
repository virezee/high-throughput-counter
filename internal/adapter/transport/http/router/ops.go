package router

import (
	"github.com/go-chi/chi/v5"

	httphandler "high-throughput-counter/internal/adapter/transport/http/handler"
)

func RegisterOps(r chi.Router, h *httphandler.OpsHandler) {
	r.Get("/healthz", h.Healthz)
	r.Get("/metrics", h.Metrics)
}
