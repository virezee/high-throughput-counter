package httphandler

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type OpsHandler struct {
	metrics http.Handler
}

func NewOpsHandler() *OpsHandler {
	return &OpsHandler{metrics: promhttp.Handler()}
}

func (h *OpsHandler) Healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (h *OpsHandler) Metrics(w http.ResponseWriter, r *http.Request) {
	h.metrics.ServeHTTP(w, r)
}
