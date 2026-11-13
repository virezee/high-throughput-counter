package router

import (
	"github.com/go-chi/chi/v5"

	httphandler "high-throughput-counter/internal/adapter/transport/http/handler"
)

func RegisterFlashSale(r chi.Router, h *httphandler.FlashSaleHandler) {
	r.Get("/flash-sales/{id}", h.RemainingStock)
	r.Post("/flash-sales/{id}/checkout", h.Checkout)
	r.Get("/orders/{id}", h.FindOrder)
}
