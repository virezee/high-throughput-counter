package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type FlashSaleHandlers struct {
	Get      http.Handler
	Checkout http.Handler
	GetOrder http.Handler
	PayOrder http.Handler
}

func RegisterFlashSale(r chi.Router, h FlashSaleHandlers) {
	r.Method(http.MethodGet, "/flash-sales/{id}", h.Get)
	r.Method(http.MethodPost, "/flash-sales/{id}/checkout", h.Checkout)
	r.Method(http.MethodGet, "/orders/{id}", h.GetOrder)
	r.Method(http.MethodPost, "/orders/{id}/pay", h.PayOrder)
}
