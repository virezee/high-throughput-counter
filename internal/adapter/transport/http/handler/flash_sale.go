package httphandler

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"high-throughput-counter/internal/application/port/inbound"
	"high-throughput-counter/internal/domain/flashsale"
)

type FlashSaleHandler struct {
	flashSaleUC inbound.FlashSaleUseCase
}

func NewFlashSaleHandler(flashSaleUC inbound.FlashSaleUseCase) *FlashSaleHandler {
	return &FlashSaleHandler{flashSaleUC}
}

func (h *FlashSaleHandler) RemainingStock(w http.ResponseWriter, r *http.Request) {
	saleID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	stock, err := h.flashSaleUC.RemainingStock(r.Context(), saleID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"stock": stock})
}

func (h *FlashSaleHandler) Checkout(w http.ResponseWriter, r *http.Request) {
	saleID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	userID, err := uuid.Parse(r.Header.Get("User-ID"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	orderID, err := h.flashSaleUC.Checkout(r.Context(), saleID, userID)
	if errors.Is(err, flashsale.ErrSoldOut) || errors.Is(err, flashsale.ErrAlreadyOrdered) {
		w.WriteHeader(http.StatusConflict)
		return
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"order_id": orderID})
}

func (h *FlashSaleHandler) FindOrder(w http.ResponseWriter, r *http.Request) {
	orderID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	order, err := h.flashSaleUC.FindOrder(r.Context(), orderID)
	if errors.Is(err, flashsale.ErrOrderNotFound) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":            order.ID,
		"flash_sale_id": order.FlashSaleID,
		"user_id":       order.UserID,
		"created_at":    order.CreatedAt,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(v)
	if err != nil {
		log.Error().Err(err).Send()
	}
}
