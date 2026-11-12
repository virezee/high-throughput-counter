package httphandler

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

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
	writeJSON(w, http.StatusOK, stockResponse{Stock: stock})
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
	writeJSON(w, http.StatusAccepted, checkoutResponse{OrderID: orderID})
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
	writeJSON(w, http.StatusOK, orderResponse{
		ID:          order.ID,
		FlashSaleID: order.FlashSaleID,
		UserID:      order.UserID,
		CreatedAt:   order.CreatedAt,
	})
}

type stockResponse struct {
	Stock int64 `json:"stock"`
}

type checkoutResponse struct {
	OrderID uuid.UUID `json:"order_id"`
}

type orderResponse struct {
	ID          uuid.UUID `json:"id"`
	FlashSaleID uuid.UUID `json:"flash_sale_id"`
	UserID      uuid.UUID `json:"user_id"`
	CreatedAt   time.Time `json:"created_at"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(v)
	if err != nil {
		log.Error().Err(err).Send()
	}
}
