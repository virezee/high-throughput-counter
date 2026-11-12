package flashsale

import (
	"time"

	"github.com/google/uuid"
)

type Order struct {
	ID          uuid.UUID
	FlashSaleID uuid.UUID
	UserID      uuid.UUID
	CreatedAt   time.Time
}
