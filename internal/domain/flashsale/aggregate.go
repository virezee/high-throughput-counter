package flashsale

import (
	"time"

	"github.com/google/uuid"
)

type Order struct {
	CreatedAt   time.Time
	ID          uuid.UUID
	FlashSaleID uuid.UUID
	UserID      uuid.UUID
}
