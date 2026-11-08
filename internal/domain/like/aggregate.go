package like

import (
	"time"

	"github.com/google/uuid"
)

type Like struct {
	CreatedAt time.Time
	PostID    uuid.UUID
	UserID    uuid.UUID
}
