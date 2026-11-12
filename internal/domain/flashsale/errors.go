package flashsale

import "errors"

var (
	ErrOrderNotFound  = errors.New("order not found")
	ErrSoldOut        = errors.New("flash sale sold out")
	ErrAlreadyOrdered = errors.New("user already ordered")
)
