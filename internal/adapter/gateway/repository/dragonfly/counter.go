package dragonfly

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type counterRepository struct {
	rdb *redis.Client
}

func NewCounter(rdb *redis.Client) *counterRepository {
	return &counterRepository{rdb}
}

func (c *counterRepository) Increment(ctx context.Context, postID uuid.UUID, n int64) error {
	return c.rdb.IncrBy(ctx, likesKey(postID), n).Err()
}

func (c *counterRepository) Count(ctx context.Context, postID uuid.UUID) (int64, error) {
	count, err := c.rdb.Get(ctx, likesKey(postID)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return count, err
}

func likesKey(postID uuid.UUID) string {
	return "post:" + postID.String() + ":likes"
}
