package dragonfly

import (
	"context"
	"errors"
	"fmt"

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
	if err := c.rdb.IncrBy(ctx, likesKey(postID), n).Err(); err != nil {
		return fmt.Errorf("dragonfly increment: %w", err)
	}
	return nil
}

func (c *counterRepository) Count(ctx context.Context, postID uuid.UUID) (int64, error) {
	count, err := c.rdb.Get(ctx, likesKey(postID)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("dragonfly count: %w", err)
	}
	return count, nil
}

func (c *counterRepository) Set(ctx context.Context, postID uuid.UUID, n int64) error {
	if err := c.rdb.Set(ctx, likesKey(postID), n, 0).Err(); err != nil {
		return fmt.Errorf("dragonfly set: %w", err)
	}
	return nil
}

func likesKey(postID uuid.UUID) string {
	return "post:" + postID.String() + ":likes"
}
