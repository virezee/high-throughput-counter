package kafka

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"

	"high-throughput-counter/internal/domain/like"
)

var invalidUUID = []byte("not-a-uuid")

func TestToLikesSkipsInvalidRecords(t *testing.T) {
	postID := uuid.MustParse("0b6f4c3e-8f4a-4d1e-9c2a-000000000001")
	userID := uuid.MustParse("0b6f4c3e-8f4a-4d1e-9c2a-000000000101")
	likedAt := time.Now()
	fetches := kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: "likes",
			Partitions: []kgo.FetchPartition{{
				Records: []*kgo.Record{
					{Key: userID[:], Value: postID[:], Timestamp: likedAt},
					{Key: invalidUUID, Value: postID[:], Timestamp: likedAt},
					{Key: userID[:], Value: invalidUUID, Timestamp: likedAt},
				},
			}},
		}},
	}}
	likes := toLikes(fetches)
	assert.Equal(t, []like.Like{{PostID: postID, UserID: userID, CreatedAt: likedAt}}, likes)
}
