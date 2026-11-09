//go:build integration || chaos

package framework

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"high-throughput-counter/internal/bootstrap"

	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	postgresImage    = "postgres"
	dragonflyImage   = "docker.dragonflydb.io/dragonflydb/dragonfly"
	dragonflyPort    = "6379/tcp"
	dragonflyThreads = "--proactor_threads=8"
	kafkaImage       = "confluentinc/confluent-local:7.5.0"
	likeTopic        = "likes"
	userIDHeader     = "User-ID"
	ssePrefix        = "data: "
)

const (
	likeTopicPartitions = 3
	likeTopicReplicas   = 1
	decimalBase         = 10
	int64Bits           = 64
)

const (
	waitTimeout    = 60 * time.Second
	pollInterval   = 500 * time.Millisecond
	requestTimeout = 5 * time.Second
)

type Infra struct {
	Postgres      testcontainers.Container
	Dragonfly     testcontainers.Container
	Kafka         testcontainers.Container
	PostgresDSN   string
	DragonflyAddr string
	KafkaAddr     string
}

type Worker struct {
	cancel  context.CancelFunc
	cleanup func()
	done    chan struct{}
	err     error
	stopped bool
}

func migrationPath() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "migrations", "000001_create_likes.up.sql")
}

func startPostgres(t *testing.T) (testcontainers.Container, string) {
	t.Helper()
	ctr, err := tcpostgres.Run(
		t.Context(),
		postgresImage,
		tcpostgres.WithInitScripts(migrationPath()),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	dsn, err := ctr.ConnectionString(t.Context(), "sslmode=disable")
	require.NoError(t, err)
	return ctr, dsn
}

func startDragonfly(t *testing.T) (testcontainers.Container, string) {
	t.Helper()
	ctr, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        dragonflyImage,
			Cmd:          []string{dragonflyThreads},
			ExposedPorts: []string{dragonflyPort},
			WaitingFor:   wait.ForListeningPort(dragonflyPort),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	addr, err := ctr.PortEndpoint(t.Context(), dragonflyPort, "")
	require.NoError(t, err)
	return ctr, addr
}

func startKafka(t *testing.T) (testcontainers.Container, string) {
	t.Helper()
	ctr, err := tckafka.Run(t.Context(), kafkaImage, tckafka.WithClusterID("test-cluster"))
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	brokers, err := ctr.Brokers(t.Context())
	require.NoError(t, err)
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer cl.Close()
	resp, err := kadm.NewClient(cl).CreateTopic(
		t.Context(),
		likeTopicPartitions,
		likeTopicReplicas,
		nil,
		likeTopic,
	)
	require.NoError(t, err)
	require.NoError(t, resp.Err)
	return ctr, brokers[0]
}

func StartInfra(t *testing.T) *Infra {
	t.Helper()
	in := &Infra{}
	in.Postgres, in.PostgresDSN = startPostgres(t)
	in.Dragonfly, in.DragonflyAddr = startDragonfly(t)
	in.Kafka, in.KafkaAddr = startKafka(t)
	return in
}

func StartApp(t *testing.T, in *Infra) *httptest.Server {
	t.Helper()
	app, stopStreams, cleanup := bootstrap.NewApp(in.DragonflyAddr, in.KafkaAddr)
	t.Cleanup(cleanup)
	srv := httptest.NewServer(app)
	t.Cleanup(srv.Close)
	t.Cleanup(stopStreams)
	return srv
}

func StartWorker(t *testing.T, in *Infra) *Worker {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	processLikes, cleanup := bootstrap.NewWorker(
		ctx,
		in.PostgresDSN,
		in.DragonflyAddr,
		in.KafkaAddr,
	)
	w := &Worker{cancel: cancel, cleanup: cleanup, done: make(chan struct{})}
	go func() {
		w.err = processLikes(ctx)
		close(w.done)
	}()
	t.Cleanup(func() {
		if !w.stopped {
			assert.ErrorIs(t, w.Stop(), context.Canceled)
		}
	})
	return w
}

func (w *Worker) Stop() error {
	w.cancel()
	<-w.done
	return w.finish()
}

func (w *Worker) Wait(t *testing.T) error {
	t.Helper()
	select {
	case <-w.done:
	case <-time.After(waitTimeout):
		require.FailNow(t, "worker did not stop")
	}
	return w.finish()
}

func (w *Worker) finish() error {
	if !w.stopped {
		w.stopped = true
		w.cancel()
		w.cleanup()
	}
	return w.err
}

func StartJob(t *testing.T, in *Infra) func(context.Context) error {
	t.Helper()
	reconcileLikes, cleanup := bootstrap.NewJob(t.Context(), in.PostgresDSN, in.DragonflyAddr)
	t.Cleanup(cleanup)
	return reconcileLikes
}

func NewPool(t *testing.T, in *Infra) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), in.PostgresDSN)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func NewUsers(n int) []uuid.UUID {
	users := make([]uuid.UUID, n)
	for i := range users {
		users[i] = uuid.New()
	}
	return users
}

func PostLike(ctx context.Context, srv *httptest.Server, postID, userID uuid.UUID) (int, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		srv.URL+"/posts/"+postID.String()+"/like",
		http.NoBody,
	)
	if err != nil {
		return 0, err
	}
	req.Header.Set(userIDHeader, userID.String())
	resp, err := srv.Client().Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, resp.Body.Close()
}

func SendLikes(t *testing.T, srv *httptest.Server, postID uuid.UUID, users []uuid.UUID) {
	t.Helper()
	for _, userID := range users {
		status, err := PostLike(t.Context(), srv, postID, userID)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, status)
	}
}

func LikeCount(ctx context.Context, srv *httptest.Server, postID uuid.UUID) (int64, error) {
	rctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(
		rctx,
		http.MethodGet,
		srv.URL+"/posts/"+postID.String()+"/likes",
		http.NoBody,
	)
	if err != nil {
		return 0, err
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		return 0, err
	}
	line, readErr := bufio.NewReader(resp.Body).ReadString('\n')
	closeErr := resp.Body.Close()
	err = errors.Join(readErr, closeErr)
	if err != nil {
		return 0, err
	}
	value := strings.TrimSpace(strings.TrimPrefix(line, ssePrefix))
	return strconv.ParseInt(value, decimalBase, int64Bits)
}

func StoredCount(ctx context.Context, pool *pgxpool.Pool, postID uuid.UUID) (int64, error) {
	var count int64
	err := pool.QueryRow(ctx, "SELECT count(*) FROM likes WHERE post_id = $1", postID).Scan(&count)
	return count, err
}

func storedUsersCount(
	ctx context.Context,
	pool *pgxpool.Pool,
	postID uuid.UUID,
	users []uuid.UUID,
) (int64, error) {
	var count int64
	err := pool.QueryRow(
		ctx,
		"SELECT count(*) FROM likes WHERE post_id = $1 AND user_id = ANY($2::uuid[])",
		postID,
		users,
	).Scan(&count)
	return count, err
}

func AssertCounts(
	t *testing.T,
	srv *httptest.Server,
	pool *pgxpool.Pool,
	want map[uuid.UUID]int64,
) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for postID, count := range want {
			stored, err := StoredCount(t.Context(), pool, postID)
			assert.NoError(c, err)
			assert.Equal(c, count, stored)
			live, err := LikeCount(t.Context(), srv, postID)
			assert.NoError(c, err)
			assert.Equal(c, count, live)
		}
	}, waitTimeout, pollInterval)
}

func AssertStored(t *testing.T, pool *pgxpool.Pool, postID uuid.UUID, want int64) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		stored, err := StoredCount(t.Context(), pool, postID)
		assert.NoError(c, err)
		assert.Equal(c, want, stored)
	}, waitTimeout, pollInterval)
}

func AssertStoredUsers(t *testing.T, pool *pgxpool.Pool, postID uuid.UUID, users []uuid.UUID) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		stored, err := storedUsersCount(t.Context(), pool, postID, users)
		assert.NoError(c, err)
		assert.Equal(c, int64(len(users)), stored)
	}, waitTimeout, pollInterval)
}

func FlushDragonfly(t *testing.T, in *Infra) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: in.DragonflyAddr})
	require.NoError(t, rdb.FlushAll(t.Context()).Err())
	require.NoError(t, rdb.Close())
}

func Pause(t *testing.T, ctr testcontainers.Container) {
	t.Helper()
	docker(t, "pause", ctr.GetContainerID())
}

func Unpause(t *testing.T, ctr testcontainers.Container) {
	t.Helper()
	docker(t, "unpause", ctr.GetContainerID())
}

func docker(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "docker", args...).CombinedOutput()
	require.NoError(t, err, string(out))
}
