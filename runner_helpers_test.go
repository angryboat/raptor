package raptor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// beginTx starts a transaction on a shared, already-migrated connection and
// rolls it back on cleanup. Subtests that only need a *pgx.DB (not a raw
// *pgx.Conn) should use this instead of test.CreateDB + Setup, so a whole
// group of subtests can share one database/migration instead of paying for
// one each — the DROP/CREATE DATABASE + migration run is the expensive part.
// The rollback keeps subtests isolated from each other despite sharing the
// underlying connection.
func beginTx(t *testing.T, conn *pgx.Conn) pgx.Tx {
	t.Helper()

	tx, err := conn.Begin(t.Context())
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = tx.Rollback(context.Background())
	})

	return tx
}

// recordingWorker records every job it executes (thread-safe) and can be
// configured to always return a fixed error, for testing failure paths.
type recordingWorker struct {
	mu       sync.Mutex
	executed []*Job
	err      error
}

func (w *recordingWorker) Execute(_ context.Context, job *Job) error {
	w.mu.Lock()
	w.executed = append(w.executed, job)
	w.mu.Unlock()

	return w.err
}

func (w *recordingWorker) jobs() []*Job {
	w.mu.Lock()
	defer w.mu.Unlock()

	out := make([]*Job, len(w.executed))
	copy(out, w.executed)

	return out
}

func (w *recordingWorker) ids() []string {
	jobs := w.jobs()

	out := make([]string, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}

	return out
}

func enqueueN(t *testing.T, db DB, queue string, n int) []string {
	t.Helper()

	ids := make([]string, n)
	for i := range n {
		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue: queue,
			Type:  "test.job",
		})
		require.NoError(t, err)
		ids[i] = id
	}

	return ids
}

// isDead reports whether a job has landed in raptor_dead_jobs, i.e. it
// failed and exhausted its attempts. It's read-only, unlike Claim/Complete,
// so it's safe to poll from a test without racing the runner under test.
func isDead(t *testing.T, db DB, id string) bool {
	t.Helper()

	rows, err := db.Query(t.Context(), `SELECT 1 FROM "raptor_dead_jobs" WHERE "id" = $1`, id)
	require.NoError(t, err)
	defer rows.Close()

	return rows.Next()
}

// blockingWorker reports each job it starts on started, then holds it until
// unblock is closed. It returns its context's error, so a job whose context
// was cancelled mid-execution is recorded as failed.
type blockingWorker struct {
	started chan *Job
	unblock chan struct{}
}

func newBlockingWorker() *blockingWorker {
	return &blockingWorker{
		started: make(chan *Job, 16),
		unblock: make(chan struct{}),
	}
}

func (w *blockingWorker) Execute(ctx context.Context, job *Job) error {
	w.started <- job
	<-w.unblock

	return ctx.Err()
}

func (w *blockingWorker) awaitStart(t testing.TB) *Job {
	t.Helper()

	select {
	case job := <-w.started:
		return job
	case <-time.After(3 * time.Second):
		t.Fatal("worker never started a job")
		return nil
	}
}

type jobState struct {
	Status   string
	Attempts int32
}

func readJobState(t testing.TB, db DB, id string) jobState {
	t.Helper()

	rows, err := db.Query(t.Context(), `SELECT "status"::text, "attempts" FROM "raptor_jobs" WHERE "id" = $1`, id)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next(), "job %s not found", id)
	var state jobState
	require.NoError(t, rows.Scan(&state.Status, &state.Attempts))

	return state
}

func countClaimed(t testing.TB, db DB, queue string) int {
	t.Helper()

	rows, err := db.Query(t.Context(), `SELECT count(*) FROM "raptor_jobs" WHERE "queue_name" = $1 AND "status" = 'claimed'`, queue)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var n int
	require.NoError(t, rows.Scan(&n))

	return n
}
