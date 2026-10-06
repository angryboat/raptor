package raptor

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maddiesch/raptor/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetupPool_Concurrent(t *testing.T) {
	pool := test.CreatePool(t)

	n := int(pool.Config().MaxConns)
	start := make(chan struct{})
	errs := make([]error, n)

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			<-start
			errs[i] = SetupPool(t.Context(), pool)
		})
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "setup %d", i)
	}
}

func TestRaptor(t *testing.T) {
	conn := test.CreateDB(t)
	require.NoError(t, Setup(t.Context(), conn))

	t.Run("Setup", func(t *testing.T) {
		// migrate.Up tracks applied migrations, so re-running Setup against an
		// already-migrated DB should be a no-op, not an error.
		assert.NoError(t, Setup(t.Context(), conn))
	})

	t.Run("Enqueue", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:   "default",
			Type:    "test.job",
			Payload: map[string]any{"foo": "bar"},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, id)
	})

	t.Run("Claim", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:   "default",
			Type:    "test.job",
			Payload: map[string]any{"foo": "bar"},
		})
		require.NoError(t, err)
		require.NotEmpty(t, id)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		job := jobs[0]
		assert.Equal(t, id, job.ID)
		assert.Equal(t, "default", job.Queue)
		assert.Equal(t, "test.job", job.Type)
		assert.Equal(t, "claimed", job.Status)
		require.NotNil(t, job.ClaimedBy)
		assert.Equal(t, "worker-1", *job.ClaimedBy)
		assert.Equal(t, int32(60), job.ClaimedTTL)
		assert.Equal(t, int32(1), job.Attempts)

		more, err := Claim(t.Context(), db, "default", "worker-2", 1, time.Minute)
		require.NoError(t, err)
		assert.Empty(t, more)
	})

	t.Run("Complete", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue: "default",
			Type:  "test.job",
		})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.Equal(t, id, jobs[0].ID)

		err = Complete(t.Context(), db, id)
		require.NoError(t, err)

		err = Complete(t.Context(), db, id)
		assert.ErrorIs(t, err, ErrJobNotFound)
	})

	t.Run("Cancel", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue: "default",
			Type:  "test.job",
		})
		require.NoError(t, err)

		err = Cancel(t.Context(), db, id)
		require.NoError(t, err)

		err = Cancel(t.Context(), db, id)
		assert.ErrorIs(t, err, ErrJobNotFound)
	})

	t.Run("Fail_Retry", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "default",
			Type:        "test.job",
			MaxAttempts: 3,
		})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		err = Fail(t.Context(), db, id, errors.New("boom"))
		require.NoError(t, err)

		jobs, err = Claim(t.Context(), db, "default", "worker-2", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		assert.Equal(t, id, jobs[0].ID)
		assert.Equal(t, int32(2), jobs[0].Attempts)
	})

	t.Run("Fail_Dead", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "default",
			Type:        "test.job",
			MaxAttempts: 1,
		})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		err = Fail(t.Context(), db, id, errors.New("boom"))
		require.NoError(t, err)

		jobs, err = Claim(t.Context(), db, "default", "worker-2", 1, time.Minute)
		require.NoError(t, err)
		assert.Empty(t, jobs)

		err = Fail(t.Context(), db, id, errors.New("boom again"))
		assert.ErrorIs(t, err, ErrJobNotFound)
	})

	t.Run("Sweep", func(t *testing.T) {
		// Not tx-scoped like its siblings: Sweep's expiry check compares
		// claimed_at against now(), and now() is pinned to the transaction's
		// start time for its whole duration — the time.Sleep below would never
		// make the claim look expired. Needs its own connection where each
		// statement really does see the current time.
		db := test.CreateDB(t)
		require.NoError(t, Setup(t.Context(), db))

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "default",
			Type:        "test.job",
			MaxAttempts: 3,
		})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Second)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		time.Sleep(1100 * time.Millisecond)

		err = Sweep(t.Context(), db, "default")
		require.NoError(t, err)

		// raptor_reap_stuck_jobs reschedules with a 30s retry delay, so the job
		// isn't immediately reclaimable. Confirm the reap happened by checking it
		// left the "claimed" status instead (Complete only succeeds on a claimed job).
		err = Complete(t.Context(), db, id)
		assert.ErrorIs(t, err, ErrJobNotFound)
	})

	t.Run("Expired", func(t *testing.T) {
		db := beginTx(t, conn)

		expiredID, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:     "expiry",
			Type:      "test.job",
			ExpiresAt: time.Now().Add(-time.Minute),
		})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "expiry", "worker-1", 10, time.Minute)
		require.NoError(t, err)
		assert.Empty(t, jobs)

		liveID, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:     "expiry",
			Type:      "test.job",
			ExpiresAt: time.Now().Add(time.Hour),
		})
		require.NoError(t, err)

		require.NoError(t, Sweep(t.Context(), db, "expiry"))

		var status, reason string
		err = db.QueryRow(t.Context(), `SELECT "status"::text, "failure_info"->>'error' FROM "raptor_jobs" WHERE "id" = $1`, expiredID).Scan(&status, &reason)
		require.NoError(t, err)
		assert.Equal(t, "cancelled", status)
		assert.Equal(t, "expired", reason)

		stat, err := LoadQueueStat(t.Context(), db, "expiry")
		require.NoError(t, err)
		assert.Equal(t, uint64(1), stat.Jobs["test.job"].CancelledCount)

		jobs, err = Claim(t.Context(), db, "expiry", "worker-1", 10, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		assert.Equal(t, liveID, jobs[0].ID)
	})

	t.Run("Cleanup", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue: "default",
			Type:  "test.job",
		})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.Equal(t, id, jobs[0].ID)

		err = Complete(t.Context(), db, id)
		require.NoError(t, err)

		err = Cleanup(t.Context(), db, "default")
		assert.NoError(t, err)

		err = Cleanup(t.Context(), db, "")
		assert.NoError(t, err)
	})

	t.Run("Enqueue_Idempotent", func(t *testing.T) {
		db := beginTx(t, conn)

		job := EnqueueJob{
			Queue:          "default",
			Type:           "test.job",
			IdempotencyKey: "same-key",
		}

		firstID, err := Enqueue(t.Context(), db, job)
		require.NoError(t, err)

		secondID, err := Enqueue(t.Context(), db, job)
		require.NoError(t, err)

		assert.Equal(t, firstID, secondID)
	})
}
