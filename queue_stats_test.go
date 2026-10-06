package raptor

import (
	"errors"
	"testing"
	"time"

	"github.com/angryboat/raptor/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadQueueStat(t *testing.T) {
	conn := test.CreateDB(t)
	require.NoError(t, Setup(t.Context(), conn))

	t.Run("NoJobs", func(t *testing.T) {
		db := beginTx(t, conn)

		stat, err := LoadQueueStat(t.Context(), db, "default")
		require.NoError(t, err)
		require.NotNil(t, stat)
		assert.Empty(t, stat.Jobs)
	})

	t.Run("Completed", func(t *testing.T) {
		// Not tx-scoped like its siblings: total_duration_ms is derived from
		// now() - claimed_at, and now() is pinned to the transaction's start
		// time for its whole duration — the time.Sleep below would never move
		// it. Needs its own connection where each statement sees real time.
		db := test.CreateDB(t)
		require.NoError(t, Setup(t.Context(), db))

		_, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job"})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		time.Sleep(5 * time.Millisecond) // give total_duration_ms something nonzero to record

		require.NoError(t, Complete(t.Context(), db, jobs[0]))

		stat, err := LoadQueueStat(t.Context(), db, "default")
		require.NoError(t, err)
		require.Contains(t, stat.Jobs, "test.job")

		js := stat.Jobs["test.job"]
		assert.Equal(t, uint64(1), js.CompletedCount)
		assert.Zero(t, js.FailedCount)
		assert.Zero(t, js.CancelledCount)
		assert.Zero(t, js.DeadCount)
		assert.Greater(t, js.TotalDuration, time.Duration(0))
	})

	t.Run("FailedRetry", func(t *testing.T) {
		db := beginTx(t, conn)

		_, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job", MaxAttempts: 3})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		require.NoError(t, Fail(t.Context(), db, jobs[0], errors.New("boom")))

		stat, err := LoadQueueStat(t.Context(), db, "default")
		require.NoError(t, err)
		require.Contains(t, stat.Jobs, "test.job")

		js := stat.Jobs["test.job"]
		assert.Equal(t, uint64(1), js.FailedCount)
		assert.Zero(t, js.DeadCount)
		assert.Zero(t, js.CompletedCount)
		assert.Zero(t, js.CancelledCount)
	})

	t.Run("Dead", func(t *testing.T) {
		db := beginTx(t, conn)

		_, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job", MaxAttempts: 1})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		require.NoError(t, Fail(t.Context(), db, jobs[0], errors.New("boom")))

		stat, err := LoadQueueStat(t.Context(), db, "default")
		require.NoError(t, err)
		require.Contains(t, stat.Jobs, "test.job")

		js := stat.Jobs["test.job"]
		assert.Equal(t, uint64(1), js.DeadCount)
		assert.Zero(t, js.FailedCount)
		assert.Zero(t, js.CompletedCount)
		assert.Zero(t, js.CancelledCount)
	})

	t.Run("Cancelled", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job"})
		require.NoError(t, err)

		require.NoError(t, Cancel(t.Context(), db, id))

		stat, err := LoadQueueStat(t.Context(), db, "default")
		require.NoError(t, err)
		require.Contains(t, stat.Jobs, "test.job")

		js := stat.Jobs["test.job"]
		assert.Equal(t, uint64(1), js.CancelledCount)
		assert.Zero(t, js.CompletedCount)
		assert.Zero(t, js.FailedCount)
		assert.Zero(t, js.DeadCount)
	})

	t.Run("MultipleJobTypesAreKeyedSeparately", func(t *testing.T) {
		db := beginTx(t, conn)

		idA, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job.a"})
		require.NoError(t, err)
		idB, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job.b", MaxAttempts: 3})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 2, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 2)

		byID := map[string]*Job{jobs[0].ID: jobs[0], jobs[1].ID: jobs[1]}
		require.NoError(t, Complete(t.Context(), db, byID[idA]))
		require.NoError(t, Fail(t.Context(), db, byID[idB], errors.New("boom")))

		stat, err := LoadQueueStat(t.Context(), db, "default")
		require.NoError(t, err)
		require.Contains(t, stat.Jobs, "test.job.a")
		require.Contains(t, stat.Jobs, "test.job.b")

		a := stat.Jobs["test.job.a"]
		assert.Equal(t, uint64(1), a.CompletedCount)
		assert.Zero(t, a.FailedCount)

		b := stat.Jobs["test.job.b"]
		assert.Equal(t, uint64(1), b.FailedCount)
		assert.Zero(t, b.CompletedCount)
	})

	t.Run("QueueIsolation", func(t *testing.T) {
		db := beginTx(t, conn)

		_, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "queue-a", Type: "test.job"})
		require.NoError(t, err)
		_, err = Enqueue(t.Context(), db, EnqueueJob{Queue: "queue-b", Type: "test.job"})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "queue-a", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.NoError(t, Complete(t.Context(), db, jobs[0]))

		statA, err := LoadQueueStat(t.Context(), db, "queue-a")
		require.NoError(t, err)
		require.Contains(t, statA.Jobs, "test.job")
		assert.Equal(t, uint64(1), statA.Jobs["test.job"].CompletedCount)

		statB, err := LoadQueueStat(t.Context(), db, "queue-b")
		require.NoError(t, err)
		require.Contains(t, statB.Jobs, "test.job")
		assert.Zero(t, statB.Jobs["test.job"].CompletedCount)
	})

	t.Run("AccumulatesAcrossMultipleJobs", func(t *testing.T) {
		db := beginTx(t, conn)

		// Counters are incremented in place (see the UPDATE ... SET x = x + 1
		// statements backing Complete/Fail), not overwritten — completing or
		// failing several jobs of the same type should sum, not clobber.
		//
		// Claim/Complete-or-Fail always acts on jobs[0].ID rather than the
		// freshly enqueued id: this subtest shares one transaction, which
		// pins now() for its whole duration, so a retried job from an
		// earlier iteration can tie on visible_at with the next iteration's
		// new job and Claim may return either one.
		for range 3 {
			_, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job"})
			require.NoError(t, err)

			jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
			require.NoError(t, err)
			require.Len(t, jobs, 1)

			require.NoError(t, Complete(t.Context(), db, jobs[0]))
		}

		for range 2 {
			_, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job", MaxAttempts: 3})
			require.NoError(t, err)

			jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
			require.NoError(t, err)
			require.Len(t, jobs, 1)

			require.NoError(t, Fail(t.Context(), db, jobs[0], errors.New("boom")))
		}

		stat, err := LoadQueueStat(t.Context(), db, "default")
		require.NoError(t, err)
		require.Contains(t, stat.Jobs, "test.job")

		js := stat.Jobs["test.job"]
		assert.Equal(t, uint64(3), js.CompletedCount)
		assert.Equal(t, uint64(2), js.FailedCount)
		assert.Zero(t, js.DeadCount)
		assert.Zero(t, js.CancelledCount)
	})

	t.Run("UnknownQueueReturnsEmptyStat", func(t *testing.T) {
		db := beginTx(t, conn)

		stat, err := LoadQueueStat(t.Context(), db, "this-queue-has-never-been-touched")
		require.NoError(t, err)
		require.NotNil(t, stat)
		assert.Empty(t, stat.Jobs)
	})
}
