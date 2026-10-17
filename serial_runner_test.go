package raptor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/angryboat/raptor/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerialRunner(t *testing.T) {
	conn := test.CreateDB(t)
	require.NoError(t, Setup(t.Context(), conn))

	t.Run("Execute_DrainsQueuesInOrder", func(t *testing.T) {
		db := beginTx(t, conn)

		highIDs := enqueueN(t, db, "high", 2)
		lowIDs := enqueueN(t, db, "low", 2)

		worker := &recordingWorker{}
		r := NewSerialRunner([]string{"high", "low"}, map[string]Worker{"test.job": worker})

		err := r.Execute(t.Context(), db)
		require.NoError(t, err)

		got := worker.ids()
		require.Len(t, got, 4)
		assert.ElementsMatch(t, highIDs, got[:2], "high queue should be fully drained first")
		assert.ElementsMatch(t, lowIDs, got[2:], "low queue should run only after high is empty")

		for _, job := range worker.jobs() {
			err := Complete(t.Context(), db, job)
			assert.ErrorIs(t, err, ErrClaimLost, "job %s should already be completed by the runner", job.ID)
		}
	})

	t.Run("Execute_NoWorkerRegistered", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "default",
			Type:        "missing.type",
			MaxAttempts: 1,
		})
		require.NoError(t, err)

		r := NewSerialRunner([]string{"default"}, map[string]Worker{})

		err = r.Execute(t.Context(), db)
		require.NoError(t, err)

		assert.Equal(t, missingWorkerFailure, deadFailure(t, db, id))
	})

	t.Run("Execute_NilWorkerRegistered", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "default",
			Type:        "missing.type",
			MaxAttempts: 1,
		})
		require.NoError(t, err)

		r := NewSerialRunner([]string{"default"}, map[string]Worker{"missing.type": nil})

		err = r.Execute(t.Context(), db)
		require.NoError(t, err)

		assert.Equal(t, missingWorkerFailure, deadFailure(t, db, id))
	})

	t.Run("Execute_WorkerError", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "default",
			Type:        "test.job",
			MaxAttempts: 1,
		})
		require.NoError(t, err)

		worker := &recordingWorker{err: errors.New("boom")}
		r := NewSerialRunner([]string{"default"}, map[string]Worker{"test.job": worker})

		err = r.Execute(t.Context(), db)
		require.NoError(t, err)

		assert.Len(t, worker.ids(), 1)
		assert.True(t, isDead(t, db, id))
	})

	t.Run("Execute_WorkerErrorBacksOff", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job", MaxAttempts: 3})
		require.NoError(t, err)

		worker := &recordingWorker{err: errors.New("boom")}
		r := NewSerialRunner([]string{"default"}, map[string]Worker{"test.job": worker})

		require.NoError(t, r.Execute(t.Context(), db))

		assert.Len(t, worker.ids(), 1)
		delay := retryDelayOf(t, db, id)
		assert.GreaterOrEqual(t, delay, retryBaseDelay/2)
		assert.LessOrEqual(t, delay, retryBaseDelay)
	})

	t.Run("Execute_WorkerRetryAfter", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job", MaxAttempts: 3})
		require.NoError(t, err)

		worker := &recordingWorker{err: fmt.Errorf("fetch: %w", retryAfterError{after: 42 * time.Second})}
		r := NewSerialRunner([]string{"default"}, map[string]Worker{"test.job": worker})

		require.NoError(t, r.Execute(t.Context(), db))

		assert.Len(t, worker.ids(), 1)
		assert.Equal(t, 42*time.Second, retryDelayOf(t, db, id))
	})

	t.Run("Execute_WorkerContextEndsWithClaim", func(t *testing.T) {
		db := beginTx(t, conn)

		_, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job"})
		require.NoError(t, err)

		var (
			deadline    time.Time
			hasDeadline bool
			claimEnds   time.Time
		)
		worker := ValueWorkerFunc(func(ctx context.Context, job *Job, _ any) error {
			deadline, hasDeadline = ctx.Deadline()
			claimEnds = job.ClaimDeadline()
			return nil
		})
		r := NewSerialRunner([]string{"default"}, map[string]Worker{"test.job": worker})

		require.NoError(t, r.Execute(t.Context(), db))

		require.True(t, hasDeadline)
		assert.Equal(t, claimEnds, deadline)
	})

	t.Run("Execute_ClaimTimeoutSetsClaimTTL", func(t *testing.T) {
		db := beginTx(t, conn)

		enqueueN(t, db, "default", 1)

		var (
			ttl      int32
			deadline time.Time
			claimed  time.Time
		)
		worker := ValueWorkerFunc(func(ctx context.Context, job *Job, _ any) error {
			ttl = job.ClaimedTTL
			deadline, _ = ctx.Deadline()
			claimed = *job.ClaimedAt
			return nil
		})
		r := NewSerialRunner([]string{"default"}, map[string]Worker{"test.job": worker})
		r.ClaimTimeout = 2 * time.Hour

		require.NoError(t, r.Execute(t.Context(), db))

		assert.Equal(t, int32(7200), ttl)
		assert.Equal(t, claimed.Add(2*time.Hour), deadline)
	})

	t.Run("Execute_NegativeClaimTimeout", func(t *testing.T) {
		db := beginTx(t, conn)

		enqueueN(t, db, "default", 1)

		r := NewSerialRunner([]string{"default"}, nil)
		r.ClaimTimeout = -time.Second

		assert.EqualError(t, r.Execute(t.Context(), db), "raptor: claim timeout -1s must not be negative")
		assert.Zero(t, countClaimed(t, db, "default"))
	})

	t.Run("Execute_CancelWaitsForRunningJob", func(t *testing.T) {
		db := beginTx(t, conn)

		ids := enqueueN(t, db, "default", 2)

		worker := newBlockingWorker()
		r := NewSerialRunner([]string{"default"}, map[string]Worker{"test.job": worker})

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Execute(ctx, db) }()

		started := worker.awaitStart(t)
		cancel()

		select {
		case <-done:
			t.Fatal("Execute returned before the running job finished")
		case <-time.After(50 * time.Millisecond):
		}

		close(worker.unblock)
		require.ErrorIs(t, <-done, context.Canceled)

		for _, id := range ids {
			want := jobState{Status: "pending", Attempts: 0}
			if id == started.ID {
				want = jobState{Status: "completed", Attempts: 1}
			}
			assert.Equal(t, want, readJobState(t, db, id), "job %s", id)
		}
	})

	t.Run("Execute_QuietFinishesRunningJobAndReturns", func(t *testing.T) {
		db := beginTx(t, conn)

		ids := enqueueN(t, db, "default", 2)

		worker := newBlockingWorker()
		r := NewSerialRunner([]string{"default", "other"}, map[string]Worker{"test.job": worker})
		enqueueN(t, db, "other", 1)

		done := make(chan error, 1)
		go func() { done <- r.Execute(t.Context(), db) }()

		started := worker.awaitStart(t)
		r.Quiet()
		r.Quiet()
		close(worker.unblock)
		require.NoError(t, <-done)

		assert.Empty(t, worker.started, "runner started a job after Quiet")
		assert.Zero(t, countClaimed(t, db, "other"))
		for _, id := range ids {
			want := jobState{Status: "pending", Attempts: 0}
			if id == started.ID {
				want = jobState{Status: "completed", Attempts: 1}
			}
			assert.Equal(t, want, readJobState(t, db, id), "job %s", id)
		}
	})

	t.Run("Execute_QuietBeforeExecuteClaimsNothing", func(t *testing.T) {
		db := beginTx(t, conn)

		id := enqueueN(t, db, "default", 1)[0]

		worker := &recordingWorker{}
		r := NewSerialRunner([]string{"default"}, map[string]Worker{"test.job": worker})
		r.Quiet()

		require.NoError(t, r.Execute(t.Context(), db))

		assert.Empty(t, worker.ids())
		assert.Equal(t, jobState{Status: "pending", Attempts: 0}, readJobState(t, db, id))
	})

	t.Run("Execute_ShutdownTimeoutReleasesRunningJob", func(t *testing.T) {
		const timeout = 100 * time.Millisecond
		db := beginTx(t, conn)

		ids := enqueueN(t, db, "default", 1)

		worker := newBlockingWorker()
		r := NewSerialRunner([]string{"default"}, map[string]Worker{"test.job": worker})
		r.ShutdownTimeout = timeout

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Execute(ctx, db) }()

		worker.awaitStart(t)
		cancel()
		cancelled := time.Now()

		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(3 * time.Second):
			t.Fatal("Execute never returned after the shutdown timeout")
		}

		assert.GreaterOrEqual(t, time.Since(cancelled), timeout)
		assert.Equal(t, jobState{Status: "pending", Attempts: 0}, readJobState(t, db, ids[0]))
	})

	t.Run("ExecuteJob_ClaimDeadlineFailsJob", func(t *testing.T) {
		db := beginTx(t, conn)

		ids := enqueueN(t, db, "default", 1)
		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, 0)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		job := jobs[0]
		expired := job.ClaimedAt.Add(-time.Hour)
		job.ClaimedAt = &expired

		require.NoError(t, executeJob(t.Context(), db, newBlockingWorker(), job))
		assert.Equal(t, jobState{Status: "pending", Attempts: 1}, readJobState(t, db, ids[0]))
	})

	t.Run("Execute_NegativeShutdownTimeout", func(t *testing.T) {
		db := beginTx(t, conn)

		enqueueN(t, db, "default", 1)

		r := NewSerialRunner([]string{"default"}, nil)
		r.ShutdownTimeout = -time.Second

		assert.EqualError(t, r.Execute(t.Context(), db), "raptor: shutdown timeout -1s must not be negative")
		assert.Zero(t, countClaimed(t, db, "default"))
	})

	t.Run("Execute_EmptyQueueList", func(t *testing.T) {
		db := beginTx(t, conn)

		r := NewSerialRunner(nil, nil)

		err := r.Execute(t.Context(), db)
		assert.NoError(t, err)
	})
}
