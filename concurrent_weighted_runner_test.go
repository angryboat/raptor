package raptor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/angryboat/raptor/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrentWeightedRunner_Dequeue(t *testing.T) {
	conn := test.CreateDB(t)
	require.NoError(t, Setup(t.Context(), conn))

	t.Run("WeightOrder", func(t *testing.T) {
		db := beginTx(t, conn)

		enqueueN(t, db, "critical", 3)
		enqueueN(t, db, "default", 3)

		r := NewConcurrentWeightedRunner(map[string]int{"critical": 2, "default": 1}, nil)
		r.Threads = 10 // larger than either weight, so batching can't hide an over-claim

		jobs := make(chan *Job, 10)
		err := r.dequeue(t.Context(), db, weightedQueues(r.Queue), newIdleWorkers(r.Threads), jobs)
		require.NoError(t, err)
		close(jobs)

		var got []string
		for j := range jobs {
			got = append(got, j.Queue)
		}

		assert.Equal(t, []string{"critical", "critical", "default"}, got)
	})

	t.Run("MovesOnWhenQueueEmptiesEarly", func(t *testing.T) {
		db := beginTx(t, conn)

		enqueueN(t, db, "critical", 2) // fewer jobs available than the configured weight
		enqueueN(t, db, "default", 1)

		r := NewConcurrentWeightedRunner(map[string]int{"critical": 100, "default": 10}, nil)
		r.Threads = 4

		jobs := make(chan *Job, 10)
		err := r.dequeue(t.Context(), db, weightedQueues(r.Queue), newIdleWorkers(r.Threads), jobs)
		require.NoError(t, err)
		close(jobs)

		var got []string
		for j := range jobs {
			got = append(got, j.Queue)
		}

		assert.Equal(t, []string{"critical", "critical", "default"}, got)
	})

	t.Run("WaitsWhenEmpty", func(t *testing.T) {
		db := beginTx(t, conn)

		r := NewConcurrentWeightedRunner(map[string]int{"default": 1}, nil)
		r.emptyCooldown = 50 * time.Millisecond

		jobs := make(chan *Job, 1)
		start := time.Now()
		err := r.dequeue(t.Context(), db, weightedQueues(r.Queue), newIdleWorkers(1), jobs)
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
	})

	t.Run("EmptyWaitInterruptedByContext", func(t *testing.T) {
		db := beginTx(t, conn)

		r := NewConcurrentWeightedRunner(map[string]int{"default": 1}, nil)
		r.emptyCooldown = 5 * time.Second

		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()

		jobs := make(chan *Job, 1)
		start := time.Now()
		err := r.dequeue(ctx, db, weightedQueues(r.Queue), newIdleWorkers(1), jobs)
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.Less(t, elapsed, 1*time.Second, "context cancellation should interrupt the cooldown wait early")
	})
}

func TestWeightedQueues(t *testing.T) {
	got := weightedQueues(map[string]int{
		"low":      1,
		"critical": 5,
		"b-mid":    3,
		"a-mid":    3,
		"off":      0,
		"negative": -1,
	})

	assert.Equal(t, []weightedQueue{
		{name: "critical", weight: 5},
		{name: "a-mid", weight: 3},
		{name: "b-mid", weight: 3},
		{name: "low", weight: 1},
	}, got)
}

// TestConcurrentWeightedRunner_Run shares one pool + migration across its
// subtests. Unlike the Dequeue subtests above, these run Run() with real
// worker goroutines issuing concurrent queries, so a single shared
// transaction (which only allows one in-flight query at a time) won't work.
// Each subtest uses its own queue names instead, so they stay isolated
// despite sharing the underlying database.
func TestConcurrentWeightedRunner_Run(t *testing.T) {
	db := test.CreatePool(t)
	require.NoError(t, SetupPool(t.Context(), db))

	t.Run("CompletesAllJobs", func(t *testing.T) {
		criticalIDs := enqueueN(t, db, "run-completes-critical", 5)
		defaultIDs := enqueueN(t, db, "run-completes-default", 5)
		want := append(append([]string{}, criticalIDs...), defaultIDs...)

		worker := &recordingWorker{}
		r := NewConcurrentWeightedRunner(
			map[string]int{"run-completes-critical": 2, "run-completes-default": 1},
			map[string]Worker{"test.job": worker},
		)
		r.Threads = 2
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		require.Eventually(t, func() bool {
			return len(worker.ids()) == len(want)
		}, 3*time.Second, 10*time.Millisecond, "worker never received all jobs")

		cancel()
		require.NoError(t, <-done)

		assert.ElementsMatch(t, want, worker.ids())
	})

	t.Run("NoWorkerRegistered", func(t *testing.T) {
		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "run-noworker-default",
			Type:        "missing.type",
			MaxAttempts: 1,
		})
		require.NoError(t, err)

		r := NewConcurrentWeightedRunner(map[string]int{"run-noworker-default": 1}, map[string]Worker{})
		r.Threads = 1
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		require.Eventually(t, func() bool {
			return isDead(t, db, id)
		}, 2*time.Second, 10*time.Millisecond, "job was never marked dead")

		cancel()
		require.NoError(t, <-done)
		assert.Equal(t, missingWorkerFailure, deadFailure(t, db, id))
	})

	t.Run("QueueFrozenAtRun", func(t *testing.T) {
		const queue = "run-frozen-default"
		const late = "run-frozen-late"
		enqueueN(t, db, queue, 1)
		lateIDs := enqueueN(t, db, late, 1)

		worker := &recordingWorker{}
		r := NewConcurrentWeightedRunner(map[string]int{queue: 1}, map[string]Worker{"test.job": worker})
		r.Threads = 1
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		require.Eventually(t, func() bool {
			return len(worker.ids()) == 1
		}, 2*time.Second, 10*time.Millisecond, "worker never ran the first job")

		r.Queue[late] = 1

		assert.Never(t, func() bool {
			return len(worker.ids()) > 1
		}, 200*time.Millisecond, 10*time.Millisecond, "runner picked up a queue added after Run started")

		cancel()
		require.NoError(t, <-done)
		assert.Equal(t, jobState{Status: "pending", Attempts: 0}, readJobState(t, db, lateIDs[0]))
	})

	t.Run("WorkerError", func(t *testing.T) {
		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "run-workererror-default",
			Type:        "test.job",
			MaxAttempts: 1,
		})
		require.NoError(t, err)

		worker := &recordingWorker{err: errors.New("boom")}
		r := NewConcurrentWeightedRunner(map[string]int{"run-workererror-default": 1}, map[string]Worker{"test.job": worker})
		r.Threads = 1
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		require.Eventually(t, func() bool {
			return isDead(t, db, id)
		}, 2*time.Second, 10*time.Millisecond, "job was never marked dead")

		cancel()
		require.NoError(t, <-done)

		assert.Len(t, worker.ids(), 1)
	})

	t.Run("CancelDuringExecutionCompletesJob", func(t *testing.T) {
		ids := enqueueN(t, db, "run-cancel-inflight", 1)

		worker := newBlockingWorker()
		r := NewConcurrentWeightedRunner(map[string]int{"run-cancel-inflight": 1}, map[string]Worker{"test.job": worker})
		r.Threads = 1
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		worker.awaitStart(t)
		cancel()

		select {
		case <-done:
			t.Fatal("Run returned before the in-flight job finished")
		case <-time.After(50 * time.Millisecond):
		}

		close(worker.unblock)
		require.NoError(t, <-done)

		assert.Equal(t, jobState{Status: "completed", Attempts: 1}, readJobState(t, db, ids[0]))
	})

	t.Run("ShutdownTimeoutReleasesInFlightJob", func(t *testing.T) {
		const timeout = 100 * time.Millisecond
		ids := enqueueN(t, db, "run-shutdown-timeout", 1)

		worker := newBlockingWorker()
		r := NewConcurrentWeightedRunner(map[string]int{"run-shutdown-timeout": 1}, map[string]Worker{"test.job": worker})
		r.Threads = 1
		r.ShutdownTimeout = timeout
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		worker.awaitStart(t)
		cancel()
		cancelled := time.Now()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("Run never returned after the shutdown timeout")
		}

		assert.GreaterOrEqual(t, time.Since(cancelled), timeout)
		assert.Equal(t, jobState{Status: "pending", Attempts: 0}, readJobState(t, db, ids[0]))
	})

	t.Run("ShutdownTimeoutCompletesJobFinishedInTime", func(t *testing.T) {
		ids := enqueueN(t, db, "run-shutdown-in-time", 1)

		worker := newBlockingWorker()
		r := NewConcurrentWeightedRunner(map[string]int{"run-shutdown-in-time": 1}, map[string]Worker{"test.job": worker})
		r.Threads = 1
		r.ShutdownTimeout = 5 * time.Second
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		worker.awaitStart(t)
		cancel()
		close(worker.unblock)
		require.NoError(t, <-done)

		assert.Equal(t, jobState{Status: "completed", Attempts: 1}, readJobState(t, db, ids[0]))
	})

	t.Run("ShutdownTimeoutFailsJobErroringBeforeTimeout", func(t *testing.T) {
		ids := enqueueN(t, db, "run-shutdown-error", 1)

		worker := newBlockingWorker()
		worker.err = errors.New("boom")
		r := NewConcurrentWeightedRunner(map[string]int{"run-shutdown-error": 1}, map[string]Worker{"test.job": worker})
		r.Threads = 1
		r.ShutdownTimeout = 5 * time.Second
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		worker.awaitStart(t)
		cancel()
		close(worker.unblock)
		require.NoError(t, <-done)

		assert.Equal(t, jobState{Status: "pending", Attempts: 1}, readJobState(t, db, ids[0]))
	})

	t.Run("NegativeShutdownTimeout", func(t *testing.T) {
		const queue = "run-shutdown-negative"
		enqueueN(t, db, queue, 1)

		r := NewConcurrentWeightedRunner(map[string]int{queue: 1}, nil)
		r.ShutdownTimeout = -time.Second

		assert.EqualError(t, r.Run(t.Context(), db), "raptor: shutdown timeout -1s must not be negative")
		assert.Zero(t, countClaimed(t, db, queue))
	})

	t.Run("ClaimsNoMoreThanIdleWorkers", func(t *testing.T) {
		const queue = "run-claims-idle"
		const threads = 2
		enqueueN(t, db, queue, 3*threads)

		worker := newBlockingWorker()
		r := NewConcurrentWeightedRunner(map[string]int{queue: 10}, map[string]Worker{"test.job": worker})
		r.Threads = threads
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		for range threads {
			worker.awaitStart(t)
		}

		assert.Never(t, func() bool {
			return countClaimed(t, db, queue) > threads
		}, 200*time.Millisecond, 10*time.Millisecond, "runner claimed jobs no worker was free to start")

		cancel()
		close(worker.unblock)
		require.NoError(t, <-done)
	})

	t.Run("CancelReleasesUnstartedJobs", func(t *testing.T) {
		const queue = "run-cancel-unstarted"
		ids := enqueueN(t, db, queue, 3)

		worker := newBlockingWorker()
		r := NewConcurrentWeightedRunner(map[string]int{queue: 10}, map[string]Worker{"test.job": worker})
		r.Threads = 1
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		started := worker.awaitStart(t)
		require.Equal(t, 1, countClaimed(t, db, queue), "only the running job should be claimed")

		cancel()
		close(worker.unblock)
		require.NoError(t, <-done)

		for _, id := range ids {
			want := jobState{Status: "pending", Attempts: 0}
			if id == started.ID {
				want = jobState{Status: "completed", Attempts: 1}
			}
			assert.Equal(t, want, readJobState(t, db, id), "job %s", id)
		}
	})

	t.Run("CancelRacingClaimReleasesJob", func(t *testing.T) {
		for i := range 8 {
			queue := fmt.Sprintf("run-cancel-racing-claim-%d", i)
			ids := enqueueN(t, db, queue, 1)

			worker := &recordingWorker{}
			r := NewConcurrentWeightedRunner(map[string]int{queue: 1}, map[string]Worker{"test.job": worker})
			r.Threads = 1
			r.emptyCooldown = 10 * time.Millisecond

			ctx, cancel := context.WithCancel(t.Context())
			returned := make(chan struct{})
			racing := &cancelAfterFirstClaimDB{DB: db, cancel: cancel, settled: returned}

			err := func() error {
				defer close(returned)
				return r.Run(ctx, racing)
			}()
			require.NoError(t, err)

			assert.Empty(t, worker.ids())
			assert.Equal(t, jobState{Status: "pending", Attempts: 0}, readJobState(t, db, ids[0]))
		}
	})

	t.Run("QuietStopsClaimingAndFinishesInFlightJob", func(t *testing.T) {
		const queue = "run-quiet-in-flight"
		ids := enqueueN(t, db, queue, 3)

		worker := newBlockingWorker()
		r := NewConcurrentWeightedRunner(map[string]int{queue: 10}, map[string]Worker{"test.job": worker})
		r.Threads = 1
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		started := worker.awaitStart(t)
		r.Quiet()
		close(worker.unblock)

		require.Eventually(t, func() bool {
			return readJobState(t, db, started.ID).Status == "completed"
		}, 3*time.Second, 10*time.Millisecond, "in-flight job never completed after Quiet")

		select {
		case job := <-worker.started:
			t.Fatalf("runner started job %s after Quiet", job.ID)
		case <-done:
			t.Fatal("Run returned after Quiet before its context was done")
		case <-time.After(200 * time.Millisecond):
		}
		assert.Zero(t, countClaimed(t, db, queue))

		cancel()
		require.NoError(t, <-done)

		for _, id := range ids {
			want := jobState{Status: "pending", Attempts: 0}
			if id == started.ID {
				want = jobState{Status: "completed", Attempts: 1}
			}
			assert.Equal(t, want, readJobState(t, db, id), "job %s", id)
		}
	})

	t.Run("QuietBeforeRunClaimsNothing", func(t *testing.T) {
		const queue = "run-quiet-before"
		id := enqueueN(t, db, queue, 1)[0]

		worker := newBlockingWorker()
		r := NewConcurrentWeightedRunner(map[string]int{queue: 1}, map[string]Worker{"test.job": worker})
		r.Threads = 1
		r.emptyCooldown = 10 * time.Millisecond
		r.Quiet()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		select {
		case job := <-worker.started:
			t.Fatalf("quieted runner started job %s", job.ID)
		case <-done:
			t.Fatal("Run returned before its context was done")
		case <-time.After(200 * time.Millisecond):
		}

		cancel()
		require.NoError(t, <-done)
		assert.Equal(t, jobState{Status: "pending", Attempts: 0}, readJobState(t, db, id))
	})

	t.Run("QuietRacingShutdown", func(t *testing.T) {
		const queue = "run-quiet-racing"
		enqueueN(t, db, queue, 4)

		worker := &recordingWorker{}
		r := NewConcurrentWeightedRunner(map[string]int{queue: 10}, map[string]Worker{"test.job": worker})
		r.Threads = 2
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		var quieting sync.WaitGroup
		for range 4 {
			quieting.Go(r.Quiet)
		}
		cancel()
		quieting.Wait()
		r.Quiet()

		require.NoError(t, <-done)
		assert.Zero(t, countClaimed(t, db, queue))
	})

	t.Run("ParentCancelledWithCause", func(t *testing.T) {
		r := NewConcurrentWeightedRunner(map[string]int{"run-parent-cause": 1}, nil)
		r.Threads = 1
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithCancelCause(t.Context())
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx, db) }()

		cancel(errors.New("shutting down"))
		assert.NoError(t, <-done)
	})

	t.Run("ParentDeadlineExceeded", func(t *testing.T) {
		r := NewConcurrentWeightedRunner(map[string]int{"run-parent-deadline": 1}, nil)
		r.Threads = 1
		r.emptyCooldown = 10 * time.Millisecond

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()

		assert.ErrorIs(t, r.Run(ctx, db), context.DeadlineExceeded)
	})
	for name, threads := range map[string]int{"ZeroThreads": 0, "NegativeThreads": -1} {
		t.Run(name, func(t *testing.T) {
			queue := "run-threads-" + name
			enqueueN(t, db, queue, 1)

			r := NewConcurrentWeightedRunner(map[string]int{queue: 1}, nil)
			r.Threads = threads
			r.emptyCooldown = 10 * time.Millisecond

			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()

			assert.EqualError(t, r.Run(ctx, db), fmt.Sprintf("raptor: runner threads %d must be positive", threads))
			assert.Zero(t, countClaimed(t, db, queue))
		})
	}
}

func TestConcurrentWeightedRunner_Run_DequeueError(t *testing.T) {
	t.Run("ReturnsClaimError", func(t *testing.T) {
		want := errors.New("db is down")

		r := NewConcurrentWeightedRunner(map[string]int{"default": 1}, nil)
		r.Threads = 1

		assert.ErrorIs(t, r.Run(t.Context(), failingDB{err: want}), want)
	})

	t.Run("ReturnsClaimErrorWrappingCanceled", func(t *testing.T) {
		want := fmt.Errorf("claim: %w", context.Canceled)

		r := NewConcurrentWeightedRunner(map[string]int{"default": 1}, nil)
		r.Threads = 1

		assert.ErrorIs(t, r.Run(t.Context(), failingDB{err: want}), want)
	})
}
