package raptor

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/angryboat/raptor/internal/test"
	"github.com/jackc/pgx/v5"
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

	t.Run("Setup_DropsFailedJobs", func(t *testing.T) {
		var exists bool
		err := conn.QueryRow(t.Context(), `SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'raptor_failed_jobs')`).Scan(&exists)
		require.NoError(t, err)
		assert.False(t, exists)
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

	t.Run("Enqueue_LengthLimits", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:          strings.Repeat("q", 64),
			Type:           strings.Repeat("é", 64),
			IdempotencyKey: strings.Repeat("é", 128),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, id)
	})

	for name, tc := range map[string]struct {
		job  EnqueueJob
		want string
	}{
		"empty queue":    {EnqueueJob{Type: "test.job"}, "raptor: job queue must be 1-64 characters"},
		"queue too long": {EnqueueJob{Queue: strings.Repeat("q", 65), Type: "test.job"}, "raptor: job queue must be 1-64 characters"},
		"empty type":     {EnqueueJob{Queue: "default"}, "raptor: job type must be 1-64 characters"},
		"type too long":  {EnqueueJob{Queue: "default", Type: strings.Repeat("t", 65)}, "raptor: job type must be 1-64 characters"},
		"key too long":   {EnqueueJob{Queue: "default", Type: "test.job", IdempotencyKey: strings.Repeat("k", 129)}, "raptor: job idempotency key must be at most 128 characters"},
		"negative TTL":   {EnqueueJob{Queue: "default", Type: "test.job", ClaimTTL: -time.Second}, "raptor: job claim TTL must not be negative"},
	} {
		t.Run("Enqueue_Invalid/"+name, func(t *testing.T) {
			db := beginTx(t, conn)

			_, err := Enqueue(t.Context(), db, tc.job)
			assert.EqualError(t, err, tc.want)

			var n int
			require.NoError(t, db.QueryRow(t.Context(), `SELECT count(*) FROM "raptor_jobs"`).Scan(&n))
			assert.Zero(t, n)
		})
	}

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

	t.Run("Claim rounds timeout up to whole seconds", func(t *testing.T) {
		for timeout, ttl := range map[time.Duration]int32{
			time.Nanosecond:         1,
			500 * time.Millisecond:  1,
			time.Second:             1,
			1900 * time.Millisecond: 2,
			0:                       300,
			-time.Second:            300,
		} {
			t.Run(timeout.String(), func(t *testing.T) {
				db := beginTx(t, conn)

				_, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job"})
				require.NoError(t, err)

				jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, timeout)
				require.NoError(t, err)
				require.Len(t, jobs, 1)

				job := jobs[0]
				assert.Equal(t, ttl, job.ClaimedTTL)
				require.NotNil(t, job.ClaimedAt)
				assert.Equal(t, job.ClaimedAt.Add(time.Duration(ttl)*time.Second), job.ClaimDeadline())

				var reaped int
				require.NoError(t, db.QueryRow(t.Context(), `SELECT "raptor_reap_stuck_jobs"()`).Scan(&reaped))
				assert.Zero(t, reaped)
			})
		}
	})

	t.Run("Enqueue_ClaimTTL", func(t *testing.T) {
		for ttl, want := range map[time.Duration]int32{
			0:                       300,
			time.Nanosecond:         1,
			1900 * time.Millisecond: 2,
			30 * time.Minute:        1800,
		} {
			t.Run(ttl.String(), func(t *testing.T) {
				db := beginTx(t, conn)

				_, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job", ClaimTTL: ttl})
				require.NoError(t, err)

				jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, 0)
				require.NoError(t, err)
				require.Len(t, jobs, 1)

				job := jobs[0]
				assert.Equal(t, want, job.ClaimedTTL)
				require.NotNil(t, job.ClaimedAt)
				assert.Equal(t, job.ClaimedAt.Add(time.Duration(want)*time.Second), job.ClaimDeadline())
			})
		}
	})

	t.Run("Enqueue_ClaimTTLOverriddenByClaimTimeout", func(t *testing.T) {
		db := beginTx(t, conn)

		_, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job", ClaimTTL: 30 * time.Minute})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		assert.Equal(t, int32(60), jobs[0].ClaimedTTL)
	})

	t.Run("Enqueue_ClaimTTLKeptByIdempotentEnqueue", func(t *testing.T) {
		db := beginTx(t, conn)

		first, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job", IdempotencyKey: "k", ClaimTTL: 30 * time.Minute})
		require.NoError(t, err)
		second, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job", IdempotencyKey: "k", ClaimTTL: time.Minute})
		require.NoError(t, err)
		require.Equal(t, first, second)

		var ttl int32
		require.NoError(t, db.QueryRow(t.Context(), `SELECT "claimed_ttl" FROM "raptor_jobs" WHERE "id" = $1`, first).Scan(&ttl))
		assert.Equal(t, int32(1800), ttl)
	})

	t.Run("Sweep_HonorsEnqueuedClaimTTL", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "sweep-ttl", Type: "test.job", ClaimTTL: 30 * time.Minute})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "sweep-ttl", "worker-1", 1, 0)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		backdate := func(age string) {
			t.Helper()
			_, err := db.Exec(t.Context(), `UPDATE "raptor_jobs" SET "claimed_at" = now() - $2::interval WHERE "id" = $1`, id, age)
			require.NoError(t, err)
		}

		backdate("29 minutes")
		require.NoError(t, Sweep(t.Context(), db, "sweep-ttl"))
		assert.Equal(t, "claimed", readJobState(t, db, id).Status)

		backdate("30 minutes")
		require.NoError(t, Sweep(t.Context(), db, "sweep-ttl"))
		assert.Equal(t, "pending", readJobState(t, db, id).Status)
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

		err = Complete(t.Context(), db, jobs[0])
		require.NoError(t, err)

		err = Complete(t.Context(), db, jobs[0])
		assert.ErrorIs(t, err, ErrClaimLost)
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

		err = FailAfter(t.Context(), db, jobs[0], errors.New("boom"), 0)
		require.NoError(t, err)

		jobs, err = Claim(t.Context(), db, "default", "worker-2", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		assert.Equal(t, id, jobs[0].ID)
		assert.Equal(t, int32(2), jobs[0].Attempts)
	})

	t.Run("Fail_Backoff", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "default",
			Type:        "test.job",
			MaxAttempts: 4,
		})
		require.NoError(t, err)

		for attempt := range int32(3) {
			jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			require.Equal(t, attempt+1, jobs[0].Attempts)

			require.NoError(t, Fail(t.Context(), db, jobs[0], errors.New("boom")))

			ceiling := retryBaseDelay << attempt
			delay := retryDelayOf(t, db, id)
			assert.GreaterOrEqual(t, delay, ceiling/2, "attempt %d", attempt+1)
			assert.LessOrEqual(t, delay, ceiling, "attempt %d", attempt+1)

			more, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
			require.NoError(t, err)
			require.Empty(t, more, "a backed-off job shouldn't be claimable yet")

			_, err = db.Exec(t.Context(), `UPDATE "raptor_jobs" SET "visible_at" = now() WHERE "id" = $1`, id)
			require.NoError(t, err)
		}
	})

	t.Run("Fail_RetryAfter", func(t *testing.T) {
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

		failure := fmt.Errorf("fetch: %w", retryAfterError{after: 90 * time.Second})
		require.NoError(t, Fail(t.Context(), db, jobs[0], failure))

		assert.Equal(t, 90*time.Second, retryDelayOf(t, db, id))
	})

	t.Run("Fail_RetryAfterNonPositive", func(t *testing.T) {
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

		require.NoError(t, Fail(t.Context(), db, jobs[0], retryAfterError{}))

		delay := retryDelayOf(t, db, id)
		assert.GreaterOrEqual(t, delay, retryBaseDelay/2)
		assert.LessOrEqual(t, delay, retryBaseDelay)
	})

	t.Run("FailAfter", func(t *testing.T) {
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

		failure := retryAfterError{after: time.Hour}
		require.NoError(t, FailAfter(t.Context(), db, jobs[0], failure, 5*time.Second))

		assert.Equal(t, 5*time.Second, retryDelayOf(t, db, id), "an explicit delay overrides RetryAfter")
	})

	t.Run("FailAfter_LongDelay", func(t *testing.T) {
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

		delay := 400 * 24 * time.Hour
		require.NoError(t, FailAfter(t.Context(), db, jobs[0], errors.New("boom"), delay))

		assert.Equal(t, delay, retryDelayOf(t, db, id))
	})

	t.Run("Sweep_RetryDelay", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "sweep-delay",
			Type:        "test.job",
			MaxAttempts: 3,
		})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "sweep-delay", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		_, err = db.Exec(t.Context(), `UPDATE "raptor_jobs" SET "claimed_at" = now() - INTERVAL '1 hour' WHERE "id" = $1`, id)
		require.NoError(t, err)

		require.NoError(t, Sweep(t.Context(), db, "sweep-delay"))

		assert.Equal(t, 30*time.Second, retryDelayOf(t, db, id))
	})

	t.Run("Fail_Dead", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "default",
			Type:        "test.job",
			MaxAttempts: 1,
		})
		require.NoError(t, err)

		claimed, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		require.Equal(t, id, claimed[0].ID)

		err = Fail(t.Context(), db, claimed[0], errors.New("boom"))
		require.NoError(t, err)

		var (
			queue, jobType, status, claimedBy, reason string
			attempts                                  int32
			claimSeq                                  int64
			failedAtIsNow                             bool
		)
		err = db.QueryRow(t.Context(), `
			SELECT "queue_name", "job_type", "status"::text, "claimed_by", "attempts", "claim_seq", "failed_at" = now(), "failure_info"->>'error'
			FROM "raptor_dead_jobs" WHERE "id" = $1`, id).
			Scan(&queue, &jobType, &status, &claimedBy, &attempts, &claimSeq, &failedAtIsNow, &reason)
		require.NoError(t, err)
		assert.Equal(t, "default", queue)
		assert.Equal(t, "test.job", jobType)
		assert.Equal(t, "failed", status)
		assert.Equal(t, "worker-1", claimedBy)
		assert.Equal(t, int32(1), attempts)
		assert.Equal(t, claimed[0].ClaimSeq, claimSeq)
		assert.True(t, failedAtIsNow)
		assert.Equal(t, "boom", reason)

		var live bool
		require.NoError(t, db.QueryRow(t.Context(), `SELECT EXISTS (SELECT 1 FROM "raptor_jobs" WHERE "id" = $1)`, id).Scan(&live))
		assert.False(t, live)

		jobs, err := Claim(t.Context(), db, "default", "worker-2", 1, time.Minute)
		require.NoError(t, err)
		assert.Empty(t, jobs)

		err = Fail(t.Context(), db, claimed[0], errors.New("boom again"))
		assert.ErrorIs(t, err, ErrClaimLost, "a dead job still exists")
	})

	t.Run("Fail_Dead_AddedColumn", func(t *testing.T) {
		db := beginTx(t, conn)

		_, err := db.Exec(t.Context(), `ALTER TABLE "raptor_jobs" ADD COLUMN "test_added" TEXT NOT NULL DEFAULT 'added'`)
		require.NoError(t, err)
		// A statement cached by an earlier subtest keeps the old row shape and would hide a SELECT * regression.
		require.NoError(t, conn.DeallocateAll(t.Context()))

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue:       "default",
			Type:        "test.job",
			Payload:     map[string]any{"foo": "bar"},
			MaxAttempts: 1,
		})
		require.NoError(t, err)

		claimed, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, claimed, 1)

		job := claimed[0]
		assert.Equal(t, id, job.ID)
		assert.Equal(t, "test.job", job.Type)
		assert.JSONEq(t, `{"foo":"bar"}`, string(job.Payload))
		assert.Equal(t, int32(1), job.Attempts)

		require.NoError(t, Fail(t.Context(), db, job, errors.New("boom")))

		var status, reason string
		err = db.QueryRow(t.Context(), `SELECT "status"::text, "failure_info"->>'error' FROM "raptor_dead_jobs" WHERE "id" = $1`, id).Scan(&status, &reason)
		require.NoError(t, err)
		assert.Equal(t, "failed", status)
		assert.Equal(t, "boom", reason)
	})

	t.Run("Fail_Dead_ReusedID", func(t *testing.T) {
		db := beginTx(t, conn)

		var id string
		err := db.QueryRow(t.Context(), `
			INSERT INTO "raptor_dead_jobs" ("queue_name", "job_type", "status", "failed_at", "failure_info")
			VALUES ('reused', 'test.job', 'failed', now() - INTERVAL '1 day', '{"error":"first"}')
			RETURNING "id"`).Scan(&id)
		require.NoError(t, err)

		_, err = db.Exec(t.Context(), `INSERT INTO "raptor_jobs" ("id", "queue_name", "job_type", "max_attempts") VALUES ($1, 'reused', 'test.job', 1)`, id)
		require.NoError(t, err)

		claimed, err := Claim(t.Context(), db, "reused", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, claimed, 1)

		require.NoError(t, Fail(t.Context(), db, claimed[0], errors.New("second")))

		rows, err := db.Query(t.Context(), `SELECT "failure_info"->>'error' FROM "raptor_dead_jobs" WHERE "id" = $1`, id)
		require.NoError(t, err)
		reasons, err := pgx.CollectRows(rows, pgx.RowTo[string])
		require.NoError(t, err)
		assert.Equal(t, []string{"second"}, reasons)
	})

	t.Run("Fail_Permanent", func(t *testing.T) {
		for name, tc := range map[string]struct {
			failure error
			reason  string
			dead    bool
		}{
			"Marked":    {failure: Permanent(errors.New("bad payload")), reason: "bad payload", dead: true},
			"Wrapped":   {failure: fmt.Errorf("handle: %w", Permanent(errors.New("bad payload"))), reason: "handle: bad payload", dead: true},
			"NotMarked": {failure: errors.New("flaky"), reason: "flaky", dead: false},
		} {
			t.Run(name, func(t *testing.T) {
				db := beginTx(t, conn)

				id, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "permanent", Type: "test.job", MaxAttempts: 10})
				require.NoError(t, err)

				claimed, err := Claim(t.Context(), db, "permanent", "worker-1", 1, time.Minute)
				require.NoError(t, err)
				require.Len(t, claimed, 1)
				require.Equal(t, int32(1), claimed[0].Attempts)

				require.NoError(t, Fail(t.Context(), db, claimed[0], tc.failure))

				assert.Equal(t, tc.dead, isDead(t, db, id))

				table := `"raptor_jobs"`
				if tc.dead {
					table = `"raptor_dead_jobs"`
				}
				var reason string
				var attempts int32
				err = db.QueryRow(t.Context(), `SELECT "failure_info"->>'error', "attempts" FROM `+table+` WHERE "id" = $1`, id).Scan(&reason, &attempts)
				require.NoError(t, err)
				assert.Equal(t, tc.reason, reason)
				assert.Equal(t, int32(1), attempts)

				stat, err := LoadQueueStat(t.Context(), db, "permanent")
				require.NoError(t, err)
				js := stat.Jobs["test.job"]
				if tc.dead {
					assert.Equal(t, uint64(1), js.DeadCount)
					assert.Zero(t, js.FailedCount)
				} else {
					assert.Zero(t, js.DeadCount)
					assert.Equal(t, uint64(1), js.FailedCount)
				}
			})
		}
	})

	t.Run("FailAfter_Permanent", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "permanent", Type: "test.job", MaxAttempts: 10})
		require.NoError(t, err)

		claimed, err := Claim(t.Context(), db, "permanent", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, claimed, 1)

		require.NoError(t, FailAfter(t.Context(), db, claimed[0], Permanent(errors.New("bad payload")), time.Minute))

		assert.True(t, isDead(t, db, id))
	})

	t.Run("Fail_Permanent_ClaimLost", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "permanent", Type: "test.job", MaxAttempts: 10})
		require.NoError(t, err)

		claimed, err := Claim(t.Context(), db, "permanent", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, claimed, 1)

		stale := *claimed[0]
		stale.ClaimedBy = new("worker-2")

		assert.ErrorIs(t, Fail(t.Context(), db, &stale, Permanent(errors.New("bad payload"))), ErrClaimLost)
		assert.False(t, isDead(t, db, id))
	})

	t.Run("DeadJobs_Indexes", func(t *testing.T) {
		db := beginTx(t, conn)

		var pkey string
		err := db.QueryRow(t.Context(), `
			SELECT pg_get_constraintdef("oid") FROM pg_constraint
			WHERE "conrelid" = '"raptor_dead_jobs"'::regclass AND "contype" = 'p'`).Scan(&pkey)
		require.NoError(t, err)
		assert.Equal(t, "PRIMARY KEY (id)", pkey)

		var failedAtIndexed bool
		err = db.QueryRow(t.Context(), `
			SELECT EXISTS (
				SELECT 1 FROM pg_index i
				JOIN pg_attribute a ON a."attrelid" = i."indrelid" AND a."attnum" = i."indkey"[0]
				WHERE i."indrelid" = '"raptor_dead_jobs"'::regclass AND a."attname" = 'failed_at'
			)`).Scan(&failedAtIndexed)
		require.NoError(t, err)
		assert.True(t, failedAtIndexed)
	})

	t.Run("TimeoutMsDropped", func(t *testing.T) {
		db := beginTx(t, conn)

		rows, err := db.Query(t.Context(), `
			SELECT "table_name"::text FROM information_schema.columns
			WHERE "table_name" IN ('raptor_jobs', 'raptor_dead_jobs') AND "column_name" = 'timeout_ms'`)
		require.NoError(t, err)
		tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
		require.NoError(t, err)
		assert.Empty(t, tables)
	})

	t.Run("Release", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue: "default",
			Type:  "test.job",
		})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		require.NoError(t, Release(t.Context(), db, jobs[0]))

		jobs, err = Claim(t.Context(), db, "default", "worker-2", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		assert.Equal(t, id, jobs[0].ID)
		assert.Equal(t, int32(1), jobs[0].Attempts, "a released claim shouldn't count as an attempt")
	})

	t.Run("Release_StaleClaim", func(t *testing.T) {
		db := beginTx(t, conn)

		_, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue: "default",
			Type:  "test.job",
		})
		require.NoError(t, err)

		stale, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, stale, 1)

		require.NoError(t, Release(t.Context(), db, stale[0]))

		fresh, err := Claim(t.Context(), db, "default", "worker-2", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, fresh, 1)

		assert.ErrorIs(t, Release(t.Context(), db, stale[0]), ErrClaimLost)

		require.NoError(t, Complete(t.Context(), db, fresh[0]))
		assert.ErrorIs(t, Release(t.Context(), db, fresh[0]), ErrClaimLost)
	})

	t.Run("Release_ReclaimBySameWorker", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job", MaxAttempts: 3})
		require.NoError(t, err)

		stale, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, stale, 1)

		require.NoError(t, Release(t.Context(), db, stale[0]))

		fresh, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, fresh, 1)
		require.Equal(t, id, fresh[0].ID)
		require.Equal(t, stale[0].Attempts, fresh[0].Attempts)

		assert.ErrorIs(t, Complete(t.Context(), db, stale[0]), ErrClaimLost)
		assert.ErrorIs(t, Fail(t.Context(), db, stale[0], errors.New("late")), ErrClaimLost)
		assert.ErrorIs(t, Release(t.Context(), db, stale[0]), ErrClaimLost)
		assert.Equal(t, jobState{Status: "claimed", Attempts: 1}, readJobState(t, db, id))

		require.NoError(t, Complete(t.Context(), db, fresh[0]))
	})

	t.Run("Fail_DeadIDReused", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job", MaxAttempts: 1})
		require.NoError(t, err)

		stale, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, stale, 1)
		require.NoError(t, Fail(t.Context(), db, stale[0], errors.New("boom")))

		_, err = db.Exec(t.Context(), `INSERT INTO "raptor_jobs" ("id", "queue_name", "job_type", "max_attempts") VALUES ($1, 'default', 'test.job', 1)`, id)
		require.NoError(t, err)

		fresh, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, fresh, 1)
		require.Equal(t, id, fresh[0].ID)

		assert.NotEqual(t, stale[0].ClaimSeq, fresh[0].ClaimSeq)
		assert.ErrorIs(t, Complete(t.Context(), db, stale[0]), ErrClaimLost)
		assert.Equal(t, jobState{Status: "claimed", Attempts: 1}, readJobState(t, db, id))

		require.NoError(t, Complete(t.Context(), db, fresh[0]))
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
		// isn't immediately reclaimable. Confirm the reap happened by checking the
		// original claim no longer holds it.
		err = Complete(t.Context(), db, jobs[0])
		assert.ErrorIs(t, err, ErrClaimLost)

		var status string
		err = db.QueryRow(t.Context(), `SELECT "status"::text FROM "raptor_jobs" WHERE "id" = $1`, id).Scan(&status)
		require.NoError(t, err)
		assert.Equal(t, "pending", status)
	})

	t.Run("StaleClaim", func(t *testing.T) {
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
		stale := jobs[0]

		_, err = db.Exec(t.Context(), `SELECT "raptor_fail_job"($1, $2, $3, '{"error":"claim timeout"}'::jsonb)`, id, *stale.ClaimedBy, stale.ClaimSeq)
		require.NoError(t, err)

		jobs, err = Claim(t.Context(), db, "default", "worker-2", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		fresh := jobs[0]
		require.Equal(t, id, fresh.ID)

		assert.ErrorIs(t, Complete(t.Context(), db, stale), ErrClaimLost)
		assert.ErrorIs(t, Fail(t.Context(), db, stale, errors.New("late")), ErrClaimLost)

		sameWorker := *fresh
		sameWorker.ClaimSeq = stale.ClaimSeq
		assert.ErrorIs(t, Complete(t.Context(), db, &sameWorker), ErrClaimLost, "claim_seq must fence a worker that reclaims its own job")

		require.NoError(t, Complete(t.Context(), db, fresh))
	})

	t.Run("ClaimDeadline", func(t *testing.T) {
		db := beginTx(t, conn)

		_, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job"})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.NotNil(t, jobs[0].ClaimedAt)

		assert.Equal(t, jobs[0].ClaimedAt.Add(time.Minute), jobs[0].ClaimDeadline())
		assert.True(t, (&Job{}).ClaimDeadline().IsZero())
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

		err = Complete(t.Context(), db, jobs[0])
		require.NoError(t, err)

		err = Cleanup(t.Context(), db, "default")
		assert.NoError(t, err)

		err = Cleanup(t.Context(), db, "")
		assert.NoError(t, err)
	})

	t.Run("Settle_Cancelled", func(t *testing.T) {
		settles := map[string]func(DB, *Job) error{
			"Complete": func(db DB, job *Job) error { return Complete(t.Context(), db, job) },
			"Fail":     func(db DB, job *Job) error { return Fail(t.Context(), db, job, errors.New("boom")) },
			"Release":  func(db DB, job *Job) error { return Release(t.Context(), db, job) },
		}
		for name, settle := range settles {
			t.Run(name, func(t *testing.T) {
				db := beginTx(t, conn)

				id, err := Enqueue(t.Context(), db, EnqueueJob{
					Queue: "default",
					Type:  "test.job",
				})
				require.NoError(t, err)

				jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
				require.NoError(t, err)
				require.Len(t, jobs, 1)

				require.NoError(t, Cancel(t.Context(), db, id))

				assert.ErrorIs(t, settle(db, jobs[0]), ErrClaimLost)
				assert.Equal(t, "cancelled", readJobState(t, db, id).Status)
			})
		}
	})

	t.Run("Settle_Missing", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{
			Queue: "default",
			Type:  "test.job",
		})
		require.NoError(t, err)

		jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		_, err = db.Exec(t.Context(), `DELETE FROM "raptor_jobs" WHERE "id" = $1`, id)
		require.NoError(t, err)

		missing := &Job{ID: "00000000-0000-0000-0000-000000000000"}
		for _, job := range []*Job{jobs[0], missing} {
			assert.ErrorIs(t, Complete(t.Context(), db, job), ErrJobNotFound)
			assert.ErrorIs(t, Fail(t.Context(), db, job, errors.New("boom")), ErrJobNotFound)
			assert.ErrorIs(t, Release(t.Context(), db, job), ErrJobNotFound)
		}
	})

	t.Run("Settle_NoRow", func(t *testing.T) {
		db := noRowsDB{DB: beginTx(t, conn)}
		job := &Job{ID: "00000000-0000-0000-0000-000000000000"}

		assert.ErrorIs(t, Complete(t.Context(), db, job), errSettleNoRow)
		assert.ErrorIs(t, Fail(t.Context(), db, job, errors.New("boom")), errSettleNoRow)
		assert.ErrorIs(t, Release(t.Context(), db, job), errSettleNoRow)
	})

	t.Run("ExecuteJob_Cancelled", func(t *testing.T) {
		for name, worker := range map[string]*recordingWorker{
			"Succeeds": {},
			"Fails":    {err: errors.New("boom")},
		} {
			t.Run(name, func(t *testing.T) {
				db := beginTx(t, conn)

				id, err := Enqueue(t.Context(), db, EnqueueJob{
					Queue: "default",
					Type:  "test.job",
				})
				require.NoError(t, err)

				jobs, err := Claim(t.Context(), db, "default", "worker-1", 1, time.Minute)
				require.NoError(t, err)
				require.Len(t, jobs, 1)

				require.NoError(t, Cancel(t.Context(), db, id))

				assert.NoError(t, executeJob(t.Context(), db, worker, jobs[0]))
				assert.Equal(t, "cancelled", readJobState(t, db, id).Status)
			})
		}
	})

	t.Run("Cleanup_DeadJobs", func(t *testing.T) {
		db := beginTx(t, conn)

		insertDead := func(queue, age string) string {
			var id string
			err := db.QueryRow(t.Context(), `
				INSERT INTO "raptor_dead_jobs" ("queue_name", "job_type", "status", "failed_at")
				VALUES ($1, 'test.job', 'failed', now() - $2::interval)
				RETURNING "id"`, queue, age).Scan(&id)
			require.NoError(t, err)
			return id
		}
		remaining := func() []string {
			rows, err := db.Query(t.Context(), `SELECT "id"::text FROM "raptor_dead_jobs" WHERE "queue_name" LIKE 'dead-%'`)
			require.NoError(t, err)
			ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
			require.NoError(t, err)
			return ids
		}

		insertDead("dead-a", "31 days")
		insertDead("dead-a", "30 days")
		recent := insertDead("dead-a", "29 days")
		otherQueue := insertDead("dead-b", "31 days")

		require.NoError(t, Cleanup(t.Context(), db, "dead-a"))
		assert.ElementsMatch(t, []string{recent, otherQueue}, remaining())

		require.NoError(t, Cleanup(t.Context(), db, ""))
		assert.ElementsMatch(t, []string{recent}, remaining())
	})

	t.Run("Cleanup_Batches", func(t *testing.T) {
		seedExpired := func(t testing.TB, db pgx.Tx) []string {
			rows, err := db.Query(t.Context(), `
				INSERT INTO "raptor_jobs" ("queue_name", "job_type", "status", "completed_at")
				VALUES ('batch', 'test.job', 'pending', NULL), ('batch', 'test.job', 'completed', now())
				RETURNING "id"::text`)
			require.NoError(t, err)
			kept, err := pgx.CollectRows(rows, pgx.RowTo[string])
			require.NoError(t, err)

			_, err = db.Exec(t.Context(), `
				INSERT INTO "raptor_jobs" ("queue_name", "job_type", "status", "completed_at", "cancelled_at")
				VALUES
					('batch', 'test.job', 'completed', now() - interval '25 hours', NULL),
					('batch', 'test.job', 'completed', now() - interval '25 hours', NULL),
					('batch', 'test.job', 'completed', now() - interval '25 hours', NULL),
					('batch', 'test.job', 'cancelled', NULL, now() - interval '3 hours'),
					('batch', 'test.job', 'cancelled', NULL, now() - interval '3 hours')`)
			require.NoError(t, err)

			_, err = db.Exec(t.Context(), `
				INSERT INTO "raptor_dead_jobs" ("queue_name", "job_type", "status", "failed_at")
				SELECT 'batch', 'test.job', 'failed', now() - interval '31 days' FROM generate_series(1, 2)`)
			require.NoError(t, err)
			return kept
		}
		remaining := func(t testing.TB, db pgx.Tx) int {
			rows, err := db.Query(t.Context(), `
				SELECT (SELECT count(*) FROM "raptor_jobs" WHERE "queue_name" = 'batch')
					+ (SELECT count(*) FROM "raptor_dead_jobs" WHERE "queue_name" = 'batch')`)
			require.NoError(t, err)
			n, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[int])
			require.NoError(t, err)
			return n
		}

		t.Run("DeletesEveryBatch", func(t *testing.T) {
			db := beginTx(t, conn)
			kept := seedExpired(t, db)

			require.NoError(t, cleanup(t.Context(), db, "batch", 2))

			rows, err := db.Query(t.Context(), `SELECT "id"::text FROM "raptor_jobs" WHERE "queue_name" = 'batch'`)
			require.NoError(t, err)
			ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
			require.NoError(t, err)
			assert.ElementsMatch(t, kept, ids)
			assert.Equal(t, len(kept), remaining(t, db))
		})

		t.Run("CallDeletesAtMostTheLimit", func(t *testing.T) {
			db := beginTx(t, conn)
			kept := seedExpired(t, db)

			var deleted []int32
			for {
				rows, err := db.Query(t.Context(), `SELECT "raptor_cleanup_jobs"('batch', p_limit => 2)`)
				require.NoError(t, err)
				n, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[int32])
				require.NoError(t, err)
				deleted = append(deleted, n)
				if n < 2 {
					break
				}
			}

			assert.Equal(t, []int32{2, 2, 2, 1}, deleted)
			assert.Equal(t, len(kept), remaining(t, db))
		})

		t.Run("RejectsNonPositiveLimit", func(t *testing.T) {
			db := beginTx(t, conn)

			rows, err := db.Query(t.Context(), `SELECT "raptor_cleanup_jobs"(p_limit => 0)`)
			require.NoError(t, err)
			_, err = pgx.CollectExactlyOneRow(rows, pgx.RowTo[int32])
			assert.ErrorContains(t, err, "raptor: cleanup limit must be positive")
		})
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

	t.Run("Enqueue_KeyFreedBeforeLookup", func(t *testing.T) {
		db := beginTx(t, conn)

		job := EnqueueJob{Queue: "default", Type: "test.job", IdempotencyKey: "freed-key"}
		firstID, err := Enqueue(t.Context(), db, job)
		require.NoError(t, err)

		_, err = db.Exec(t.Context(), `SELECT set_config('raptor.test_free_job', $1, true)`, firstID)
		require.NoError(t, err)
		_, err = db.Exec(t.Context(), `
			CREATE FUNCTION "raptor_test_free_job"() RETURNS TRIGGER AS $$
			BEGIN
				UPDATE "raptor_jobs" SET "status" = 'completed', "completed_at" = now()
				WHERE "id" = current_setting('raptor.test_free_job')::uuid AND "status" = 'pending';
				RETURN NULL;
			END;
			$$ LANGUAGE plpgsql;
			CREATE TRIGGER "raptor_test_free_job" AFTER INSERT ON "raptor_jobs"
			FOR EACH STATEMENT EXECUTE FUNCTION "raptor_test_free_job"();`)
		require.NoError(t, err)

		before := globalTotals(t, db).Jobs

		secondID, err := Enqueue(t.Context(), db, job)
		require.NoError(t, err)
		assert.NotEmpty(t, secondID)
		assert.NotEqual(t, firstID, secondID)
		assert.Equal(t, before+1, globalTotals(t, db).Jobs)
	})

	t.Run("Enqueue_KeyNeverFree", func(t *testing.T) {
		db := beginTx(t, conn)
		keepStealingKey(t, db, "default", "test.job", "taken-key")

		id, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job", IdempotencyKey: "taken-key"})
		assert.ErrorIs(t, err, errEnqueueMissed)
		assert.Empty(t, id)
	})
}

func keepStealingKey(t testing.TB, db pgx.Tx, queue, jobType, key string) {
	t.Helper()

	_, err := db.Exec(t.Context(), fmt.Sprintf(`
		CREATE FUNCTION "raptor_test_take_key"() RETURNS TRIGGER AS $$
		BEGIN
			IF pg_trigger_depth() = 1 THEN
				INSERT INTO "raptor_jobs" ("queue_name", "job_type", "idempotency_key") VALUES (%[1]s, %[2]s, %[3]s);
			END IF;
			RETURN NULL;
		END;
		$$ LANGUAGE plpgsql;
		CREATE FUNCTION "raptor_test_free_key"() RETURNS TRIGGER AS $$
		BEGIN
			IF pg_trigger_depth() = 1 THEN
				UPDATE "raptor_jobs" SET "status" = 'completed', "completed_at" = now()
				WHERE "idempotency_key" = %[3]s AND "status" = 'pending';
			END IF;
			RETURN NULL;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER "raptor_test_take_key" BEFORE INSERT ON "raptor_jobs"
		FOR EACH STATEMENT EXECUTE FUNCTION "raptor_test_take_key"();
		CREATE TRIGGER "raptor_test_free_key" AFTER INSERT ON "raptor_jobs"
		FOR EACH STATEMENT EXECUTE FUNCTION "raptor_test_free_key"();`,
		quoteLiteral(queue), quoteLiteral(jobType), quoteLiteral(key)))
	require.NoError(t, err)
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func TestEnqueue_KeyFreedConcurrently(t *testing.T) {
	pool := test.CreatePool(t)
	require.NoError(t, SetupPool(t.Context(), pool))

	const pauseLock = 0x7261_7074_0308
	_, err := pool.Exec(t.Context(), fmt.Sprintf(`
		CREATE FUNCTION "raptor_test_pause"() RETURNS TRIGGER AS $$
		BEGIN
			IF current_setting('raptor.test_pause', true) = 'on' THEN
				PERFORM pg_advisory_xact_lock_shared(%d);
			END IF;
			RETURN NULL;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER "raptor_test_pause" AFTER INSERT ON "raptor_jobs"
		FOR EACH STATEMENT EXECUTE FUNCTION "raptor_test_pause"();`, pauseLock))
	require.NoError(t, err)

	job := EnqueueJob{Queue: "default", Type: "test.job", IdempotencyKey: "racy-key"}
	firstID, err := Enqueue(t.Context(), pool, job)
	require.NoError(t, err)

	holder, err := pool.Acquire(t.Context())
	require.NoError(t, err)
	defer holder.Release()
	_, err = holder.Exec(t.Context(), `SELECT pg_advisory_lock($1)`, pauseLock)
	require.NoError(t, err)

	enqueuer, err := pool.Acquire(t.Context())
	require.NoError(t, err)
	defer enqueuer.Release()
	_, err = enqueuer.Exec(t.Context(), `SET raptor.test_pause = 'on'`)
	require.NoError(t, err)

	type result struct {
		id  string
		err error
	}
	done := make(chan result, 1)
	go func() {
		id, err := Enqueue(t.Context(), enqueuer, job)
		done <- result{id, err}
	}()

	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(t.Context(), `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE "locktype" = 'advisory' AND NOT "granted")`).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)

	jobs, err := Claim(t.Context(), pool, "default", "worker-1", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.NoError(t, Complete(t.Context(), pool, jobs[0]))

	_, err = holder.Exec(t.Context(), `SELECT pg_advisory_unlock($1)`, pauseLock)
	require.NoError(t, err)

	got := <-done
	require.NoError(t, got.err)
	assert.NotEmpty(t, got.id)
	assert.NotEqual(t, firstID, got.id)
	assert.Equal(t, int64(2), globalTotals(t, pool).Jobs)
}

func TestClaim_Concurrent(t *testing.T) {
	pool := test.CreatePool(t)
	require.NoError(t, SetupPool(t.Context(), pool))

	const (
		claimers = 8
		rounds   = 10
		jobs     = 40
	)
	for round := 1; round <= rounds; round++ {
		queue := fmt.Sprintf("claim-race-%d", round)
		want := enqueueN(t, pool, queue, jobs)

		start := make(chan struct{})
		claimed := make([][]string, claimers)
		errs := make([]error, claimers)

		var wg sync.WaitGroup
		for i := range claimers {
			wg.Go(func() {
				<-start
				for {
					got, err := Claim(t.Context(), pool, queue, fmt.Sprintf("worker-%d", i), 3, time.Minute)
					if err != nil {
						errs[i] = err
						return
					}
					if len(got) == 0 {
						return
					}
					for _, job := range got {
						claimed[i] = append(claimed[i], job.ID)
					}
				}
			})
		}
		close(start)
		wg.Wait()

		var all []string
		for i := range claimers {
			require.NoError(t, errs[i])
			all = append(all, claimed[i]...)
		}
		assert.ElementsMatch(t, want, all, "round %d: every job claimed exactly once", round)
	}
}

func TestSweep_ConcurrentExpire(t *testing.T) {
	pool := test.CreatePool(t)
	require.NoError(t, SetupPool(t.Context(), pool))

	const (
		sweepers = 8
		rounds   = 10
		jobs     = 40
	)
	for round := 1; round <= rounds; round++ {
		queue := fmt.Sprintf("expire-race-%d", round)
		for range jobs {
			_, err := Enqueue(t.Context(), pool, EnqueueJob{
				Queue:     queue,
				Type:      "test.job",
				ExpiresAt: time.Now().Add(-time.Minute),
			})
			require.NoError(t, err)
		}
		before := globalTotals(t, pool).Cancelled

		start := make(chan struct{})
		errs := make([]error, sweepers)

		var wg sync.WaitGroup
		for i := range sweepers {
			wg.Go(func() {
				<-start
				errs[i] = Sweep(t.Context(), pool, queue)
			})
		}
		close(start)
		wg.Wait()

		for i := range sweepers {
			require.NoError(t, errs[i])
		}
		stat, err := LoadQueueStat(t.Context(), pool, queue)
		require.NoError(t, err)
		assert.Equal(t, uint64(jobs), stat.Jobs["test.job"].CancelledCount, "round %d", round)
		assert.Equal(t, int64(jobs), globalTotals(t, pool).Cancelled-before, "round %d", round)
	}
}

func TestPermanent(t *testing.T) {
	assert.NoError(t, Permanent(nil))

	cause := errors.New("bad payload")
	err := Permanent(cause)
	assert.EqualError(t, err, "bad payload")
	assert.ErrorIs(t, err, cause)
}

func TestBackoff(t *testing.T) {
	for attempts, ceiling := range map[int32]time.Duration{
		0:             time.Second,
		1:             time.Second,
		2:             2 * time.Second,
		10:            512 * time.Second,
		13:            retryMaxDelay,
		math.MaxInt32: retryMaxDelay,
	} {
		for range 100 {
			delay := backoff(attempts)
			assert.GreaterOrEqual(t, delay, ceiling/2, "attempts %d", attempts)
			assert.LessOrEqual(t, delay, ceiling, "attempts %d", attempts)
		}
	}
}

func TestTTLSeconds(t *testing.T) {
	for d, want := range map[time.Duration]int32{
		-time.Second:                  0,
		0:                             0,
		time.Nanosecond:               1,
		999 * time.Millisecond:        1,
		time.Second:                   1,
		time.Second + time.Nanosecond: 2,
		time.Minute:                   60,
		math.MaxInt32 * time.Second:   math.MaxInt32,
		(math.MaxInt32-1)*time.Second + time.Nanosecond: math.MaxInt32,
		time.Duration(math.MaxInt64):                    math.MaxInt32,
	} {
		assert.Equal(t, want, ttlSeconds(d), "duration %s", d)
	}
}
