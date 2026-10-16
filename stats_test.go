package raptor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/angryboat/raptor/internal/test"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatsRollup(t *testing.T) {
	conn := test.CreateDB(t)
	require.NoError(t, Setup(t.Context(), conn))

	t.Run("CleanupKeepsTotals", func(t *testing.T) {
		db := beginTx(t, conn)

		require.NoError(t, Complete(t.Context(), db, claimNew(t, db, EnqueueJob{Queue: "default", Type: "test.job"})))
		require.NoError(t, Fail(t.Context(), db, claimNew(t, db, EnqueueJob{Queue: "default", Type: "test.job", MaxAttempts: 3}), errors.New("boom")))
		id, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job"})
		require.NoError(t, err)
		require.NoError(t, Cancel(t.Context(), db, id))

		before, err := LoadQueueStat(t.Context(), db, "default")
		require.NoError(t, err)
		beforeTotals := globalTotals(t, db)
		require.NotZero(t, pendingStatEvents(t, db))

		require.NoError(t, Cleanup(t.Context(), db, ""))

		assert.Zero(t, pendingStatEvents(t, db))
		after, err := LoadQueueStat(t.Context(), db, "default")
		require.NoError(t, err)
		assert.Equal(t, before, after)
		assert.Equal(t, beforeTotals, globalTotals(t, db))
		assert.Equal(t, statTotals{Jobs: 3, Completed: 1, Failed: 1, Cancelled: 1}, beforeTotals)
	})

	t.Run("SumsRolledUpAndPendingEvents", func(t *testing.T) {
		db := beginTx(t, conn)

		require.NoError(t, Complete(t.Context(), db, claimNew(t, db, EnqueueJob{Queue: "default", Type: "test.job"})))
		require.NoError(t, Cleanup(t.Context(), db, ""))
		require.NoError(t, Complete(t.Context(), db, claimNew(t, db, EnqueueJob{Queue: "default", Type: "test.job"})))

		stat, err := LoadQueueStat(t.Context(), db, "default")
		require.NoError(t, err)
		assert.Equal(t, uint64(2), stat.Jobs["test.job"].CompletedCount)
		assert.Equal(t, statTotals{Jobs: 2, Completed: 2}, globalTotals(t, db))
	})

	t.Run("WritesOnlyDayAndAllTimeRows", func(t *testing.T) {
		db := beginTx(t, conn)

		_, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job"})
		require.NoError(t, err)
		require.NoError(t, Cleanup(t.Context(), db, ""))

		rows, err := db.Query(t.Context(), `SELECT "span", "date" = CURRENT_DATE, "total_jobs" FROM "raptor_stats" ORDER BY "span"`)
		require.NoError(t, err)
		type statRow struct {
			Span  string
			Today bool
			Jobs  int64
		}
		got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[statRow])
		require.NoError(t, err)

		assert.Equal(t, []statRow{{"all_time", false, 1}, {"day", true, 1}}, got)
	})
}

func TestStatsContention(t *testing.T) {
	pool := test.CreatePool(t)
	require.NoError(t, SetupPool(t.Context(), pool))

	t.Run("OpenEnqueueDoesNotBlockComplete", func(t *testing.T) {
		job := claimNew(t, pool, EnqueueJob{Queue: "enqueue", Type: "contention.job"})

		tx := beginPoolTx(t, pool)
		_, err := Enqueue(t.Context(), tx, EnqueueJob{Queue: "enqueue", Type: "contention.job"})
		require.NoError(t, err)

		requireUnblocked(t, func(ctx context.Context) error { return Complete(ctx, pool, job) })
	})

	t.Run("OpenCancelDoesNotBlockComplete", func(t *testing.T) {
		job := claimNew(t, pool, EnqueueJob{Queue: "cancel", Type: "contention.job"})
		id, err := Enqueue(t.Context(), pool, EnqueueJob{Queue: "cancel", Type: "contention.job"})
		require.NoError(t, err)

		tx := beginPoolTx(t, pool)
		require.NoError(t, Cancel(t.Context(), tx, id))

		requireUnblocked(t, func(ctx context.Context) error { return Complete(ctx, pool, job) })
	})

	t.Run("OpenCompleteDoesNotBlockFail", func(t *testing.T) {
		first := claimNew(t, pool, EnqueueJob{Queue: "complete", Type: "contention.job"})
		second := claimNew(t, pool, EnqueueJob{Queue: "complete", Type: "contention.job", MaxAttempts: 3})

		tx := beginPoolTx(t, pool)
		require.NoError(t, Complete(t.Context(), tx, first))

		requireUnblocked(t, func(ctx context.Context) error { return Fail(ctx, pool, second, errors.New("boom")) })
	})

	t.Run("OpenSweepDoesNotBlockSweep", func(t *testing.T) {
		enqueueExpiredAndSweep := func(ctx context.Context, db DB) error {
			for _, jobType := range []string{"contention.b", "contention.a"} {
				if _, err := Enqueue(ctx, pool, EnqueueJob{Queue: "sweep", Type: jobType, ExpiresAt: time.Now().Add(-time.Minute)}); err != nil {
					return err
				}
			}
			return Sweep(ctx, db, "sweep")
		}

		require.NoError(t, enqueueExpiredAndSweep(t.Context(), beginPoolTx(t, pool)))

		requireUnblocked(t, func(ctx context.Context) error { return enqueueExpiredAndSweep(ctx, pool) })
	})

	t.Run("ConcurrentRollupsCountEveryEvent", func(t *testing.T) {
		const workers, perWorker = 8, 25
		before := globalTotals(t, pool)

		var wg sync.WaitGroup
		done := make(chan struct{})
		for range 4 {
			wg.Go(func() {
				for {
					select {
					case <-done:
						return
					default:
						assert.NoError(t, Cleanup(t.Context(), pool, ""))
					}
				}
			})
		}

		var settlers sync.WaitGroup
		for i := range workers {
			queue := fmt.Sprintf("rollup-%d", i)
			settlers.Go(func() {
				for range perWorker {
					_, err := Enqueue(t.Context(), pool, EnqueueJob{Queue: queue, Type: "rollup.job"})
					if !assert.NoError(t, err) {
						return
					}
					jobs, err := Claim(t.Context(), pool, queue, "worker", 1, time.Minute)
					if !assert.NoError(t, err) || !assert.Len(t, jobs, 1) {
						return
					}
					assert.NoError(t, Complete(t.Context(), pool, jobs[0]))
				}
			})
		}
		settlers.Wait()
		close(done)
		wg.Wait()

		require.NoError(t, Cleanup(t.Context(), pool, ""))
		assert.Zero(t, pendingStatEvents(t, pool))

		var completed uint64
		for i := range workers {
			stat, err := LoadQueueStat(t.Context(), pool, fmt.Sprintf("rollup-%d", i))
			require.NoError(t, err)
			completed += stat.Jobs["rollup.job"].CompletedCount
		}
		assert.Equal(t, uint64(workers*perWorker), completed)

		after := globalTotals(t, pool)
		assert.Equal(t, int64(workers*perWorker), after.Jobs-before.Jobs)
		assert.Equal(t, int64(workers*perWorker), after.Completed-before.Completed)
	})
}

func BenchmarkSettleParallel(b *testing.B) {
	pool := test.CreatePool(b)
	require.NoError(b, SetupPool(b.Context(), pool))

	var next atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		queue := fmt.Sprintf("bench-%d", next.Add(1))
		for pb.Next() {
			_, err := Enqueue(b.Context(), pool, EnqueueJob{Queue: queue, Type: "bench.job"})
			if err != nil {
				b.Error(err)
				return
			}
			jobs, err := Claim(b.Context(), pool, queue, "worker", 1, time.Minute)
			if err != nil || len(jobs) != 1 {
				b.Error(err, len(jobs))
				return
			}
			if err := Complete(b.Context(), pool, jobs[0]); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func claimNew(t testing.TB, db DB, job EnqueueJob) *Job {
	t.Helper()

	id, err := Enqueue(t.Context(), db, job)
	require.NoError(t, err)

	jobs, err := Claim(t.Context(), db, job.Queue, "worker-1", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, id, jobs[0].ID)

	return jobs[0]
}

func beginPoolTx(t testing.TB, pool *pgxpool.Pool) pgx.Tx {
	t.Helper()

	tx, err := pool.Begin(t.Context())
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = tx.Rollback(context.Background())
	})

	return tx
}

func requireUnblocked(t testing.TB, settle func(context.Context) error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	require.NoError(t, settle(ctx), "blocked behind another transaction's stats write")
}

type statTotals struct {
	Jobs      int64
	Completed int64
	Failed    int64
	Cancelled int64
	Dead      int64
}

func globalTotals(t testing.TB, db DB) statTotals {
	t.Helper()

	rows, err := db.Query(t.Context(), `
		SELECT coalesce(sum("total_jobs"), 0)::bigint, coalesce(sum("total_completed"), 0)::bigint,
			coalesce(sum("total_failed"), 0)::bigint, coalesce(sum("total_cancelled"), 0)::bigint,
			coalesce(sum("total_dead"), 0)::bigint
		FROM (
			SELECT "total_jobs", "total_completed", "total_failed", "total_cancelled", "total_dead"
			FROM "raptor_stats" WHERE "span" = 'all_time'
			UNION ALL
			SELECT "total_jobs", "total_completed", "total_failed", "total_cancelled", "total_dead"
			FROM "raptor_stat_events"
		) s`)
	require.NoError(t, err)

	totals, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[statTotals])
	require.NoError(t, err)

	return totals
}

func pendingStatEvents(t testing.TB, db DB) int {
	t.Helper()

	rows, err := db.Query(t.Context(), `SELECT (SELECT count(*) FROM "raptor_stat_events") + (SELECT count(*) FROM "raptor_job_stat_events")`)
	require.NoError(t, err)

	n, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[int])
	require.NoError(t, err)

	return n
}
