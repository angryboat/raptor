package raptor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/angryboat/raptor/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewScheduler(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		s, err := NewScheduler(
			Schedule{Name: "b", Spec: "@every 5m"},
			Schedule{Name: "a", Spec: "*/15 * * * *"},
			Schedule{Name: "c", Spec: "CRON_TZ=America/Denver 0 9 * * 1-5"},
		)
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c"}, s.names)
		assert.Equal(t, []string{"*/15 * * * *", "@every 5m", "CRON_TZ=America/Denver 0 9 * * 1-5"}, s.specs)
	})

	for name, schedules := range map[string][]Schedule{
		"empty name":   {{Spec: "@hourly"}},
		"duplicate":    {{Name: "a", Spec: "@hourly"}, {Name: "a", Spec: "@daily"}},
		"invalid spec": {{Name: "a", Spec: "every hour"}},
		"never fires":  {{Name: "a", Spec: "0 0 31 2 *"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewScheduler(schedules...)
			assert.Error(t, err)
		})
	}
}

func TestScheduler(t *testing.T) {
	conn := test.CreateDB(t)
	require.NoError(t, Setup(t.Context(), conn))

	hourly := Schedule{Name: "hourly", Spec: "@every 1h", Job: EnqueueJob{Queue: "default", Type: "test.hourly"}}

	t.Run("Sync inserts schedules at their next tick", func(t *testing.T) {
		db := beginTx(t, conn)
		s := newTestScheduler(t, hourly)

		require.NoError(t, s.Sync(t.Context(), db))

		row := readSchedule(t, db, "hourly")
		assert.Equal(t, "@every 1h", row.Spec)
		assert.Equal(t, dbNow(t, db).Truncate(time.Second).Add(time.Hour), row.NextRunAt)
	})

	t.Run("Sync keeps next_run_at for unchanged specs", func(t *testing.T) {
		db := beginTx(t, conn)
		s := newTestScheduler(t, hourly)
		require.NoError(t, s.Sync(t.Context(), db))
		setNextRunAt(t, db, "hourly", "now() + interval '5 minutes'")
		before := readSchedule(t, db, "hourly")

		require.NoError(t, s.Sync(t.Context(), db))

		assert.Equal(t, before.NextRunAt, readSchedule(t, db, "hourly").NextRunAt)
	})

	t.Run("Sync reschedules changed specs", func(t *testing.T) {
		db := beginTx(t, conn)
		require.NoError(t, newTestScheduler(t, hourly).Sync(t.Context(), db))

		changed := hourly
		changed.Spec = "@every 1m"
		require.NoError(t, newTestScheduler(t, changed).Sync(t.Context(), db))

		row := readSchedule(t, db, "hourly")
		assert.Equal(t, "@every 1m", row.Spec)
		assert.Equal(t, dbNow(t, db).Truncate(time.Second).Add(time.Minute), row.NextRunAt)
	})

	t.Run("Sync deletes schedules removed from code", func(t *testing.T) {
		db := beginTx(t, conn)
		other := Schedule{Name: "other", Spec: "@daily"}
		require.NoError(t, newTestScheduler(t, hourly, other).Sync(t.Context(), db))

		require.NoError(t, newTestScheduler(t, hourly).Sync(t.Context(), db))
		assert.Equal(t, []string{"hourly"}, scheduleNames(t, db))

		require.NoError(t, newTestScheduler(t).Sync(t.Context(), db))
		assert.Empty(t, scheduleNames(t, db))
	})

	t.Run("Tick skips schedules that aren't due", func(t *testing.T) {
		db := beginTx(t, conn)
		s := newTestScheduler(t, hourly)
		require.NoError(t, s.Sync(t.Context(), db))

		n, err := s.Tick(t.Context(), db)
		require.NoError(t, err)
		assert.Zero(t, n)
		assert.Zero(t, countJobs(t, db, "test.hourly"))
	})

	t.Run("Tick enqueues due schedules on cadence", func(t *testing.T) {
		db := beginTx(t, conn)
		s := newTestScheduler(t, hourly)
		require.NoError(t, s.Sync(t.Context(), db))
		setNextRunAt(t, db, "hourly", "date_trunc('second', now()) - interval '10 seconds'")
		prev := readSchedule(t, db, "hourly").NextRunAt

		n, err := s.Tick(t.Context(), db)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.Equal(t, 1, countJobs(t, db, "test.hourly"))

		row := readSchedule(t, db, "hourly")
		assert.Equal(t, prev.Add(time.Hour), row.NextRunAt)
		require.NotNil(t, row.LastRunAt)
		assert.Equal(t, dbNow(t, db), *row.LastRunAt)

		n, err = s.Tick(t.Context(), db)
		require.NoError(t, err)
		assert.Zero(t, n)
	})

	t.Run("Tick collapses missed ticks into one job", func(t *testing.T) {
		db := beginTx(t, conn)
		minutely := Schedule{Name: "minutely", Spec: "* * * * *", Job: EnqueueJob{Queue: "default", Type: "test.minutely"}}
		s := newTestScheduler(t, minutely)
		require.NoError(t, s.Sync(t.Context(), db))
		setNextRunAt(t, db, "minutely", "now() - interval '1 day'")

		n, err := s.Tick(t.Context(), db)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.Equal(t, 1, countJobs(t, db, "test.minutely"))

		now := dbNow(t, db)
		assert.Equal(t, now.Truncate(time.Minute).Add(time.Minute), readSchedule(t, db, "minutely").NextRunAt)
	})

	t.Run("Tick ignores schedules this scheduler doesn't own", func(t *testing.T) {
		db := beginTx(t, conn)
		changed := hourly
		changed.Spec = "@every 2h"
		require.NoError(t, newTestScheduler(t, changed, Schedule{Name: "unknown", Spec: "@hourly"}).Sync(t.Context(), db))
		setNextRunAt(t, db, "hourly", "now() - interval '1 second'")
		setNextRunAt(t, db, "unknown", "now() - interval '1 second'")

		n, err := newTestScheduler(t, hourly).Tick(t.Context(), db)
		require.NoError(t, err)
		assert.Zero(t, n)
	})

	t.Run("Tick rolls back when enqueue fails", func(t *testing.T) {
		db := beginTx(t, conn)
		broken := Schedule{Name: "broken", Spec: "@hourly", Job: EnqueueJob{Queue: "default", Type: "test.broken", Payload: make(chan int)}}
		s := newTestScheduler(t, broken)
		require.NoError(t, s.Sync(t.Context(), db))
		setNextRunAt(t, db, "broken", "now() - interval '1 second'")
		before := readSchedule(t, db, "broken")

		_, err := s.Tick(t.Context(), db)
		require.Error(t, err)

		assert.Equal(t, before, readSchedule(t, db, "broken"))
	})
}

func TestScheduler_ConcurrentTicks(t *testing.T) {
	pool := test.CreatePool(t)
	require.NoError(t, SetupPool(t.Context(), pool))

	hourly := Schedule{Name: "hourly", Spec: "@every 1h", Job: EnqueueJob{Queue: "default", Type: "test.hourly"}}
	replicas := make([]*Scheduler, 8)
	for i := range replicas {
		replicas[i] = newTestScheduler(t, hourly)
	}
	require.NoError(t, replicas[0].Sync(t.Context(), pool))

	const rounds = 25
	for round := 1; round <= rounds; round++ {
		setNextRunAt(t, pool, "hourly", "now() - interval '1 second'")

		start := make(chan struct{})
		enqueued := make([]int, len(replicas))
		errs := make([]error, len(replicas))

		var wg sync.WaitGroup
		for i, s := range replicas {
			wg.Go(func() {
				<-start
				enqueued[i], errs[i] = s.Tick(t.Context(), pool)
			})
		}
		close(start)
		wg.Wait()

		total := 0
		for i := range replicas {
			require.NoError(t, errs[i])
			total += enqueued[i]
		}
		require.Equal(t, 1, total, "round %d", round)
	}

	assert.Equal(t, rounds, countJobs(t, pool, "test.hourly"))
}

func TestScheduler_Run(t *testing.T) {
	pool := test.CreatePool(t)
	require.NoError(t, SetupPool(t.Context(), pool))

	s := newTestScheduler(t, Schedule{Name: "fast", Spec: "@every 1s", Job: EnqueueJob{Queue: "default", Type: "test.fast"}})
	s.Interval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, pool) }()

	require.Eventually(t, func() bool {
		return countJobs(t, pool, "test.fast") >= 2
	}, 5*time.Second, 50*time.Millisecond)

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func newTestScheduler(t testing.TB, schedules ...Schedule) *Scheduler {
	t.Helper()

	s, err := NewScheduler(schedules...)
	require.NoError(t, err)

	return s
}

type scheduleRow struct {
	Spec      string
	NextRunAt time.Time
	LastRunAt *time.Time
}

func readSchedule(t testing.TB, db DB, name string) scheduleRow {
	t.Helper()

	rows, err := db.Query(t.Context(), `SELECT "spec", "next_run_at", "last_run_at" FROM "raptor_schedules" WHERE "name" = $1`, name)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next(), "schedule %s not found", name)
	var row scheduleRow
	require.NoError(t, rows.Scan(&row.Spec, &row.NextRunAt, &row.LastRunAt))
	row.NextRunAt = row.NextRunAt.UTC()
	if row.LastRunAt != nil {
		last := row.LastRunAt.UTC()
		row.LastRunAt = &last
	}

	return row
}

func scheduleNames(t testing.TB, db DB) []string {
	t.Helper()

	rows, err := db.Query(t.Context(), `SELECT "name" FROM "raptor_schedules" ORDER BY "name"`)
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())

	return names
}

func setNextRunAt(t testing.TB, db DB, name, expr string) {
	t.Helper()

	rows, err := db.Query(t.Context(), `UPDATE "raptor_schedules" SET "next_run_at" = `+expr+` WHERE "name" = $1`, name)
	require.NoError(t, err)
	rows.Close()
	require.NoError(t, rows.Err())
}

func dbNow(t testing.TB, db DB) time.Time {
	t.Helper()

	rows, err := db.Query(t.Context(), `SELECT now()`)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var now time.Time
	require.NoError(t, rows.Scan(&now))

	return now.UTC()
}

func countJobs(t testing.TB, db DB, jobType string) int {
	t.Helper()

	rows, err := db.Query(t.Context(), `SELECT count(*) FROM "raptor_jobs" WHERE "job_type" = $1`, jobType)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var n int
	require.NoError(t, rows.Scan(&n))

	return n
}
