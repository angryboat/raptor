package raptor

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/angryboat/raptor/internal/test"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewScheduler(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		s, err := NewScheduler(
			Schedule{Name: "b", Spec: "@every 5m"},
			Schedule{Name: "a", Spec: "*/15 * * * *"},
			Schedule{Name: "c", Spec: "CRON_TZ=America/Denver 0 9 * * 1-5"},
			Schedule{Name: strings.Repeat("é", 128), Spec: "@hourly"},
			Schedule{Name: "d", Spec: "@every 1s"},
			Schedule{Name: "e", Spec: "TZ=UTC @every 90s"},
		)
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c", "d", "e", strings.Repeat("é", 128)}, s.names)
		assert.Equal(t, []string{"*/15 * * * *", "@every 5m", "CRON_TZ=America/Denver 0 9 * * 1-5", "@every 1s", "TZ=UTC @every 90s", "@hourly"}, s.specs)
	})

	for name, schedules := range map[string][]Schedule{
		"empty name":            {{Spec: "@hourly"}},
		"name too long":         {{Name: strings.Repeat("a", 129), Spec: "@hourly"}},
		"duplicate":             {{Name: "a", Spec: "@hourly"}, {Name: "a", Spec: "@daily"}},
		"invalid spec":          {{Name: "a", Spec: "every hour"}},
		"zone without schedule": {{Name: "a", Spec: "CRON_TZ=UTC"}},
		"spec too long":         {{Name: "a", Spec: "0 " + strings.Repeat("0,", 64) + "1 * * *"}},
		"never fires":           {{Name: "a", Spec: "0 0 31 2 *"}},
		"zero interval":         {{Name: "a", Spec: "@every 0s"}},
		"negative interval":     {{Name: "a", Spec: "@every -5m"}},
		"sub-second interval":   {{Name: "a", Spec: "@every 500ms"}},
		"fractional interval":   {{Name: "a", Spec: "@every 1500ms"}},
		"zoned zero interval":   {{Name: "a", Spec: "CRON_TZ=UTC @every 0s"}},
		"queue too long":        {{Name: "a", Spec: "@hourly", Job: EnqueueJob{Queue: strings.Repeat("q", 65)}}},
		"type too long":         {{Name: "a", Spec: "@hourly", Job: EnqueueJob{Type: strings.Repeat("t", 65)}}},
		"key too long":          {{Name: "a", Spec: "@hourly", Job: EnqueueJob{IdempotencyKey: strings.Repeat("k", 129)}}},
		"VisibleAt set":         {{Name: "a", Spec: "@hourly", Job: EnqueueJob{VisibleAt: time.Now()}}},
		"ExpiresAt set":         {{Name: "a", Spec: "@hourly", Job: EnqueueJob{ExpiresAt: time.Now()}}},
		"payload won't marshal": {{Name: "a", Spec: "@hourly", Job: EnqueueJob{Payload: make(chan int)}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewScheduler(schedules...)
			assert.ErrorContains(t, err, "raptor:")
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

		row := readSchedule(t, db, hourly)
		assert.Equal(t, dbNow(t, db).Truncate(time.Second).Add(time.Hour), row.NextRunAt)
	})

	t.Run("Sync keeps next_run_at for unchanged specs", func(t *testing.T) {
		db := beginTx(t, conn)
		s := newTestScheduler(t, hourly)
		require.NoError(t, s.Sync(t.Context(), db))
		setNextRunAt(t, db, hourly, "now() + interval '5 minutes'")
		before := readSchedule(t, db, hourly)

		require.NoError(t, s.Sync(t.Context(), db))

		assert.Equal(t, before.NextRunAt, readSchedule(t, db, hourly).NextRunAt)
	})

	t.Run("Sync adds a changed spec alongside the old one", func(t *testing.T) {
		db := beginTx(t, conn)
		require.NoError(t, newTestScheduler(t, hourly).Sync(t.Context(), db))
		before := readSchedule(t, db, hourly)

		changed := hourly
		changed.Spec = "@every 1m"
		require.NoError(t, newTestScheduler(t, changed).Sync(t.Context(), db))

		assert.Equal(t, []scheduleKey{{"hourly", "@every 1h"}, {"hourly", "@every 1m"}}, scheduleKeys(t, db))
		assert.Equal(t, before, readSchedule(t, db, hourly))
		assert.Equal(t, dbNow(t, db).Truncate(time.Second).Add(time.Minute), readSchedule(t, db, changed).NextRunAt)
	})

	t.Run("Sync deletes abandoned schedules it doesn't define", func(t *testing.T) {
		db := beginTx(t, conn)
		abandoned := Schedule{Name: "abandoned", Spec: "@daily"}
		overdue := Schedule{Name: "overdue", Spec: "@daily"}
		upcoming := Schedule{Name: "upcoming", Spec: "@daily"}
		require.NoError(t, newTestScheduler(t, hourly, abandoned, overdue, upcoming).Sync(t.Context(), db))
		setNextRunAt(t, db, hourly, "now() - interval '2 hours'")
		setNextRunAt(t, db, abandoned, "now() - interval '61 minutes'")
		setNextRunAt(t, db, overdue, "now() - interval '59 minutes'")

		require.NoError(t, newTestScheduler(t, hourly).Sync(t.Context(), db))

		assert.Equal(t, []scheduleKey{{"hourly", "@every 1h"}, {"overdue", "@daily"}, {"upcoming", "@daily"}}, scheduleKeys(t, db))
	})

	t.Run("old and new replicas each run their own schedules", func(t *testing.T) {
		db := beginTx(t, conn)
		shared := Schedule{Name: "shared", Spec: "@hourly", Job: EnqueueJob{Queue: "default", Type: "test.shared"}}
		removed := Schedule{Name: "removed", Spec: "@hourly", Job: EnqueueJob{Queue: "default", Type: "test.removed"}}
		added := Schedule{Name: "added", Spec: "@hourly", Job: EnqueueJob{Queue: "default", Type: "test.added"}}
		changed := hourly
		changed.Spec = "@every 2h"
		old := newTestScheduler(t, shared, hourly, removed)
		next := newTestScheduler(t, shared, changed, added)

		require.NoError(t, old.Sync(t.Context(), db))
		require.NoError(t, next.Sync(t.Context(), db))
		before := map[scheduleKey]scheduleRow{}
		for _, key := range scheduleKeys(t, db) {
			before[key] = readSchedule(t, db, Schedule{Name: key.Name, Spec: key.Spec})
		}

		require.NoError(t, old.Sync(t.Context(), db))

		require.Len(t, before, 5)
		for key, row := range before {
			assert.Equal(t, row, readSchedule(t, db, Schedule{Name: key.Name, Spec: key.Spec}), "%v", key)
		}

		for key := range before {
			setNextRunAt(t, db, Schedule{Name: key.Name, Spec: key.Spec}, "now() - interval '1 second'")
		}
		n, err := next.Tick(t.Context(), db)
		require.NoError(t, err)
		assert.Equal(t, 3, n)
		n, err = old.Tick(t.Context(), db)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
		assert.Equal(t, 1, countJobs(t, db, "test.shared"))
		assert.Equal(t, 2, countJobs(t, db, "test.hourly"))
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
		setNextRunAt(t, db, hourly, "date_trunc('second', now()) - interval '10 seconds'")
		prev := readSchedule(t, db, hourly).NextRunAt

		n, err := s.Tick(t.Context(), db)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.Equal(t, 1, countJobs(t, db, "test.hourly"))

		row := readSchedule(t, db, hourly)
		assert.Equal(t, prev.Add(time.Hour), row.NextRunAt)
		require.NotNil(t, row.LastRunAt)
		assert.Equal(t, dbNow(t, db), *row.LastRunAt)

		n, err = s.Tick(t.Context(), db)
		require.NoError(t, err)
		assert.Zero(t, n)
	})

	t.Run("Tick enqueues each schedule's job", func(t *testing.T) {
		db := beginTx(t, conn)
		plain := Schedule{Name: "plain", Spec: "@hourly", Job: EnqueueJob{Queue: "default", Type: "test.plain"}}
		custom := Schedule{Name: "custom", Spec: "@hourly", Job: EnqueueJob{
			Queue:          "other",
			Type:           "test.custom",
			Payload:        map[string]int{"n": 1},
			Priority:       5,
			MaxAttempts:    3,
			IdempotencyKey: "custom",
		}}
		s := newTestScheduler(t, plain, custom)
		require.NoError(t, s.Sync(t.Context(), db))
		setNextRunAt(t, db, plain, "now() - interval '1 second'")
		setNextRunAt(t, db, custom, "now() - interval '1 second'")

		n, err := s.Tick(t.Context(), db)
		require.NoError(t, err)
		assert.Equal(t, 2, n)

		got := readJob(t, db, "test.plain")
		assert.Equal(t, scheduledJob{Queue: "default", Payload: json.RawMessage("null"), MaxAttempts: 10}, got)
		key := "custom"
		got = readJob(t, db, "test.custom")
		assert.Equal(t, scheduledJob{Queue: "other", Payload: json.RawMessage(`{"n": 1}`), Priority: 5, MaxAttempts: 3, IdempotencyKey: &key}, got)
	})

	t.Run("Tick collapses missed ticks into one job", func(t *testing.T) {
		db := beginTx(t, conn)
		minutely := Schedule{Name: "minutely", Spec: "* * * * *", Job: EnqueueJob{Queue: "default", Type: "test.minutely"}}
		s := newTestScheduler(t, minutely)
		require.NoError(t, s.Sync(t.Context(), db))
		setNextRunAt(t, db, minutely, "now() - interval '1 day'")

		n, err := s.Tick(t.Context(), db)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.Equal(t, 1, countJobs(t, db, "test.minutely"))

		now := dbNow(t, db)
		assert.Equal(t, now.Truncate(time.Minute).Add(time.Minute), readSchedule(t, db, minutely).NextRunAt)
	})

	t.Run("Tick ignores schedules this scheduler doesn't own", func(t *testing.T) {
		db := beginTx(t, conn)
		changed := hourly
		changed.Spec = "@every 2h"
		unknown := Schedule{Name: "unknown", Spec: "@hourly"}
		require.NoError(t, newTestScheduler(t, changed, unknown).Sync(t.Context(), db))
		setNextRunAt(t, db, changed, "now() - interval '1 second'")
		setNextRunAt(t, db, unknown, "now() - interval '1 second'")

		n, err := newTestScheduler(t, hourly).Tick(t.Context(), db)
		require.NoError(t, err)
		assert.Zero(t, n)
	})

	t.Run("Tick counts a tick that IdempotencyKey deduplicates", func(t *testing.T) {
		db := beginTx(t, conn)
		keyed := Schedule{Name: "keyed", Spec: "@hourly", Job: EnqueueJob{Queue: "default", Type: "test.keyed", IdempotencyKey: "keyed"}}
		s := newTestScheduler(t, keyed)
		require.NoError(t, s.Sync(t.Context(), db))

		for range 2 {
			setNextRunAt(t, db, keyed, "now() - interval '1 second'")
			n, err := s.Tick(t.Context(), db)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
		}
		assert.Equal(t, 1, countJobs(t, db, "test.keyed"))
	})

	t.Run("Tick rolls back every schedule when an enqueue fails", func(t *testing.T) {
		db := beginTx(t, conn)
		broken := Schedule{Name: "broken", Spec: "@hourly", Job: EnqueueJob{Queue: "default", Type: "test.broken"}}
		s := newTestScheduler(t, broken, hourly)
		require.NoError(t, s.Sync(t.Context(), db))
		setNextRunAt(t, db, broken, "now() - interval '1 second'")
		setNextRunAt(t, db, hourly, "now() - interval '1 second'")
		before := []scheduleRow{readSchedule(t, db, broken), readSchedule(t, db, hourly)}
		_, err := db.Exec(t.Context(), `ALTER TABLE "raptor_jobs" ADD CONSTRAINT "test_broken" CHECK ("job_type" <> 'test.broken')`)
		require.NoError(t, err)

		_, err = s.Tick(t.Context(), db)
		require.Error(t, err)

		assert.Equal(t, before, []scheduleRow{readSchedule(t, db, broken), readSchedule(t, db, hourly)})
		assert.Zero(t, countJobs(t, db, "test.hourly"))
	})

	t.Run("Tick rolls back when an enqueue can't settle its idempotency key", func(t *testing.T) {
		db := beginTx(t, conn)
		keyed := Schedule{Name: "keyed", Spec: "@hourly", Job: EnqueueJob{Queue: "default", Type: "test.keyed", IdempotencyKey: "keyed"}}
		s := newTestScheduler(t, keyed)
		require.NoError(t, s.Sync(t.Context(), db))
		setNextRunAt(t, db, keyed, "now() - interval '1 second'")
		before := readSchedule(t, db, keyed)
		keepStealingKey(t, db, "default", "test.keyed", "keyed")

		_, err := s.Tick(t.Context(), db)
		require.Error(t, err)

		assert.Equal(t, before, readSchedule(t, db, keyed))
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
		setNextRunAt(t, pool, hourly, "now() - interval '1 second'")

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

func TestScheduler_ConcurrentSync(t *testing.T) {
	pool := test.CreatePool(t)
	require.NoError(t, SetupPool(t.Context(), pool))

	t.Run("replicas with different schedules", func(t *testing.T) {
		replicas := make([]*Scheduler, 8)
		for i := range replicas {
			replicas[i] = newTestScheduler(t,
				Schedule{Name: "shared", Spec: "@hourly"},
				Schedule{Name: "versioned", Spec: []string{"@hourly", "@daily"}[i%2]},
			)
		}

		start := make(chan struct{})
		errs := make([]error, len(replicas))
		var wg sync.WaitGroup
		for i, s := range replicas {
			wg.Go(func() {
				<-start
				errs[i] = s.Sync(t.Context(), pool)
			})
		}
		close(start)
		wg.Wait()

		for _, err := range errs {
			require.NoError(t, err)
		}
		assert.Equal(t, []scheduleKey{{"shared", "@hourly"}, {"versioned", "@daily"}, {"versioned", "@hourly"}}, scheduleKeys(t, pool))
	})

	t.Run("a Sync deleting a schedule another Sync revives", func(t *testing.T) {
		added := Schedule{Name: "a", Spec: "@hourly"}
		revived := Schedule{Name: "b", Spec: "@hourly"}
		require.NoError(t, newTestScheduler(t, revived).Sync(t.Context(), pool))
		setNextRunAt(t, pool, revived, "now() - interval '2 hours'")

		other, err := pool.Begin(t.Context())
		require.NoError(t, err)
		defer other.Rollback(context.Background())
		_, err = other.Exec(t.Context(), `SELECT pg_advisory_xact_lock($1)`, syncLockID)
		require.NoError(t, err)
		_, err = other.Exec(t.Context(), `DELETE FROM "raptor_schedules" WHERE "name" = 'b'`)
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() { done <- newTestScheduler(t, added, revived).Sync(t.Context(), pool) }()
		require.Eventually(t, func() bool { return lockWaiters(t, pool) > 0 }, 5*time.Second, 10*time.Millisecond)

		_, err = other.Exec(t.Context(), `INSERT INTO "raptor_schedules" ("name", "spec", "next_run_at") VALUES ('a', '@hourly', now()) ON CONFLICT DO NOTHING`)
		require.NoError(t, err)
		require.NoError(t, other.Commit(t.Context()))

		require.NoError(t, <-done)
		assert.Subset(t, scheduleKeys(t, pool), []scheduleKey{{"a", "@hourly"}, {"b", "@hourly"}})
	})
}

func TestScheduler_Run(t *testing.T) {
	pool := test.CreatePool(t)
	require.NoError(t, SetupPool(t.Context(), pool))

	t.Run("enqueues each tick once and wakes when due", func(t *testing.T) {
		fast := Schedule{Name: "fast", Spec: "@every 1s", Job: EnqueueJob{Queue: "default", Type: "test.fast"}}
		replicas := []*Scheduler{newTestScheduler(t, fast), newTestScheduler(t, fast)}

		ctx, cancel := context.WithCancel(t.Context())
		start := dbNow(t, pool)
		var wg sync.WaitGroup
		errs := make([]error, len(replicas))
		for i, s := range replicas {
			s.Interval = time.Hour
			wg.Go(func() { errs[i] = s.Run(ctx, pool) })
		}

		require.Eventually(t, func() bool {
			return countJobs(t, pool, "test.fast") >= 3
		}, 10*time.Second, 50*time.Millisecond)

		cancel()
		wg.Wait()
		elapsed := dbNow(t, pool).Sub(start)

		for _, err := range errs {
			assert.NoError(t, err)
		}
		assert.LessOrEqual(t, countJobs(t, pool, "test.fast"), int(elapsed.Seconds())+1)
	})

	t.Run("takes over the next tick from a replica that won a race and stopped", func(t *testing.T) {
		race := Schedule{Name: "race", Spec: "@every 1s", Job: EnqueueJob{Queue: "default", Type: "test.race"}}
		winner, loser := newTestScheduler(t, race), newTestScheduler(t, race)
		loser.Interval = time.Hour
		require.NoError(t, winner.Sync(t.Context(), pool))
		setNextRunAt(t, pool, race, "now() - interval '1 second'")

		atFirstTick, startFirstTick := make(chan struct{}), make(chan struct{})
		lost := make(chan struct{}, 16)
		loserDB := &hookedDB{
			TxDB: pool,
			onBegin: func(n int32) {
				if n == 2 {
					close(atFirstTick)
					<-startFirstTick
				}
			},
			onRollback: func() { lost <- struct{}{} },
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- loser.Run(ctx, loserDB) }()
		<-atFirstTick

		holding, release := make(chan struct{}), make(chan struct{})
		winnerDB := &hookedDB{TxDB: pool, onCommit: func() {
			close(holding)
			<-release
		}}
		won := make(chan error, 1)
		go func() {
			_, err := winner.Tick(t.Context(), winnerDB)
			won <- err
		}()
		<-holding
		close(startFirstTick)
		<-lost
		close(release)
		require.NoError(t, <-won)
		require.Equal(t, 1, countJobs(t, pool, "test.race"))

		assert.Eventually(t, func() bool {
			return countJobs(t, pool, "test.race") >= 2
		}, 3*time.Second, 50*time.Millisecond)

		cancel()
		assert.NoError(t, <-done)
	})

	t.Run("waits Interval once a due schedule stays locked", func(t *testing.T) {
		stuck := Schedule{Name: "stuck", Spec: "@every 1s", Job: EnqueueJob{Queue: "default", Type: "test.stuck"}}
		s := newTestScheduler(t, stuck)
		s.Interval = time.Hour
		require.NoError(t, s.Sync(t.Context(), pool))
		setNextRunAt(t, pool, stuck, "now() - interval '1 second'")

		holder, err := pool.Begin(t.Context())
		require.NoError(t, err)
		defer holder.Rollback(context.Background())
		_, err = holder.Exec(t.Context(), `SELECT 1 FROM "raptor_schedules" WHERE "name" = 'stuck' FOR UPDATE`)
		require.NoError(t, err)

		var lost atomic.Int32
		db := &hookedDB{TxDB: pool, onRollback: func() { lost.Add(1) }}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx, db) }()

		require.Eventually(t, func() bool {
			return lost.Load() == lostTickRetries+1
		}, 3*time.Second, 10*time.Millisecond)
		time.Sleep(3 * lostTickBackoff)
		assert.Equal(t, int32(lostTickRetries+1), lost.Load())

		cancel()
		assert.NoError(t, <-done)
	})

	t.Run("rejects a non-positive Interval", func(t *testing.T) {
		s := newTestScheduler(t)
		s.Interval = 0
		assert.ErrorContains(t, s.Run(t.Context(), pool), "raptor:")
	})

	t.Run("returns a Sync error", func(t *testing.T) {
		s := newTestScheduler(t)
		assert.ErrorIs(t, s.Run(t.Context(), &failingBegin{TxDB: pool}), errBeginFailed)
	})

	t.Run("returns a Tick error", func(t *testing.T) {
		s := newTestScheduler(t, Schedule{Name: "failing", Spec: "@every 1s"})
		s.Interval = time.Hour
		db := &failingBegin{TxDB: pool}
		db.allowed.Store(1)

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		assert.ErrorIs(t, s.Run(ctx, db), errBeginFailed)
	})
}

var errBeginFailed = errors.New("begin failed")

type failingBegin struct {
	TxDB
	allowed atomic.Int32
}

func (f *failingBegin) Begin(ctx context.Context) (pgx.Tx, error) {
	if f.allowed.Add(-1) < 0 {
		return nil, errBeginFailed
	}
	return f.TxDB.Begin(ctx)
}

type hookedDB struct {
	TxDB
	begins     atomic.Int32
	onBegin    func(int32)
	onCommit   func()
	onRollback func()
}

func (h *hookedDB) Begin(ctx context.Context) (pgx.Tx, error) {
	if n := h.begins.Add(1); h.onBegin != nil {
		h.onBegin(n)
	}
	tx, err := h.TxDB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &hookedTx{Tx: tx, db: h}, nil
}

type hookedTx struct {
	pgx.Tx
	db        *hookedDB
	committed bool
}

func (t *hookedTx) Commit(ctx context.Context) error {
	if t.db.onCommit != nil {
		t.db.onCommit()
	}
	t.committed = true
	return t.Tx.Commit(ctx)
}

func (t *hookedTx) Rollback(ctx context.Context) error {
	if !t.committed && t.db.onRollback != nil {
		t.db.onRollback()
	}
	return t.Tx.Rollback(ctx)
}

func newTestScheduler(t testing.TB, schedules ...Schedule) *Scheduler {
	t.Helper()

	s, err := NewScheduler(schedules...)
	require.NoError(t, err)

	return s
}

type scheduleRow struct {
	NextRunAt time.Time
	LastRunAt *time.Time
}

func readSchedule(t testing.TB, db DB, sched Schedule) scheduleRow {
	t.Helper()

	rows, err := db.Query(t.Context(), `SELECT "next_run_at", "last_run_at" FROM "raptor_schedules" WHERE "name" = $1 AND "spec" = $2`, sched.Name, sched.Spec)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next(), "schedule %s %q not found", sched.Name, sched.Spec)
	var row scheduleRow
	require.NoError(t, rows.Scan(&row.NextRunAt, &row.LastRunAt))
	row.NextRunAt = row.NextRunAt.UTC()
	if row.LastRunAt != nil {
		last := row.LastRunAt.UTC()
		row.LastRunAt = &last
	}

	return row
}

type scheduleKey struct {
	Name string
	Spec string
}

func scheduleKeys(t testing.TB, db DB) []scheduleKey {
	t.Helper()

	rows, err := db.Query(t.Context(), `SELECT "name", "spec" FROM "raptor_schedules" ORDER BY "name", "spec"`)
	require.NoError(t, err)

	keys, err := pgx.CollectRows(rows, pgx.RowToStructByPos[scheduleKey])
	require.NoError(t, err)

	return keys
}

func setNextRunAt(t testing.TB, db DB, sched Schedule, expr string) {
	t.Helper()

	rows, err := db.Query(t.Context(), `UPDATE "raptor_schedules" SET "next_run_at" = `+expr+` WHERE "name" = $1 AND "spec" = $2`, sched.Name, sched.Spec)
	require.NoError(t, err)
	rows.Close()
	require.NoError(t, rows.Err())
	require.Equal(t, int64(1), rows.CommandTag().RowsAffected(), "schedule %s %q not found", sched.Name, sched.Spec)
}

type scheduledJob struct {
	Queue          string
	Payload        json.RawMessage
	Priority       int16
	MaxAttempts    int32
	IdempotencyKey *string
}

func readJob(t testing.TB, db DB, jobType string) scheduledJob {
	t.Helper()

	rows, err := db.Query(t.Context(), `
		SELECT "queue_name", "job_data", "priority", "max_attempts", "idempotency_key"
		FROM "raptor_jobs" WHERE "job_type" = $1`, jobType)
	require.NoError(t, err)

	job, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[scheduledJob])
	require.NoError(t, err)

	return job
}

func lockWaiters(t testing.TB, db DB) int {
	t.Helper()

	rows, err := db.Query(t.Context(), `SELECT count(*) FROM pg_stat_activity WHERE "datname" = current_database() AND "wait_event_type" = 'Lock'`)
	require.NoError(t, err)

	n, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[int])
	require.NoError(t, err)

	return n
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
