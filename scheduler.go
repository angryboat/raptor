package raptor

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/robfig/cron/v3"
)

// TxDB is a DB that can open a transaction, such as *pgxpool.Pool,
// *pgx.Conn or pgx.Tx.
type TxDB interface {
	DB
	Begin(context.Context) (pgx.Tx, error)
}

// Schedule enqueues a copy of Job every time Spec fires.
type Schedule struct {
	// Name identifies the schedule in the database. It must be unique and
	// 1-128 characters. Renaming a schedule deletes the old one and creates
	// a new one that first fires at its next tick.
	Name string

	// Spec is a five-field cron expression ("*/15 * * * *"), a descriptor
	// ("@hourly", "@daily", ...) or a fixed interval ("@every 5m"). It is
	// evaluated in UTC unless prefixed with a zone, as in
	// "CRON_TZ=America/Denver 0 9 * * 1-5". Changing it reschedules the next
	// run from the time of the next Sync.
	Spec string

	// Job is enqueued as-is on every tick. Leave VisibleAt and ExpiresAt
	// zero: they are absolute times, so they would apply to every run. Set
	// IdempotencyKey to skip a tick while the previous run is still pending
	// or claimed.
	Job EnqueueJob
}

// Scheduler enqueues jobs for a fixed set of schedules.
//
// Any number of Schedulers built from the same schedules can run against one
// database, such as one per worker replica; each tick is enqueued exactly
// once across all of them. A Scheduler only enqueues schedules whose name and
// spec match its own, so replicas running old and new code side by side don't
// fight over a changed schedule.
//
// If no Scheduler ran across several ticks, the next Tick enqueues a single
// job and moves the schedule to its next tick after now; missed ticks are not
// replayed.
//
// Sync deletes every stored schedule a Scheduler doesn't know about, so a
// database should have one set of schedules, not several Schedulers with
// different ones.
type Scheduler struct {
	// Interval is how often Run checks for due schedules. It defaults to one
	// second and bounds how late a job can be enqueued after its tick.
	Interval time.Duration

	schedules map[string]parsedSchedule
	names     []string
	specs     []string
}

type parsedSchedule struct {
	Schedule
	cron cron.Schedule
}

// NewScheduler returns a Scheduler for schedules. It returns an error if a
// name is empty, too long or repeated, or if a spec doesn't parse or never
// fires.
func NewScheduler(schedules ...Schedule) (*Scheduler, error) {
	s := &Scheduler{
		Interval:  1 * time.Second,
		schedules: make(map[string]parsedSchedule, len(schedules)),
		names:     make([]string, 0, len(schedules)),
		specs:     make([]string, 0, len(schedules)),
	}

	for _, sched := range schedules {
		if sched.Name == "" || len(sched.Name) > 128 {
			return nil, fmt.Errorf("raptor: schedule name %q must be 1-128 characters", sched.Name)
		}
		if _, ok := s.schedules[sched.Name]; ok {
			return nil, fmt.Errorf("raptor: duplicate schedule %q", sched.Name)
		}

		c, err := cron.ParseStandard(sched.Spec)
		if err != nil {
			return nil, fmt.Errorf("raptor: schedule %q: %w", sched.Name, err)
		}
		if c.Next(time.Now()).IsZero() {
			return nil, fmt.Errorf("raptor: schedule %q never fires", sched.Name)
		}

		s.schedules[sched.Name] = parsedSchedule{Schedule: sched, cron: c}
		s.names = append(s.names, sched.Name)
	}

	slices.Sort(s.names)
	for _, name := range s.names {
		s.specs = append(s.specs, s.schedules[name].Spec)
	}

	return s, nil
}

// Run calls Sync, then calls Tick every Interval until ctx is cancelled.
// It returns nil once ctx is cancelled. Any other error from Sync or Tick,
// including a transient database error, stops Run and is returned, so callers
// that need the scheduler to keep running should restart it.
func (s *Scheduler) Run(ctx context.Context, db TxDB) error {
	if err := s.Sync(ctx, db); err != nil {
		return shutdownErr(ctx, err)
	}

	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	for {
		if _, err := s.Tick(ctx, db); err != nil {
			return shutdownErr(ctx, err)
		}

		select {
		case <-ctx.Done():
			return shutdownErr(ctx, ctx.Err())
		case <-ticker.C:
		}
	}
}

func shutdownErr(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}
	return err
}

// Sync makes the stored schedules match s in one transaction. New schedules
// first fire at their next tick after now, not immediately. A schedule whose
// spec changed is rescheduled the same way. An unchanged schedule keeps its
// next run, so restarts don't delay or repeat it. Stored schedules missing
// from s are deleted. Concurrent calls from several replicas are safe.
//
// The database clock decides "now" in Sync and Tick, so clock skew between
// replicas doesn't matter.
func (s *Scheduler) Sync(ctx context.Context, db TxDB) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return err
	}

	nextRunAt := make([]time.Time, len(s.names))
	for i, name := range s.names {
		nextRunAt[i] = s.schedules[name].cron.Next(now.UTC())
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO "raptor_schedules" ("name", "spec", "next_run_at")
		SELECT * FROM unnest($1::text[], $2::text[], $3::timestamptz[])
		ON CONFLICT ("name") DO UPDATE SET
			"spec" = EXCLUDED."spec",
			"next_run_at" = EXCLUDED."next_run_at",
			"updated_at" = now()
		WHERE "raptor_schedules"."spec" <> EXCLUDED."spec"`,
		s.names, s.specs, nextRunAt,
	); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM "raptor_schedules" WHERE "name" <> ALL($1::text[])`, s.names); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

type dueSchedule struct {
	Name      string
	NextRunAt time.Time
	Now       time.Time
}

// Tick enqueues one job for every due schedule, advances each to its next
// tick and returns how many it enqueued. It locks due schedules and enqueues
// their jobs in the same transaction, so a concurrent Tick skips them and a
// failed Tick enqueues nothing and leaves them due for the next one.
//
// Run calls Tick for you; call it directly to drive scheduling from your own
// loop.
func (s *Scheduler) Tick(ctx context.Context, db TxDB) (int, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT s."name", s."next_run_at", now()
		FROM "raptor_schedules" s
		JOIN unnest($1::text[], $2::text[]) AS k("name", "spec") ON k."name" = s."name" AND k."spec" = s."spec"
		WHERE s."next_run_at" <= now()
		ORDER BY s."name"
		FOR UPDATE OF s SKIP LOCKED`,
		s.names, s.specs,
	)
	if err != nil {
		return 0, err
	}
	due, err := pgx.CollectRows(rows, pgx.RowToStructByPos[dueSchedule])
	if err != nil {
		return 0, err
	}

	for _, d := range due {
		sched := s.schedules[d.Name]

		if _, err := tx.Exec(ctx,
			`UPDATE "raptor_schedules" SET "next_run_at" = $2, "last_run_at" = $3, "updated_at" = $3 WHERE "name" = $1`,
			d.Name, sched.next(d.NextRunAt, d.Now), d.Now,
		); err != nil {
			return 0, err
		}

		if _, err := Enqueue(ctx, tx, sched.Job); err != nil {
			return 0, fmt.Errorf("raptor: schedule %q: %w", d.Name, err)
		}
	}

	return len(due), tx.Commit(ctx)
}

// next stays on cadence while on time and jumps past now once behind, so missed ticks collapse into one run.
func (p parsedSchedule) next(prev, now time.Time) time.Time {
	if next := p.cron.Next(prev.UTC()); next.After(now) {
		return next
	}
	return p.cron.Next(now.UTC())
}
