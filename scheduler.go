package raptor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

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
	// Name identifies the schedule, together with Spec. It must be unique
	// within a Scheduler and 1-128 characters. Renaming a schedule makes a new
	// one that first fires at its next tick.
	Name string

	// Spec is a five-field cron expression ("*/15 * * * *"), a descriptor
	// ("@hourly", "@daily", ...) or a fixed interval of whole seconds
	// ("@every 5m"), at most 128 characters. It is evaluated in UTC unless prefixed with a zone, as
	// in "CRON_TZ=America/Denver 0 9 * * 1-5". Changing it makes a new
	// schedule that first fires at its next tick.
	Spec string

	// Job is enqueued as-is on every tick. VisibleAt and ExpiresAt must be
	// zero: they are absolute times, so they would apply to every run. Set
	// IdempotencyKey to skip a tick while the previous run is still pending
	// or claimed.
	Job EnqueueJob
}

// Scheduler enqueues jobs for a fixed set of schedules.
//
// Any number of Schedulers can run against one database, such as one per
// worker replica. A stored schedule is identified by its name and spec, and
// each of its ticks is enqueued exactly once across the Schedulers that define
// it. So during a rolling deploy, old and new replicas each run their own
// version of a changed schedule, which may fire on both cadences until the old
// replicas stop, while schedules both versions share fire once.
//
// If no Scheduler ran across several ticks, the next Tick enqueues a single
// job and moves the schedule to its next tick after now; missed ticks are not
// replayed.
//
// A stored schedule more than an hour overdue is abandoned: no running
// Scheduler defines it. Sync deletes abandoned schedules it doesn't define,
// so a schedule removed from code is deleted by a Sync after its last due
// time. A running Scheduler whose schedule is deleted anyway stores it again at
// its next check.
type Scheduler struct {
	// Interval is the longest Run waits between checks for due schedules. Run
	// wakes when the next schedule is due, so Interval only bounds how long it
	// takes to notice a schedule another process stored or failed to enqueue.
	// When another Scheduler holds a due schedule, Run checks again after a
	// short backoff, and waits Interval only if it stays held for about a
	// second. It defaults to one minute.
	Interval time.Duration

	schedules map[string]parsedSchedule
	names     []string
	specs     []string
}

type parsedSchedule struct {
	Schedule
	cron cron.Schedule
	job  enqueueParams
}

const syncLockID int64 = 0x7261_7074_6f72_7363

// NewScheduler returns a Scheduler for schedules. It returns an error if a
// name is empty, too long or repeated, if a spec is too long, doesn't parse,
// never fires or has an interval that isn't a positive number of whole
// seconds, or if a Job can't be enqueued.
func NewScheduler(schedules ...Schedule) (*Scheduler, error) {
	s := &Scheduler{
		Interval:  time.Minute,
		schedules: make(map[string]parsedSchedule, len(schedules)),
		names:     make([]string, 0, len(schedules)),
		specs:     make([]string, 0, len(schedules)),
	}

	for _, sched := range schedules {
		if n := utf8.RuneCountInString(sched.Name); n == 0 || n > 128 {
			return nil, fmt.Errorf("raptor: schedule name %q must be 1-128 characters", sched.Name)
		}
		if _, ok := s.schedules[sched.Name]; ok {
			return nil, fmt.Errorf("raptor: duplicate schedule %q", sched.Name)
		}

		parsed, err := parseSchedule(sched)
		if err != nil {
			return nil, fmt.Errorf("raptor: schedule %q: %w", sched.Name, err)
		}

		s.schedules[sched.Name] = parsed
		s.names = append(s.names, sched.Name)
	}

	slices.Sort(s.names)
	for _, name := range s.names {
		s.specs = append(s.specs, s.schedules[name].Spec)
	}

	return s, nil
}

func parseSchedule(sched Schedule) (parsedSchedule, error) {
	if utf8.RuneCountInString(sched.Spec) > 128 {
		return parsedSchedule{}, errors.New("spec must be at most 128 characters")
	}

	c, err := parseSpec(sched.Spec)
	if err != nil {
		return parsedSchedule{}, err
	}
	if c.Next(time.Now()).IsZero() {
		return parsedSchedule{}, errors.New("spec never fires")
	}
	if err := validateInterval(sched.Spec); err != nil {
		return parsedSchedule{}, err
	}

	if !sched.Job.VisibleAt.IsZero() || !sched.Job.ExpiresAt.IsZero() {
		return parsedSchedule{}, errors.New("job VisibleAt and ExpiresAt must be zero")
	}
	job, err := newEnqueueParams(sched.Job)
	if err != nil {
		return parsedSchedule{}, err
	}

	return parsedSchedule{Schedule: sched, cron: c, job: job}, nil
}

func parseSpec(spec string) (c cron.Schedule, err error) {
	// robfig/cron v3.0.1 panics on a zone prefix with nothing after it.
	defer func() {
		if recover() != nil {
			c, err = nil, fmt.Errorf("invalid spec %q", spec)
		}
	}()

	return cron.ParseStandard(spec)
}

// robfig/cron silently rounds "@every" durations up to 1s and truncates them to whole seconds.
func validateInterval(spec string) error {
	if strings.HasPrefix(spec, "TZ=") || strings.HasPrefix(spec, "CRON_TZ=") {
		spec = strings.TrimSpace(spec[strings.Index(spec, " "):])
	}

	const every = "@every "
	if !strings.HasPrefix(spec, every) {
		return nil
	}

	d, err := time.ParseDuration(spec[len(every):])
	if err != nil {
		return err
	}
	if d < time.Second || d%time.Second != 0 {
		return fmt.Errorf("interval %v must be a positive number of whole seconds", d)
	}
	return nil
}

// Run calls Sync, then calls Tick whenever a schedule is due, checking at
// least every Interval, until ctx is cancelled. Whenever a check finds a
// schedule s defines missing from the database, Run calls Sync again to store
// it, first due at its next tick after now. It returns nil once ctx is
// cancelled. Any other error from Sync or Tick, including a transient
// database error, stops Run and is returned, so callers that need the
// scheduler to keep running should restart it.
func (s *Scheduler) Run(ctx context.Context, db TxDB) error {
	if s.Interval <= 0 {
		return fmt.Errorf("raptor: scheduler interval %v must be positive", s.Interval)
	}

	if err := s.Sync(ctx, db); err != nil {
		return shutdownErr(ctx, err)
	}

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return shutdownErr(ctx, ctx.Err())
		case <-timer.C:
		}

		wait, err := s.poll(ctx, db)
		if err != nil {
			return shutdownErr(ctx, err)
		}
		timer.Reset(min(wait, s.Interval))
	}
}

func shutdownErr(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}
	return err
}

const (
	lostTickBackoff = 250 * time.Millisecond
	lostTickRetries = 4
)

func (s *Scheduler) poll(ctx context.Context, db TxDB) (time.Duration, error) {
	for attempt := 0; ; attempt++ {
		wait, err := s.syncedUntilDue(ctx, db)
		if err != nil || wait > 0 {
			return wait, err
		}

		n, err := s.Tick(ctx, db)
		if err != nil || n > 0 {
			return 0, err
		}
		if attempt == lostTickRetries {
			return s.Interval, nil
		}

		// Another Scheduler holds or just advanced the due row; look again once it commits.
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(lostTickBackoff):
		}
	}
}

func (s *Scheduler) syncedUntilDue(ctx context.Context, db TxDB) (time.Duration, error) {
	wait, stored, err := s.untilDue(ctx, db)
	if err != nil || stored == len(s.names) {
		return wait, err
	}

	if err := s.Sync(ctx, db); err != nil {
		return 0, err
	}
	wait, _, err = s.untilDue(ctx, db)
	return wait, err
}

type earliestDue struct {
	At     *time.Time
	Stored int
	Now    time.Time
}

func (s *Scheduler) untilDue(ctx context.Context, db DB) (time.Duration, int, error) {
	rows, err := db.Query(ctx, `
		SELECT min(s."next_run_at"), count(*), now()
		FROM "raptor_schedules" s
		JOIN unnest($1::text[], $2::text[]) AS k("name", "spec") ON k."name" = s."name" AND k."spec" = s."spec"`,
		s.names, s.specs,
	)
	if err != nil {
		return 0, 0, err
	}
	due, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[earliestDue])
	if err != nil {
		return 0, 0, err
	}

	if due.At == nil {
		return s.Interval, due.Stored, nil
	}
	return due.At.Sub(due.Now), due.Stored, nil
}

// Sync stores each of s's schedules that the database doesn't have yet, first
// due at its next tick after now. A stored schedule with the same name and
// spec keeps its next run, so restarts don't delay or repeat it. Sync also
// deletes abandoned schedules that s doesn't define. Concurrent calls from
// several replicas are serialized.
//
// The database clock decides "now" in Sync and Tick, so clock skew between
// replicas doesn't matter.
func (s *Scheduler) Sync(ctx context.Context, db TxDB) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)

	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT now() FROM pg_advisory_xact_lock($1)`, syncLockID).Scan(&now); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM "raptor_schedules" s
		WHERE s."next_run_at" < now() - interval '1 hour'
		AND NOT EXISTS (
			SELECT 1 FROM unnest($1::text[], $2::text[]) AS k("name", "spec")
			WHERE k."name" = s."name" AND k."spec" = s."spec"
		)`,
		s.names, s.specs,
	); err != nil {
		return err
	}

	nextRunAt := make([]time.Time, len(s.names))
	for i, name := range s.names {
		nextRunAt[i] = s.schedules[name].cron.Next(now.UTC())
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO "raptor_schedules" ("name", "spec", "next_run_at")
		SELECT * FROM unnest($1::text[], $2::text[], $3::timestamptz[])
		ON CONFLICT DO NOTHING`,
		s.names, s.specs, nextRunAt,
	); err != nil {
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
// tick and returns how many schedules fired, including any whose job
// IdempotencyKey deduplicated. It locks due schedules and enqueues their jobs
// in the same transaction, so a concurrent Tick skips them and a failed Tick
// enqueues nothing and leaves them due for the next one.
//
// Run calls Tick for you; call it directly to drive scheduling from your own
// loop.
func (s *Scheduler) Tick(ctx context.Context, db TxDB) (int, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer rollback(ctx, tx)

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
	if len(due) == 0 {
		return 0, nil
	}

	var f firings
	for _, d := range due {
		sched := s.schedules[d.Name]
		f.add(sched, sched.next(d.NextRunAt, d.Now))
	}

	if _, err := tx.Exec(ctx, `
		WITH "fired" AS (
			SELECT * FROM unnest($1::text[], $2::text[], $3::timestamptz[], $4::text[], $5::text[], $6::jsonb[], $7::smallint[], $8::int[], $9::text[])
				AS f("name", "spec", "next_run_at", "queue", "type", "payload", "priority", "max_attempts", "idempotency_key")
		), "advanced" AS (
			UPDATE "raptor_schedules" s
			SET "next_run_at" = f."next_run_at", "last_run_at" = now(), "updated_at" = now()
			FROM "fired" f
			WHERE s."name" = f."name" AND s."spec" = f."spec"
		)
		SELECT "raptor_enqueue_job"(f."queue", f."type", f."payload", f."priority", NULL, NULL, f."max_attempts", f."idempotency_key")
		FROM "fired" f`,
		f.names, f.specs, f.nextRunAts, f.queues, f.types, f.payloads, f.priorities, f.maxAttempts, f.idempotencyKeys,
	); err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(due), nil
}

type firings struct {
	names           []string
	specs           []string
	nextRunAts      []time.Time
	queues          []string
	types           []string
	payloads        []json.RawMessage
	priorities      []int16
	maxAttempts     []int32
	idempotencyKeys []*string
}

func (f *firings) add(p parsedSchedule, next time.Time) {
	f.names = append(f.names, p.Name)
	f.specs = append(f.specs, p.Spec)
	f.nextRunAts = append(f.nextRunAts, next)
	f.queues = append(f.queues, p.job.queue)
	f.types = append(f.types, p.job.jobType)
	f.payloads = append(f.payloads, p.job.payload)
	f.priorities = append(f.priorities, p.job.priority)
	f.maxAttempts = append(f.maxAttempts, p.job.maxAttempts)
	f.idempotencyKeys = append(f.idempotencyKeys, p.job.idempotencyKey)
}

// next stays on cadence while on time and jumps past now once behind, so missed ticks collapse into one run.
func (p parsedSchedule) next(prev, now time.Time) time.Time {
	if next := p.cron.Next(prev.UTC()); next.After(now) {
		return next
	}
	return p.cron.Next(now.UTC())
}

func rollback(ctx context.Context, tx pgx.Tx) {
	ctx, cancel := settleContext(ctx)
	defer cancel()
	_ = tx.Rollback(ctx)
}
