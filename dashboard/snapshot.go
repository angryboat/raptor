package dashboard

import (
	"context"
	"fmt"
	"time"

	"github.com/angryboat/raptor"
	"github.com/jackc/pgx/v5"
)

const listLimit = 50

type snapshot struct {
	Now       time.Time
	Totals    []totals
	Queues    []queue
	JobTypes  []jobType
	Schedules []schedule
	Running   []runningJob
	Retrying  []retryingJob
	Dead      []deadJob
}

type totals struct {
	Label     string
	Enqueued  int64
	Completed int64
	Failed    int64
	Cancelled int64
	Dead      int64
}

type queue struct {
	Name       string
	Ready      int64
	Delayed    int64
	Retrying   int64
	Running    int64
	Expired    int64
	Completed  int64
	Failed     int64
	Cancelled  int64
	Dead       int64
	DurationMs int64
}

func (q queue) AvgDuration() time.Duration {
	return avgDuration(q.DurationMs, q.Completed)
}

type jobType struct {
	Queue      string
	Type       string
	Completed  int64
	Failed     int64
	Cancelled  int64
	Dead       int64
	DurationMs int64
}

func (t jobType) AvgDuration() time.Duration {
	return avgDuration(t.DurationMs, t.Completed)
}

func avgDuration(totalMs, completed int64) time.Duration {
	if completed == 0 {
		return 0
	}
	return time.Duration(totalMs/completed) * time.Millisecond
}

type schedule struct {
	Name      string
	Spec      string
	NextRunAt time.Time
	LastRunAt *time.Time
	Due       bool
	Abandoned bool
}

type runningJob struct {
	ID          string
	Queue       string
	Type        string
	Worker      string
	Attempts    int32
	MaxAttempts int32
	ClaimedAt   time.Time
	Deadline    time.Time
	Expired     bool
}

type retryingJob struct {
	ID          string
	Queue       string
	Type        string
	Attempts    int32
	MaxAttempts int32
	Error       string
	FailedAt    time.Time
	RetryAt     time.Time
}

type deadJob struct {
	ID       string
	Queue    string
	Type     string
	Attempts int32
	Error    string
	DiedAt   time.Time
}

const totalsQuery = `
	SELECT p."label",
		coalesce(s."total_jobs", 0), coalesce(s."total_completed", 0), coalesce(s."total_failed", 0),
		coalesce(s."total_cancelled", 0), coalesce(s."total_dead", 0)
	FROM (VALUES
		(1, 'Today', 'day', CURRENT_DATE),
		(2, 'This week', 'week', date_trunc('week', now())::date),
		(3, 'This month', 'month', date_trunc('month', now())::date),
		(4, 'All time', 'all_time', DATE '1970-01-01')
	) AS p("ord", "label", "span", "date")
	LEFT JOIN "raptor_stats" s ON s."span" = p."span" AND s."date" = p."date"
	ORDER BY p."ord"`

const queuesQuery = `
	SELECT q."name",
		coalesce(a."ready", 0), coalesce(a."delayed", 0), coalesce(a."retrying", 0), coalesce(a."running", 0),
		coalesce(a."expired", 0),
		coalesce(s."completed", 0), coalesce(s."failed", 0), coalesce(s."cancelled", 0), coalesce(s."dead", 0),
		coalesce(s."duration_ms", 0)
	FROM "raptor_queues" q
	LEFT JOIN (
		SELECT "queue_name",
			count(*) FILTER (WHERE "status" = 'pending' AND NOT "expired" AND "failed_at" IS NULL AND "visible_at" <= now()) AS "ready",
			count(*) FILTER (WHERE "status" = 'pending' AND NOT "expired" AND "failed_at" IS NULL AND "visible_at" > now()) AS "delayed",
			count(*) FILTER (WHERE "status" = 'pending' AND NOT "expired" AND "failed_at" IS NOT NULL) AS "retrying",
			count(*) FILTER (WHERE "status" = 'claimed') AS "running",
			count(*) FILTER (WHERE "status" = 'pending' AND "expired") AS "expired"
		FROM (
			SELECT "queue_name", "status", "failed_at", "visible_at", coalesce("expires_at" <= now(), false) AS "expired"
			FROM "raptor_jobs"
			WHERE "status" IN ('pending', 'claimed')
		) j
		GROUP BY "queue_name"
	) a ON a."queue_name" = q."name"
	LEFT JOIN (
		SELECT "queue_name",
			sum("completed_count")::bigint AS "completed", sum("failed_count")::bigint AS "failed",
			sum("cancelled_count")::bigint AS "cancelled", sum("dead_count")::bigint AS "dead",
			sum("total_duration_ms")::bigint AS "duration_ms"
		FROM "raptor_job_stats"
		GROUP BY "queue_name"
	) s ON s."queue_name" = q."name"
	ORDER BY q."name"`

const jobTypesQuery = `
	SELECT "queue_name", "job_type", "completed_count", "failed_count", "cancelled_count", "dead_count", "total_duration_ms"
	FROM "raptor_job_stats"
	ORDER BY "queue_name", "job_type"`

const schedulesQuery = `
	SELECT "name", "spec", "next_run_at", "last_run_at",
		"next_run_at" <= now(), "next_run_at" < now() - interval '1 hour'
	FROM "raptor_schedules"
	ORDER BY "next_run_at", "name"`

const runningQuery = `
	SELECT "id", "queue_name", "job_type", coalesce("claimed_by", ''), "attempts", "max_attempts",
		"claimed_at", "claimed_at" + "claimed_ttl" * interval '1 second',
		"claimed_at" + "claimed_ttl" * interval '1 second' <= now()
	FROM "raptor_jobs"
	WHERE "status" = 'claimed'
	ORDER BY "claimed_at"
	LIMIT $1`

const retryingQuery = `
	SELECT "id", "queue_name", "job_type", "attempts", "max_attempts",
		coalesce("failure_info"->>'error', ''), "failed_at", "visible_at"
	FROM "raptor_jobs"
	WHERE "status" = 'pending' AND "failed_at" IS NOT NULL
	ORDER BY "failed_at" DESC
	LIMIT $1`

const deadQuery = `
	SELECT "id", "queue_name", "job_type", "attempts", coalesce("failure_info"->>'error', ''), "failed_at"
	FROM "raptor_dead_jobs"
	ORDER BY "failed_at" DESC
	LIMIT $1`

func load(ctx context.Context, db raptor.DB) (snapshot, error) {
	var s snapshot
	var err error

	rows, err := db.Query(ctx, `SELECT now()`)
	if err != nil {
		return s, fmt.Errorf("reading clock: %w", err)
	}
	if s.Now, err = pgx.CollectExactlyOneRow(rows, pgx.RowTo[time.Time]); err != nil {
		return s, fmt.Errorf("reading clock: %w", err)
	}

	if s.Totals, err = collect[totals](ctx, db, totalsQuery); err != nil {
		return s, fmt.Errorf("loading totals: %w", err)
	}
	if s.Queues, err = collect[queue](ctx, db, queuesQuery); err != nil {
		return s, fmt.Errorf("loading queues: %w", err)
	}
	if s.JobTypes, err = collect[jobType](ctx, db, jobTypesQuery); err != nil {
		return s, fmt.Errorf("loading job types: %w", err)
	}
	if s.Schedules, err = collect[schedule](ctx, db, schedulesQuery); err != nil {
		return s, fmt.Errorf("loading schedules: %w", err)
	}
	if s.Running, err = collect[runningJob](ctx, db, runningQuery, listLimit); err != nil {
		return s, fmt.Errorf("loading running jobs: %w", err)
	}
	if s.Retrying, err = collect[retryingJob](ctx, db, retryingQuery, listLimit); err != nil {
		return s, fmt.Errorf("loading retrying jobs: %w", err)
	}
	if s.Dead, err = collect[deadJob](ctx, db, deadQuery, listLimit); err != nil {
		return s, fmt.Errorf("loading dead jobs: %w", err)
	}

	return s, nil
}

func collect[T any](ctx context.Context, db raptor.DB, query string, args ...any) ([]T, error) {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[T])
}
