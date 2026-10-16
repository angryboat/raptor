package raptor

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"time"

	"github.com/angryboat/raptor/internal/migrate"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Setup(ctx context.Context, conn *pgx.Conn) error {
	if err := conn.Ping(ctx); err != nil {
		return err
	}

	return migrate.Up(ctx, conn)
}

func SetupPool(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	return Setup(ctx, conn.Conn())
}

type DB interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type Job struct {
	ID             string          `db:"id"`
	Queue          string          `db:"queue_name"`
	Type           string          `db:"job_type"`
	Payload        json.RawMessage `db:"job_data"`
	Priority       int16           `db:"priority"`
	Status         string          `db:"status"`
	CreatedAt      time.Time       `db:"created_at"`
	VisibleAt      time.Time       `db:"visible_at"`
	ExpiresAt      *time.Time      `db:"expires_at"`
	ClaimedAt      *time.Time      `db:"claimed_at"`
	ClaimedBy      *string         `db:"claimed_by"`
	ClaimedTTL     int32           `db:"claimed_ttl"`
	CompletedAt    *time.Time      `db:"completed_at"`
	CancelledAt    *time.Time      `db:"cancelled_at"`
	FailedAt       *time.Time      `db:"failed_at"`
	Attempts       int32           `db:"attempts"`
	MaxAttempts    int32           `db:"max_attempts"`
	TimeoutMs      int32           `db:"timeout_ms"`
	FailureInfo    json.RawMessage `db:"failure_info"`
	IdempotencyKey *string         `db:"idempotency_key"`
}

const jobColumns = `"id", "queue_name", "job_type", "job_data", "priority", "status", "created_at", "visible_at", "expires_at", "claimed_at", "claimed_by", "claimed_ttl", "completed_at", "cancelled_at", "failed_at", "attempts", "max_attempts", "timeout_ms", "failure_info", "idempotency_key"`

type EnqueueJob struct {
	Queue          string
	Type           string
	Priority       int16
	Payload        any
	VisibleAt      time.Time
	ExpiresAt      time.Time
	MaxAttempts    int32
	IdempotencyKey string
}

type enqueueParams struct {
	queue          string
	jobType        string
	payload        json.RawMessage
	priority       int16
	visibleAt      *time.Time
	expiresAt      *time.Time
	maxAttempts    int32
	idempotencyKey *string
}

func newEnqueueParams(job EnqueueJob) (enqueueParams, error) {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return enqueueParams{}, err
	}

	p := enqueueParams{
		queue:       job.Queue,
		jobType:     job.Type,
		payload:     payload,
		priority:    job.Priority,
		maxAttempts: job.MaxAttempts,
	}
	if !job.VisibleAt.IsZero() {
		p.visibleAt = &job.VisibleAt
	}
	if !job.ExpiresAt.IsZero() {
		p.expiresAt = &job.ExpiresAt
	}
	if p.maxAttempts <= 0 {
		p.maxAttempts = 10
	}
	if job.IdempotencyKey != "" {
		p.idempotencyKey = &job.IdempotencyKey
	}

	return p, nil
}

func Enqueue(ctx context.Context, db DB, job EnqueueJob) (string, error) {
	p, err := newEnqueueParams(job)
	if err != nil {
		return "", err
	}

	rows, err := db.Query(ctx, `SELECT "raptor_enqueue_job"($1, $2, $3, $4, $5, $6, $7, $8)`,
		p.queue,
		p.jobType,
		p.payload,
		p.priority,
		p.visibleAt,
		p.expiresAt,
		p.maxAttempts,
		p.idempotencyKey,
	)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var id *string
	if rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return "", err
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	if id == nil {
		return "", nil
	}

	return *id, nil
}

// Claim claims up to limit pending jobs from queue for worker. A positive
// timeout sets each job's claim TTL, rounded up to whole seconds because TTLs
// are stored in seconds, so a sub-second timeout claims for one second. A zero
// or negative timeout keeps each job's existing TTL (5 minutes by default).
func Claim(ctx context.Context, db DB, queue, worker string, limit int, timeout time.Duration) ([]*Job, error) {
	if limit <= 0 {
		limit = 1
	}

	var claimedTTL any
	if timeout > 0 {
		claimedTTL = ttlSeconds(timeout)
	}

	rows, err := db.Query(ctx, `SELECT `+jobColumns+` FROM "raptor_claim_jobs"($1, $2, $3, $4)`,
		queue,
		worker,
		limit,
		claimedTTL,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[Job])
}

var (
	ErrJobNotFound = errors.New("raptor: job not found")
	ErrClaimLost   = errors.New("raptor: job claim lost")
)

// ClaimDeadline is when the job's claim expires and Sweep may hand it to
// another worker.
func (j *Job) ClaimDeadline() time.Time {
	if j.ClaimedAt == nil {
		return time.Time{}
	}
	return j.ClaimedAt.Add(time.Duration(j.ClaimedTTL) * time.Second)
}

func ttlSeconds(d time.Duration) int32 {
	secs := d / time.Second
	if d%time.Second > 0 {
		secs++
	}
	switch {
	case secs <= 0:
		return 0
	case secs > math.MaxInt32:
		return math.MaxInt32
	}
	return int32(secs)
}

// Complete marks a claimed job as completed. It returns ErrClaimLost if the
// job was reaped or reclaimed since job was claimed.
func Complete(ctx context.Context, db DB, job *Job) error {
	return settleClaim(ctx, db, `SELECT "raptor_complete_job"($1, $2, $3)`, job.ID, job.ClaimedBy, job.Attempts)
}

func Cancel(ctx context.Context, db DB, jobID string) error {
	rows, err := db.Query(ctx, `SELECT "raptor_cancel_job"($1)`, jobID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var ok bool
	if rows.Next() {
		if err := rows.Scan(&ok); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if !ok {
		return ErrJobNotFound
	}

	return nil
}

// RetryAfterError is a failure that knows when its job should next run, such
// as a rate limit response carrying Retry-After. Fail finds it with
// errors.As, so it applies when wrapped.
type RetryAfterError interface {
	error
	RetryAfter() time.Duration
}

const (
	retryBaseDelay = time.Second
	retryMaxDelay  = time.Hour
)

// Fail records a failed attempt of a claimed job, scheduling a retry or
// moving it to the dead jobs once its attempts are exhausted. The retry waits
// for failure's RetryAfterError delay if it has a positive one, and otherwise
// backs off exponentially with jitter from one second after the first attempt
// up to an hour. It returns ErrClaimLost if the job was reaped or reclaimed
// since job was claimed.
func Fail(ctx context.Context, db DB, job *Job, failure error) error {
	return FailAfter(ctx, db, job, failure, retryDelay(job.Attempts, failure))
}

// FailAfter is Fail with an explicit delay before the retry.
func FailAfter(ctx context.Context, db DB, job *Job, failure error, delay time.Duration) error {
	var info any
	if failure != nil {
		info = map[string]string{"error": failure.Error()}
	}
	infoJSON, err := json.Marshal(info)
	if err != nil {
		return err
	}

	return settleClaim(ctx, db, `SELECT "raptor_fail_job"($1, $2, $3, $4, $5)`, job.ID, job.ClaimedBy, job.Attempts, infoJSON, delay)
}

func retryDelay(attempts int32, failure error) time.Duration {
	if ra, ok := errors.AsType[RetryAfterError](failure); ok && ra.RetryAfter() > 0 {
		return ra.RetryAfter()
	}
	return backoff(attempts)
}

func backoff(attempts int32) time.Duration {
	ceiling := min(retryBaseDelay<<min(max(attempts-1, 0), 30), retryMaxDelay)
	return ceiling/2 + randDuration(ceiling/2)
}

func randDuration(limit time.Duration) time.Duration {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(limit)+1))
	if err != nil {
		return limit
	}
	return time.Duration(n.Int64())
}

// Release returns a claimed job that never started back to pending without
// counting the claim as an attempt. It returns ErrClaimLost if the job was
// reaped or reclaimed since job was claimed.
func Release(ctx context.Context, db DB, job *Job) error {
	return settleClaim(ctx, db, `SELECT "raptor_release_job"($1, $2, $3)`, job.ID, job.ClaimedBy, job.Attempts)
}

func settleClaim(ctx context.Context, db DB, query string, args ...any) error {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var status string
	if rows.Next() {
		if err := rows.Scan(&status); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	switch status {
	case "NOT_FOUND":
		return ErrJobNotFound
	case "CLAIM_LOST":
		return ErrClaimLost
	}

	return nil
}

func Sweep(ctx context.Context, db DB, queue string) error {
	var q any
	if queue != "" {
		q = queue
	}

	rows, err := db.Query(ctx, `SELECT "raptor_reap_stuck_jobs"($1), "raptor_expire_jobs"($1)`, q)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		var reaped, expired int32
		if err := rows.Scan(&reaped, &expired); err != nil {
			return err
		}
	}

	return rows.Err()
}

func Cleanup(ctx context.Context, db DB, queue string) error {
	var q any
	if queue != "" {
		q = queue
	}

	rows, err := db.Query(ctx, `SELECT "raptor_cleanup_jobs"($1), "raptor_rollup_stats"()`, q)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		var deleted, rolledUp int32
		if err := rows.Scan(&deleted, &rolledUp); err != nil {
			return err
		}
	}

	return rows.Err()
}
