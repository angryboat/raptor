package raptor

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/maddiesch/raptor/internal/migrate"
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
	ID             string
	Queue          string
	Type           string
	Payload        json.RawMessage
	Priority       int16
	Status         string
	CreatedAt      time.Time
	VisibleAt      time.Time
	ExpiresAt      *time.Time
	ClaimedAt      *time.Time
	ClaimedBy      *string
	ClaimedTTL     int32
	CompletedAt    *time.Time
	CancelledAt    *time.Time
	FailedAt       *time.Time
	Attempts       int32
	MaxAttempts    int32
	TimeoutMs      int32
	FailureInfo    json.RawMessage
	IdempotencyKey *string
}

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

func Enqueue(ctx context.Context, db DB, job EnqueueJob) (string, error) {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return "", err
	}

	var visibleAt any
	if !job.VisibleAt.IsZero() {
		visibleAt = job.VisibleAt
	}

	var expiresAt any
	if !job.ExpiresAt.IsZero() {
		expiresAt = job.ExpiresAt
	}

	maxAttempts := job.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 10
	}

	var idempotencyKey any
	if job.IdempotencyKey != "" {
		idempotencyKey = job.IdempotencyKey
	}

	rows, err := db.Query(ctx, `SELECT "raptor_enqueue_job"($1, $2, $3, $4, $5, $6, $7, $8)`,
		job.Queue,
		job.Type,
		payload,
		job.Priority,
		visibleAt,
		expiresAt,
		maxAttempts,
		idempotencyKey,
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

func Claim(ctx context.Context, db DB, queue, worker string, limit int, timeout time.Duration) ([]*Job, error) {
	if limit <= 0 {
		limit = 1
	}

	var claimedTTL any
	if timeout > 0 {
		claimedTTL = int32(timeout.Seconds())
	}

	rows, err := db.Query(ctx, `SELECT * FROM "raptor_claim_jobs"($1, $2, $3, $4)`,
		queue,
		worker,
		limit,
		claimedTTL,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(
			&job.ID,
			&job.Queue,
			&job.Type,
			&job.Payload,
			&job.Priority,
			&job.Status,
			&job.CreatedAt,
			&job.VisibleAt,
			&job.ExpiresAt,
			&job.ClaimedAt,
			&job.ClaimedBy,
			&job.ClaimedTTL,
			&job.CompletedAt,
			&job.CancelledAt,
			&job.FailedAt,
			&job.Attempts,
			&job.MaxAttempts,
			&job.TimeoutMs,
			&job.FailureInfo,
			&job.IdempotencyKey,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, &job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}

var ErrJobNotFound = errors.New("raptor: job not found")

func Complete(ctx context.Context, db DB, jobID string) error {
	rows, err := db.Query(ctx, `SELECT "raptor_complete_job"($1)`, jobID)
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

func Fail(ctx context.Context, db DB, jobID string, failure error) error {
	var (
		rows pgx.Rows
		err  error
	)

	if failure == nil {
		rows, err = db.Query(ctx, `SELECT "raptor_fail_job"($1)`, jobID)
	} else {
		info, marshalErr := json.Marshal(map[string]string{"error": failure.Error()})
		if marshalErr != nil {
			return marshalErr
		}
		rows, err = db.Query(ctx, `SELECT "raptor_fail_job"($1, $2)`, jobID, info)
	}
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

	if status == "NOT_FOUND" {
		return ErrJobNotFound
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

	rows, err := db.Query(ctx, `SELECT "raptor_cleanup_jobs"($1)`, q)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		var deleted int32
		if err := rows.Scan(&deleted); err != nil {
			return err
		}
	}

	return rows.Err()
}
