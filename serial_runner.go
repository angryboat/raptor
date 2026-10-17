package raptor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"time"
)

type SerialRunner struct {
	Queue   []string
	Workers map[string]Worker
	// ShutdownTimeout bounds how long Execute lets the running job continue
	// once its context is done. It behaves as it does on
	// ConcurrentWeightedRunner.
	ShutdownTimeout time.Duration
	// ClaimTimeout is how long each claim lasts, and so the deadline on each
	// job's context. It behaves as it does on ConcurrentWeightedRunner.
	ClaimTimeout time.Duration

	id    string
	quiet quietSignal
}

func NewSerialRunner(queue []string, workers map[string]Worker) *SerialRunner {
	return &SerialRunner{
		Queue:   queue,
		Workers: workers,
		id:      newRunnerID(),
	}
}

// Quiet stops Execute from claiming new jobs. The job already running
// finishes and settles, then Execute returns nil, as if its queues were empty.
// Quiet doesn't wait for Execute, so a claim racing Quiet may still take one
// more job, which runs and settles like any other. Quiet is permanent: a quieted runner never claims again, even from a later
// Execute. It is safe to call more than once, before Execute starts, and
// concurrently with Execute.
func (r *SerialRunner) Quiet() {
	r.quiet.quiet()
}

// Execute drains each queue in Queue, in slice order: a queue is claimed
// from repeatedly until empty before the next queue in the slice starts.
func (r *SerialRunner) Execute(ctx context.Context, db DB) error {
	if err := validateShutdownTimeout(r.ShutdownTimeout); err != nil {
		return err
	}
	if err := validateClaimTimeout(r.ClaimTimeout); err != nil {
		return err
	}

	jobCtx, stopJobs := shutdownContext(ctx, r.ShutdownTimeout)
	defer stopJobs()

	for _, queue := range r.Queue {
		if err := r.drainQueue(ctx, jobCtx, db, queue); err != nil {
			return err
		}
	}

	return nil
}

func (r *SerialRunner) drainQueue(ctx, jobCtx context.Context, db DB, queue string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.quiet.quieted() {
			return nil
		}

		jobs, err := Claim(ctx, db, queue, r.id, 1, r.ClaimTimeout)
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			return nil
		}

		r.runJob(jobCtx, db, jobs[0])
	}
}

func (r *SerialRunner) runJob(ctx context.Context, db DB, job *Job) {
	if err := executeJob(ctx, db, r.Workers[job.Type], job); err != nil {
		slog.ErrorContext(ctx, "failed to execute job", slog.String("job-id", job.ID), slog.String("job-type", job.Type), slog.String("queue", job.Queue), slog.String("runner-id", r.id), slog.Any("error", err))
	}
}

func newRunnerID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "runner"
	}
	return "runner-" + hex.EncodeToString(b)
}
