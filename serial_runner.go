package raptor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
)

type SerialRunner struct {
	Queue   []string
	Workers map[string]Worker

	id string
}

func NewSerialRunner(queue []string, workers map[string]Worker) *SerialRunner {
	return &SerialRunner{
		Queue:   queue,
		Workers: workers,
		id:      newRunnerID(),
	}
}

// Execute drains each queue in Queue, in slice order: a queue is claimed
// from repeatedly until empty before the next queue in the slice starts.
func (r *SerialRunner) Execute(ctx context.Context, db DB) error {
	for _, queue := range r.Queue {
		if err := r.drainQueue(ctx, db, queue); err != nil {
			return err
		}
	}

	return nil
}

func (r *SerialRunner) drainQueue(ctx context.Context, db DB, queue string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		jobs, err := Claim(ctx, db, queue, r.id, 1, 0)
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			return nil
		}

		r.runJob(ctx, db, jobs[0])
	}
}

func (r *SerialRunner) runJob(ctx context.Context, db DB, job *Job) {
	worker, ok := r.Workers[job.Type]
	if !ok {
		_ = Fail(ctx, db, job, fmt.Errorf("raptor: no worker registered for job type %q", job.Type))
		return
	}

	if err := executeJob(ctx, db, worker, job); err != nil {
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
