package raptor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sort"
	"sync/atomic"
	"time"
)

type ConcurrentWeightedRunner struct {
	Queue   map[string]int
	Workers map[string]Worker
	Threads int

	emptyCooldown time.Duration
	id            string
	quiet         atomic.Bool
}

func NewConcurrentWeightedRunner(queue map[string]int, workers map[string]Worker) *ConcurrentWeightedRunner {
	return &ConcurrentWeightedRunner{
		Queue:         queue,
		Workers:       workers,
		Threads:       runtime.NumCPU(),
		emptyCooldown: 1 * time.Second,
		id:            newRunnerID(),
	}
}

func (c *ConcurrentWeightedRunner) Quiet() {
	c.quiet.Store(true)
}

func (c *ConcurrentWeightedRunner) Run(ctx context.Context, db DB) error {
	jobs := make(chan *Job, c.Threads)
	defer close(jobs)

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if c.quiet.Load() {
					return
				}
				if err := c.dequeue(ctx, db, jobs); err != nil {
					cancel(err)
					return
				}
			}
		}
	}()

	for i := 0; i < c.Threads; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case job := <-jobs:
					if job == nil {
						return
					}
					c.execute(ctx, db, job)
				}
			}
		}()
	}

	<-ctx.Done()
	err := ctx.Err()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// dequeue runs one weighted round: each queue in c.Queue is drained up to
// its configured weight (or until empty) before moving to the next queue,
// highest weight first (ties broken by name for determinism). If nothing
// was claimed this round, it waits out emptyCooldown before returning so
// the caller's loop doesn't spin.
func (c *ConcurrentWeightedRunner) dequeue(ctx context.Context, db DB, queue chan<- *Job) error {
	names := make([]string, 0, len(c.Queue))
	for name := range c.Queue {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		wi, wj := c.Queue[names[i]], c.Queue[names[j]]
		if wi != wj {
			return wi > wj
		}
		return names[i] < names[j]
	})

	var claimedAny bool
	for _, name := range names {
		weight := c.Queue[name]
		if weight <= 0 {
			continue
		}

		n, err := c.drainQueue(ctx, db, name, weight, queue)
		if err != nil {
			return err
		}
		if n > 0 {
			claimedAny = true
		}
	}

	if !claimedAny {
		select {
		case <-ctx.Done():
		case <-time.After(c.emptyCooldown):
		}
	}

	return nil
}

// drainQueue claims up to max jobs from the named queue, in batches of
// c.Threads at a time, and hands each claimed job to the queue channel.
// It stops early if the queue runs dry before max is reached.
func (c *ConcurrentWeightedRunner) drainQueue(ctx context.Context, db DB, name string, max int, queue chan<- *Job) (int, error) {
	claimed := 0
	for claimed < max {
		if err := ctx.Err(); err != nil {
			return claimed, err
		}

		limit := c.Threads
		if remaining := max - claimed; remaining < limit {
			limit = remaining
		}

		jobs, err := Claim(ctx, db, name, c.id, limit, 0)
		if err != nil {
			return claimed, err
		}
		if len(jobs) == 0 {
			return claimed, nil
		}

		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return claimed, ctx.Err()
			case queue <- job:
				claimed++
			}
		}
	}

	return claimed, nil
}

func (c *ConcurrentWeightedRunner) execute(ctx context.Context, db DB, job *Job) {
	err := executeJob(ctx, db, c.Workers[job.Type], job)
	if err != nil {
		slog.ErrorContext(ctx, "failed to execute job", slog.String("job-id", job.ID), slog.String("job-type", job.Type), slog.String("queue", job.Queue), slog.String("runner-id", c.id), slog.Any("error", err))
	}
}

func executeJob(ctx context.Context, db DB, worker Worker, job *Job) error {
	defer func() {
		panicErr := recover()
		if panicErr != nil {
			_ = Fail(ctx, db, job, fmt.Errorf("panic while executing job: %v", panicErr))
		}
	}()

	if worker == nil {
		return Fail(ctx, db, job, fmt.Errorf("no worker for job type %q", job.Type))
	}

	jobCtx, cancel := context.WithDeadline(ctx, job.ClaimDeadline())
	defer cancel()

	err := worker.Execute(jobCtx, job)
	if err != nil {
		return Fail(ctx, db, job, err)
	}
	return Complete(ctx, db, job)
}
