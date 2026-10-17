package raptor

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"sync"
	"time"
)

type ConcurrentWeightedRunner struct {
	// Queue maps queue names to how many jobs each takes per round. Run reads
	// Queue, Workers and Threads without locking, so they must not be changed
	// while Run is running.
	Queue   map[string]int
	Workers map[string]Worker
	// Threads is how many jobs Run executes at once. It must be positive;
	// NewConcurrentWeightedRunner sets it to runtime.NumCPU().
	Threads int
	// ShutdownTimeout bounds how long Run waits for in-flight jobs once its
	// context is done. When it elapses, Run cancels their contexts and
	// releases any job whose worker then returns an error, so it goes back to
	// pending without using an attempt. Zero waits for jobs to finish, up to
	// their claim deadline. It must not be negative.
	ShutdownTimeout time.Duration

	emptyCooldown time.Duration
	id            string
	quiet         quietSignal
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

// Quiet stops Run from claiming new jobs. Jobs already claimed, including
// those from a claim in progress when Quiet is called, still run and settle.
// Run keeps blocking until its context is done, so a process can quiet its
// runner ahead of shutdown and cancel the context once in-flight jobs finish.
// Quiet is permanent: a quieted runner never claims again, even from a later
// Run. It is safe to call more than once, before Run starts, and concurrently
// with Run.
func (c *ConcurrentWeightedRunner) Quiet() {
	c.quiet.quiet()
}

func (c *ConcurrentWeightedRunner) Run(parent context.Context, db DB) error {
	if c.Threads <= 0 {
		return fmt.Errorf("raptor: runner threads %d must be positive", c.Threads)
	}
	if err := validateShutdownTimeout(c.ShutdownTimeout); err != nil {
		return err
	}

	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)

	jobCtx, stopJobs := shutdownContext(ctx, c.ShutdownTimeout)
	defer stopJobs()

	queues := weightedQueues(c.Queue)
	idle := newIdleWorkers(c.Threads)
	jobs := make(chan *Job)

	go func() {
		defer close(jobs)
		for ctx.Err() == nil && !c.quiet.quieted() {
			if err := c.dequeue(ctx, db, queues, idle, jobs); err != nil {
				cancel(err)
				return
			}
		}
	}()

	var workers sync.WaitGroup
	for range c.Threads {
		workers.Go(func() {
			for job := range jobs {
				if ctx.Err() != nil {
					c.release(ctx, db, job)
				} else {
					c.execute(jobCtx, db, job)
				}
				idle.put(1)
			}
		})
	}

	<-ctx.Done()
	workers.Wait()

	if err := context.Cause(ctx); err != context.Cause(parent) {
		return err
	}
	if err := parent.Err(); !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

type weightedQueue struct {
	name   string
	weight int
}

func weightedQueues(weights map[string]int) []weightedQueue {
	queues := make([]weightedQueue, 0, len(weights))
	for name, weight := range weights {
		if weight > 0 {
			queues = append(queues, weightedQueue{name: name, weight: weight})
		}
	}
	slices.SortFunc(queues, func(a, b weightedQueue) int {
		return cmp.Or(cmp.Compare(b.weight, a.weight), cmp.Compare(a.name, b.name))
	})
	return queues
}

// dequeue runs one weighted round: each queue is drained up to its weight (or
// until empty) before moving to the next, in slice order. If nothing was
// claimed this round, it waits out emptyCooldown before returning so the
// caller's loop doesn't spin.
func (c *ConcurrentWeightedRunner) dequeue(ctx context.Context, db DB, queues []weightedQueue, idle idleWorkers, jobs chan<- *Job) error {
	var claimedAny bool
	for _, q := range queues {
		n, err := c.drainQueue(ctx, db, q.name, q.weight, idle, jobs)
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
		case <-c.quiet.done():
		case <-time.After(c.emptyCooldown):
		}
	}

	return nil
}

// drainQueue claims up to max jobs from the named queue and hands each
// claimed job to the queue channel. Each batch is sized to the number of
// idle workers, waiting for at least one, so the runner never holds a claimed
// job that no worker is free to start. It stops early if the queue runs dry
// before max is reached.
func (c *ConcurrentWeightedRunner) drainQueue(ctx context.Context, db DB, name string, max int, idle idleWorkers, queue chan<- *Job) (int, error) {
	claimed := 0
	for claimed < max {
		if err := ctx.Err(); err != nil {
			return claimed, err
		}

		limit := idle.take(ctx, c.quiet.done(), max-claimed)
		if limit == 0 {
			return claimed, ctx.Err()
		}
		if c.quiet.quieted() {
			idle.put(limit)
			return claimed, nil
		}

		jobs, err := Claim(ctx, db, name, c.id, limit, 0)
		idle.put(limit - len(jobs))
		if err != nil {
			return claimed, err
		}
		if len(jobs) == 0 {
			return claimed, nil
		}

		for i, job := range jobs {
			select {
			case <-ctx.Done():
				c.release(ctx, db, jobs[i:]...)
				return claimed, ctx.Err()
			case queue <- job:
				claimed++
			}
		}
	}

	return claimed, nil
}

type idleWorkers chan struct{}

func newIdleWorkers(n int) idleWorkers {
	idle := make(idleWorkers, n)
	idle.put(n)
	return idle
}

func (w idleWorkers) take(ctx context.Context, stop <-chan struct{}, max int) int {
	select {
	case <-ctx.Done():
		return 0
	case <-stop:
		return 0
	case <-w:
	}

	n := 1
	for n < max {
		select {
		case <-w:
			n++
		default:
			return n
		}
	}
	return n
}

func (w idleWorkers) put(n int) {
	for range n {
		w <- struct{}{}
	}
}

func (c *ConcurrentWeightedRunner) execute(ctx context.Context, db DB, job *Job) {
	err := executeJob(ctx, db, c.Workers[job.Type], job)
	if err != nil {
		slog.ErrorContext(ctx, "failed to execute job", slog.String("job-id", job.ID), slog.String("job-type", job.Type), slog.String("queue", job.Queue), slog.String("runner-id", c.id), slog.Any("error", err))
	}
}

func (c *ConcurrentWeightedRunner) release(ctx context.Context, db DB, jobs ...*Job) {
	ctx, cancel := settleContext(ctx)
	defer cancel()

	for _, job := range jobs {
		if err := ignoreClaimLost(ctx, job, Release(ctx, db, job)); err != nil {
			slog.ErrorContext(ctx, "failed to release job", slog.String("job-id", job.ID), slog.String("job-type", job.Type), slog.String("queue", job.Queue), slog.String("runner-id", c.id), slog.Any("error", err))
		}
	}
}

const settleTimeout = 10 * time.Second

type quietSignal struct {
	init sync.Once
	stop sync.Once
	ch   chan struct{}
}

func (q *quietSignal) done() <-chan struct{} {
	q.init.Do(func() { q.ch = make(chan struct{}) })
	return q.ch
}

func (q *quietSignal) quiet() {
	q.done()
	q.stop.Do(func() { close(q.ch) })
}

func (q *quietSignal) quieted() bool {
	select {
	case <-q.done():
		return true
	default:
		return false
	}
}

func validateShutdownTimeout(timeout time.Duration) error {
	if timeout < 0 {
		return fmt.Errorf("raptor: shutdown timeout %s must not be negative", timeout)
	}
	return nil
}

func shutdownContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	jobCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	if timeout == 0 {
		return jobCtx, cancel
	}

	go func() {
		select {
		case <-jobCtx.Done():
			return
		case <-ctx.Done():
		}

		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case <-jobCtx.Done():
		case <-timer.C:
			cancel()
		}
	}()

	return jobCtx, cancel
}

func settleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
}

func executeJob(ctx context.Context, db DB, worker Worker, job *Job) error {
	err := runWorker(ctx, worker, job)
	interrupted := ctx.Err() != nil

	ctx, cancel := settleContext(ctx)
	defer cancel()

	switch {
	case err == nil:
		err = Complete(ctx, db, job)
	case interrupted:
		// The error most likely came from shutdown cancelling ctx, so it shouldn't cost an attempt.
		err = Release(ctx, db, job)
	default:
		err = Fail(ctx, db, job, err)
	}
	return ignoreClaimLost(ctx, job, err)
}

func ignoreClaimLost(ctx context.Context, job *Job, err error) error {
	if errors.Is(err, ErrClaimLost) {
		slog.InfoContext(ctx, "job claim lost before it settled", slog.String("job-id", job.ID), slog.String("job-type", job.Type), slog.String("queue", job.Queue))
		return nil
	}
	return err
}

func runWorker(ctx context.Context, worker Worker, job *Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic while executing job: %v", r)
		}
	}()

	if worker == nil {
		return fmt.Errorf("raptor: no worker registered for job type %q", job.Type)
	}

	ctx, cancel := context.WithDeadline(ctx, job.ClaimDeadline())
	defer cancel()

	return worker.Execute(ctx, job)
}
