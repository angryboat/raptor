# Raptor

A Postgres-backed job queue for Go. Jobs are plain rows, so you can enqueue
them in the same transaction as the data they act on.

Requires Go 1.26+ and PostgreSQL 18+.

```sh
go get github.com/angryboat/raptor
```

## Usage

Create the tables once at boot. Running it again or from several processes at
once is safe.

```go
pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
if err != nil {
	return err
}
if err := raptor.SetupPool(ctx, pool); err != nil {
	return err
}
```

Enqueue a job. Pass a `pgx.Tx` instead of the pool to enqueue it in a
transaction.

```go
_, err := raptor.Enqueue(ctx, pool, raptor.EnqueueJob{
	Queue:   "default",
	Type:    "email.welcome",
	Payload: Welcome{UserID: 42},
})
```

Run workers. `ValueWorkerFunc` decodes the JSON payload for you.

```go
runner := raptor.NewConcurrentWeightedRunner(
	map[string]int{"critical": 10, "default": 1},
	map[string]raptor.Worker{
		"email.welcome": raptor.ValueWorkerFunc(func(ctx context.Context, job *raptor.Job, w Welcome) error {
			return sendWelcome(ctx, w.UserID)
		}),
	},
)
return runner.Run(ctx, pool)
```

`Run` blocks until `ctx` is cancelled, then finishes the jobs it's already
running. Each round it takes up to a queue's weight in jobs, starting with the
highest-weight queue. It claims a job only when one of its `Threads` workers
is free to start it, so a claim's deadline never runs out while the job waits
in line. `Threads` defaults to the number of CPUs, and `Run` returns an error
if it isn't positive. `SerialRunner` empties its queues one job at a time and
then returns, which is handy for tests and scripts.

## Jobs

- **Retries**: a worker that returns an error or panics fails the job. It
  retries until it has used `MaxAttempts` (10 by default), then moves to
  `raptor_dead_jobs`. Each retry backs off exponentially with jitter, starting
  around a second and capped at an hour, so the default 10 attempts span about
  4 to 9 minutes. An error that implements `RetryAfterError`, even wrapped,
  picks its own delay instead, which suits rate limits that say when to come
  back. Callers that settle jobs themselves get the same from `Fail`, or pass
  an exact delay to `FailAfter`.
- **Delays and expiry**: `VisibleAt` holds a job back until that time.
  `ExpiresAt` cancels it if it hasn't run by then.
- **Deduplication**: while a job with the same `IdempotencyKey`, queue and type
  is pending or claimed, enqueueing another returns the existing job's ID.
- **Claims**: a worker's context has a deadline at the end of its claim (5
  minutes by default). After that, `Sweep` may give the job to another worker.
  The original worker's `Complete` or `Fail` then returns `ErrClaimLost`.

## Scheduled jobs

Declare recurring jobs in code and run a `Scheduler` on every replica. Each
tick is enqueued exactly once across all of them. A tick missed during downtime
runs once, not once for every missed tick.

```go
scheduler, err := raptor.NewScheduler(
	raptor.Schedule{Name: "sweep", Spec: "@every 30s", Job: raptor.EnqueueJob{Queue: "maintenance", Type: "maintenance.sweep"}},
	raptor.Schedule{Name: "report", Spec: "CRON_TZ=America/Denver 0 9 * * 1-5", Job: raptor.EnqueueJob{Queue: "default", Type: "report.daily"}},
)
if err != nil {
	return err
}
return scheduler.Run(ctx, pool)
```

Specs are cron expressions, descriptors like `@daily`, or `@every <duration>`
with a whole number of seconds, in UTC unless prefixed with `CRON_TZ=`. `Run` wakes when a schedule is due and
otherwise checks every `Interval` (one minute by default).

A stored schedule is identified by its name and spec, so changing either makes
a new schedule. During a rolling deploy, old and new replicas each run their
own version of a changed schedule, so it may fire on both cadences until the
old replicas stop. Once a schedule is an hour overdue, no replica is running
it, and the next boot of any replica deletes it.

## Maintenance

Call these on a timer, or from a scheduled job:

- `Sweep` retries jobs whose claim has expired and cancels jobs whose
  `ExpiresAt` has passed.
- `Cleanup` deletes completed jobs after 24 hours and cancelled jobs after 2
  hours.

`LoadQueueStat` reports completed, failed, cancelled and dead counts for each
job type in a queue.

## Dashboard

`dashboard.New` returns an `http.Handler` that renders a read-only page of
queue depths and lifetime counts, running, retrying and dead jobs, and each
schedule's next run. Turn on its Live toggle to refresh every 5 seconds. It
does no authentication, so wrap it in your own middleware.

```go
mux.Handle("/raptor/", http.StripPrefix("/raptor", requireAdmin(password, dashboard.New(pool))))
```

## Development

```sh
docker compose up -d
make test
```

Tests expect Postgres at `localhost:5432` with user `postgres` and password
`password`.
