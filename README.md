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
transaction. `Queue` and `Type` must be 1-64 characters and
`IdempotencyKey` at most 128; `Enqueue` returns an error otherwise.

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
running and returns nil, as `SerialRunner.Execute` and `Scheduler.Run` do. A
`ctx` that passes its deadline, or a failed claim, returns an error. Each
round it takes up to a queue's weight in jobs, starting with the
highest-weight queue. It claims a job only when one of its `Threads` workers
is free to start it, so a claim's deadline never runs out while the job waits
in line. `Threads` defaults to the number of CPUs, and `Run` returns an error
if it isn't positive. Don't change `Queue`, `Workers` or `Threads` while `Run`
is running; it reads them without locking. `SerialRunner` empties its queues
one job at a time and then returns, which is handy for tests and scripts.

By default, shutdown waits as long as the slowest running job, up to its
claim deadline. If the process is killed first, say by a Kubernetes
termination grace period, the job stays claimed until `Sweep` reaps it, and
that costs an attempt. Set `ShutdownTimeout` on either runner to bound the
wait: once `ctx` is done and the timeout passes, the runner cancels the
contexts of the jobs still running. A job that then returns an error is
released back to pending without using an attempt; one that returns nil is
completed. The trade-off is that interrupted jobs run again from the start, so
they should be idempotent, and workers must return when their context ends,
since the runner still waits for them. Leave room within the grace period for
the runner to settle jobs after the timeout.

Each job's context ends with its claim, 5 minutes by default, and a job still
running then can be reaped by `Sweep` and run again elsewhere. Set
`ClaimTimeout` on either runner to claim for longer than the slowest job, or
`ClaimTTL` on `EnqueueJob` to give one slow job type a longer claim without
raising it for the whole runner. Either also caps how long a default shutdown
waits. `ClaimTimeout` wins over `ClaimTTL`, and the TTL is stored on the job,
so a runner without `ClaimTimeout` that later claims it, say for a retry,
keeps the last one set rather than the enqueued `ClaimTTL`.

Call `Quiet` to stop a runner claiming new jobs without stopping the ones it
has, say from a pre-stop hook while the load balancer drains. Jobs already
claimed run and settle as usual, including one last batch from a claim that
races `Quiet`. `Run` keeps blocking until `ctx` is
cancelled, so cancel it once in-flight jobs are done; `SerialRunner.Execute`
returns nil once its running job settles. Quiet is permanent for that runner,
and it's safe to call more than once, before `Run` starts, or while it shuts
down.

## Jobs

- **Retries**: a worker that returns an error or panics fails the job, as does
  a job whose type has no registered worker. It retries until it has used
  `MaxAttempts` (10 by default), then moves to `raptor_dead_jobs`. Each retry
  backs off exponentially with jitter, starting around a second and capped at
  an hour, so the default 10 attempts span about 4 to 9 minutes. An error that
  implements `RetryAfterError`, even wrapped, picks its own delay instead,
  which suits rate limits that say when to come back. A worker that knows
  retrying won't help, like a bad payload, returns `raptor.Permanent(err)`, and
  the job moves to `raptor_dead_jobs` on that attempt, even when wrapped.
  `ValueWorkerFunc` does this for payloads it can't decode. Callers that settle
  jobs themselves get the same from `Fail`, or pass an exact delay to
  `FailAfter`.
- **Delays and expiry**: `VisibleAt` holds a job back until that time.
  `ExpiresAt` cancels it if it hasn't run by then.
- **Deduplication**: while a job with the same `IdempotencyKey`, queue and type
  is pending or claimed, enqueueing another returns the existing job's ID. If
  that job finishes mid-enqueue, the key is free and a new job is enqueued.
- **Claims**: a worker's context has a deadline at the end of its claim (5
  minutes by default, the job's `ClaimTTL`, or the runner's `ClaimTimeout`). After that, `Sweep` may give the job to another worker.
  The original worker's `Complete` or `Fail` then returns `ErrClaimLost`.
  `Complete`, `Fail` and `Release` return `ErrClaimLost` whenever the job
  still exists but the caller's claim no longer holds: it was reaped,
  reclaimed, cancelled, already settled or moved to `raptor_dead_jobs`. They
  return `ErrJobNotFound` only when raptor has no record of the job, because
  it never existed or `Cleanup` deleted it. Every claim draws a new
  `ClaimSeq` from one database-wide sequence, so a `Job` from an earlier
  claim loses even when the same worker reclaims the job after `Release`. Runners log a lost claim at info
  level and move on, so cancelling a running job doesn't surface as an error.
  Claim TTLs are whole seconds, so `Claim` rounds its timeout up, and
  `ClaimTTL` the same way: 500ms claims for one second and 1.5s for two. A
  negative `ClaimTTL` is an error.

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
otherwise checks every `Interval` (one minute by default). A replica that loses
a tick to another checks again a moment later, so it takes over if the winner
stops.

A stored schedule is identified by its name and spec, so changing either makes
a new schedule. During a rolling deploy, old and new replicas each run their
own version of a changed schedule, so it may fire on both cadences until the
old replicas stop. Once a schedule is an hour overdue, no replica is running
it, and the next boot of any replica deletes it. A running replica that still
defines a deleted schedule stores it again, first due at its next tick.

## Maintenance

Call these on a timer, or from a scheduled job:

- `Sweep` retries jobs whose claim has expired and cancels jobs whose
  `ExpiresAt` has passed.
- `Cleanup` deletes completed jobs after 24 hours, cancelled jobs after 2
  hours and dead jobs after 30 days, and rolls up stats. Per-type dead counts
  in `LoadQueueStat` are kept. It deletes at most 10,000 rows per statement
  and repeats until none are left, so given a pool, a backlog commits in
  small transactions rather than one large one. Given a transaction, every
  batch shares it.

`LoadQueueStat` reports completed, failed, cancelled and dead counts for each
job type in a queue. Each state change records its counts as a new row rather
than updating shared counters, so a transaction left open after `Enqueue` or
`Complete` doesn't block other workers. `Cleanup` folds those rows into the
totals. Counts are exact either way, but reading them gets slower the longer
`Cleanup` goes without running.

## Dashboard

`dashboard.New` returns an `http.Handler` that renders a read-only page of
queue depths and lifetime counts, running, retrying and dead jobs, and each
schedule's next run. Its throughput counts for today, this week (starting
Monday) and this month use UTC days, whatever time zone the database or each
connection is set to. Turn on its Live toggle to refresh every 5 seconds. It
does no authentication, so wrap it in your own middleware.

```go
mux.Handle("/raptor/", http.StripPrefix("/raptor", requireAdmin(password, dashboard.New(pool))))
```

Each render reuses data loaded within the last `CacheTTL` (default
`dashboard.DefaultCacheTTL`, 2 seconds), and requests that arrive during a
load wait for it, so any number of open dashboards cost the database at most
one set of queries per TTL. Set `CacheTTL` on the handler before serving to
change it; zero or less reloads for every request, sharing only a load in flight.
A load that takes longer than 30 seconds is abandoned, so a hung query fails
the requests waiting on it instead of stalling the dashboard.

Queue depths come from an index-only scan of `idx_raptor_jobs_queue_depth`, a
partial index over pending and claimed jobs, and the retrying list reads the
newest failures from `idx_raptor_jobs_retrying`, a partial index over pending
jobs that have failed. `Setup` builds each inside a transaction, which blocks
writes to `raptor_jobs` while it runs. On a large table, build them first
without blocking and `Setup` will skip them. If a build fails, drop the
invalid index it leaves before running `Setup`.

```sql
CREATE INDEX CONCURRENTLY "idx_raptor_jobs_queue_depth" ON "raptor_jobs"
  ("queue_name", "status", "visible_at", "failed_at", "expires_at")
  WHERE "status" IN ('pending', 'claimed');

CREATE INDEX CONCURRENTLY "idx_raptor_jobs_retrying" ON "raptor_jobs"
  ("failed_at" DESC)
  WHERE "status" = 'pending' AND "failed_at" IS NOT NULL;
```

## Development

```sh
docker compose up -d
make test
```

Tests expect Postgres at `localhost:5432` with user `postgres` and password
`password`.
