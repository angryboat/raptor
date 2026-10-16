# AGENTS.md

Raptor is a Go library (`github.com/angryboat/raptor`, one package `raptor`)
for a Postgres-backed job queue. It has no binary. README.md covers the public
API; this file covers how to change it safely.

## Commands

- `docker compose up -d`: starts Postgres 18 on `localhost:5432`
  (`postgres`/`password`). If the port is taken, an existing local Postgres
  with the same credentials works.
- `make test`: runs `go test -v -race -count 1 -timeout 30s ./...`. Narrow it
  with `make test GO_TEST_RUN=TestScheduler`.
- Run `go vet ./...` and `gofmt -l .` before committing.

## Layout

- `raptor.go`: core API (`Setup`, `Enqueue`, `Claim`, `Complete`, `Fail`,
  `Release`, `Cancel`, `Sweep`, `Cleanup`). These are thin wrappers that call
  SQL functions.
- `worker.go`, `serial_runner.go`, `concurrent_weighted_runner.go`: running
  jobs.
- `scheduler.go`: recurring jobs.
- `queue_stats.go`: per-type counters.
- `internal/migrate`: embedded SQL migrations. `Setup` applies them under an
  advisory lock.
- `internal/test`: per-test database helpers.

## Database rules

- Job state changes live in plpgsql functions (`raptor_*`) in the migrations,
  and Go calls them with `SELECT`. Put new state logic there unless it needs Go,
  as cron parsing in the scheduler does.
- Migrations are append-only. Never edit an existing file. Add the next
  `NNNNNN_description.sql` instead, with the name minus `.sql` at most 32
  characters. Each file runs in one transaction. To change a function, use
  `CREATE OR REPLACE` in a new migration.
- Prefix every table, type and function with `raptor_`, and double-quote
  identifiers.
- Correctness under concurrency depends on row locks (`FOR UPDATE SKIP LOCKED`)
  and on fencing: `Complete`, `Fail` and `Release` only apply when
  `claimed_by` and `attempts` match the caller's claim. Keep both. A settle
  that misses must report `CLAIM_LOST` or `NOT_FOUND`.
- For any time that more than one process compares, use the database clock
  (`now()`), not Go's.
- Accept the narrowest interface: `DB` (only `Query`) or `TxDB` (adds
  `Begin`). `*pgxpool.Pool`, `*pgx.Conn` and `pgx.Tx` satisfy both, so callers
  can pass a transaction.

## Tests

- Tests use testify (`require` for preconditions, `assert` for checks) in
  package `raptor`. Godoc examples go in package `raptor_test`.
- `test.CreateDB` and `test.CreatePool` drop and recreate a database for each
  test, which is slow. Group subtests under one database and isolate each with
  `beginTx(t, conn)`, which rolls back on cleanup.
- Inside `beginTx`, `now()` stays at the time the transaction started. That
  makes exact time assertions easy, but code that waits for time to pass needs
  a pool.
- Anything concurrent (runners, competing schedulers) needs `CreatePool`,
  because a `*pgx.Conn` allows only one query at a time.
- Concurrency tests must fail when the guard they cover is removed. Check this
  by deleting the lock or fence and running them.
- Reuse the helpers in `runner_helpers_test.go` and `scheduler_test.go`.

## Style

- Write no comments except doc comments on exported identifiers and the rare
  one-line note explaining why.
- Prefix errors that raptor creates with `raptor:`. The sentinel errors are
  `ErrJobNotFound` and `ErrClaimLost`.
- Settle jobs after cancellation with `settleContext`, so a result isn't lost
  during shutdown.
- Commits use conventional types with an emoji prefix (`✨ feat:`, `🐛 fix:`,
  `✅ test:`) and an imperative, lowercase subject.
- Update README.md when public behavior changes.
