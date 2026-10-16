package dashboard

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/angryboat/raptor"
	"github.com/angryboat/raptor/internal/test"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	conn := test.CreateDB(t)
	require.NoError(t, raptor.Setup(t.Context(), conn))

	t.Run("Empty", func(t *testing.T) {
		db := beginTx(t, conn)

		snap, err := load(t.Context(), db)
		require.NoError(t, err)

		assert.False(t, snap.Now.IsZero())
		require.Len(t, snap.Totals, 4)
		assert.Equal(t, totals{Label: "Today"}, snap.Totals[0])
		assert.Equal(t, totals{Label: "All time"}, snap.Totals[3])
		assert.Empty(t, snap.Queues)
		assert.Empty(t, snap.JobTypes)
		assert.Empty(t, snap.Schedules)
		assert.Empty(t, snap.Running)
		assert.Empty(t, snap.Retrying)
		assert.Empty(t, snap.Dead)
	})

	t.Run("Populated", func(t *testing.T) {
		db := beginTx(t, conn)
		seed(t, db)

		snap, err := load(t.Context(), db)
		require.NoError(t, err)

		want := totals{Enqueued: 9, Completed: 1, Failed: 1, Cancelled: 1, Dead: 1}
		for _, got := range snap.Totals {
			want.Label = got.Label
			assert.Equal(t, want, got)
		}

		assert.Equal(t, []queue{
			{Name: "cleanup", Cancelled: 1},
			{Name: "mail", Ready: 2, Delayed: 1, Retrying: 1, Running: 1, Expired: 1, Completed: 1, Failed: 1, Dead: 1},
		}, snap.Queues)

		assert.Equal(t, []jobType{
			{Queue: "cleanup", Type: "cleanup.run", Cancelled: 1},
			{Queue: "mail", Type: "mail.send", Completed: 1, Failed: 1, Dead: 1},
		}, snap.JobTypes)

		require.Len(t, snap.Schedules, 1)
		assert.Equal(t, "nightly", snap.Schedules[0].Name)
		assert.Equal(t, "@every 1h", snap.Schedules[0].Spec)
		assert.True(t, snap.Schedules[0].NextRunAt.After(snap.Now))
		assert.Nil(t, snap.Schedules[0].LastRunAt)
		assert.False(t, snap.Schedules[0].Due)
		assert.False(t, snap.Schedules[0].Abandoned)

		require.Len(t, snap.Running, 1)
		assert.Equal(t, "worker-1", snap.Running[0].Worker)
		assert.Equal(t, int32(1), snap.Running[0].Attempts)
		assert.Equal(t, snap.Now.Add(time.Minute), snap.Running[0].Deadline)
		assert.False(t, snap.Running[0].Expired)

		require.Len(t, snap.Retrying, 1)
		assert.Equal(t, "smtp down", snap.Retrying[0].Error)
		assert.Equal(t, int32(3), snap.Retrying[0].MaxAttempts)

		require.Len(t, snap.Dead, 1)
		assert.Equal(t, "bounced", snap.Dead[0].Error)
	})

	t.Run("Overdue", func(t *testing.T) {
		db := beginTx(t, conn)
		seed(t, db)

		exec(t, db, `UPDATE "raptor_schedules" SET "next_run_at" = now() - interval '2 hours'`)
		exec(t, db, `UPDATE "raptor_jobs" SET "claimed_at" = now() - interval '2 minutes' WHERE "status" = 'claimed'`)

		snap, err := load(t.Context(), db)
		require.NoError(t, err)

		require.Len(t, snap.Schedules, 1)
		assert.True(t, snap.Schedules[0].Due)
		assert.True(t, snap.Schedules[0].Abandoned)

		require.Len(t, snap.Running, 1)
		assert.True(t, snap.Running[0].Expired)
	})

	t.Run("Due", func(t *testing.T) {
		db := beginTx(t, conn)
		seed(t, db)

		exec(t, db, `UPDATE "raptor_schedules" SET "next_run_at" = now()`)

		snap, err := load(t.Context(), db)
		require.NoError(t, err)

		require.Len(t, snap.Schedules, 1)
		assert.True(t, snap.Schedules[0].Due)
		assert.False(t, snap.Schedules[0].Abandoned)
	})

	t.Run("CapsLists", func(t *testing.T) {
		db := beginTx(t, conn)

		for range listLimit + 1 {
			_, err := raptor.Enqueue(t.Context(), db, raptor.EnqueueJob{Queue: "bulk", Type: "bulk.job"})
			require.NoError(t, err)
		}
		_, err := raptor.Claim(t.Context(), db, "bulk", "worker-1", listLimit+1, time.Minute)
		require.NoError(t, err)

		snap, err := load(t.Context(), db)
		require.NoError(t, err)

		assert.Len(t, snap.Running, listLimit)
		require.Len(t, snap.Queues, 1)
		assert.Equal(t, int64(listLimit+1), snap.Queues[0].Running)
	})
}

func TestHandler(t *testing.T) {
	conn := test.CreateDB(t)
	require.NoError(t, raptor.Setup(t.Context(), conn))

	t.Run("RejectsOtherMethods", func(t *testing.T) {
		rec := serve(t, New(beginTx(t, conn)), http.MethodPost)

		assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		assert.Equal(t, "GET, HEAD", rec.Header().Get("Allow"))
	})

	t.Run("Empty", func(t *testing.T) {
		rec := serve(t, New(beginTx(t, conn)), http.MethodGet)

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "text/html; charset=utf-8", rec.Header().Get("Content-Type"))
		assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
		assert.Contains(t, rec.Body.String(), "No queues yet.")
		assert.Contains(t, rec.Body.String(), `id="live"`)
	})

	t.Run("Populated", func(t *testing.T) {
		db := beginTx(t, conn)
		seed(t, db)

		rec := serve(t, New(db), http.MethodGet)

		require.Equal(t, http.StatusOK, rec.Code)
		body := rec.Body.String()
		for _, s := range []string{"mail", "cleanup", "mail.send", "worker-1", "smtp down", "bounced", "nightly", "@every 1h"} {
			assert.Contains(t, body, s)
		}
	})

	t.Run("EscapesJobData", func(t *testing.T) {
		db := beginTx(t, conn)

		_, err := raptor.Enqueue(t.Context(), db, raptor.EnqueueJob{Queue: "<b>q</b>", Type: "t", MaxAttempts: 1})
		require.NoError(t, err)
		jobs, err := raptor.Claim(t.Context(), db, "<b>q</b>", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.NoError(t, raptor.Fail(t.Context(), db, jobs[0], errors.New(`<script>alert("x")</script>`)))

		rec := serve(t, New(db), http.MethodGet)

		require.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(t, rec.Body.String(), "<b>q</b>")
		assert.NotContains(t, rec.Body.String(), `<script>alert`)
		assert.Contains(t, rec.Body.String(), "&lt;script&gt;")
	})

	t.Run("DatabaseError", func(t *testing.T) {
		rec := serve(t, New(failingDB{errors.New("connection refused")}), http.MethodGet)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Contains(t, rec.Body.String(), "raptor: dashboard: reading clock: connection refused")
	})
}

func TestFormatRelative(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	for want, d := range map[string]time.Duration{
		"now":         400 * time.Millisecond,
		"in 12s":      12 * time.Second,
		"in 4m 12s":   4*time.Minute + 12*time.Second,
		"in 2h":       2 * time.Hour,
		"in 3d 4h":    3*24*time.Hour + 4*time.Hour + 5*time.Minute,
		"5s ago":      -5 * time.Second,
		"1h 30m ago":  -90 * time.Minute,
		"in 1m":       59*time.Second + 600*time.Millisecond,
		"10d 23h ago": -(10*24*time.Hour + 23*time.Hour),
	} {
		assert.Equal(t, want, formatRelative(now, now.Add(d)), "%v", d)
	}
}

func TestFormatDuration(t *testing.T) {
	assert.Equal(t, "0ms", formatDuration(0))
	assert.Equal(t, "250ms", formatDuration(250*time.Millisecond))
	assert.Equal(t, "2s", formatDuration(1500*time.Millisecond))
	assert.Equal(t, "1m 5s", formatDuration(65*time.Second))
}

func TestFormatCount(t *testing.T) {
	for want, n := range map[string]int64{
		"0":         0,
		"999":       999,
		"1,000":     1000,
		"12,345":    12345,
		"1,234,567": 1234567,
		"-1,000":    -1000,
	} {
		assert.Equal(t, want, formatCount(n))
	}
}

// Claim takes the oldest pending job, so seed settles each job before enqueueing the next.
func seed(t *testing.T, db pgx.Tx) {
	t.Helper()

	claimOne := func() *raptor.Job {
		t.Helper()
		jobs, err := raptor.Claim(t.Context(), db, "mail", "worker-1", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		return jobs[0]
	}
	enqueue := func(job raptor.EnqueueJob) string {
		t.Helper()
		id, err := raptor.Enqueue(t.Context(), db, job)
		require.NoError(t, err)
		return id
	}
	mail := raptor.EnqueueJob{Queue: "mail", Type: "mail.send"}

	enqueue(mail)
	require.NoError(t, raptor.Complete(t.Context(), db, claimOne()))

	dead := mail
	dead.MaxAttempts = 1
	enqueue(dead)
	require.NoError(t, raptor.Fail(t.Context(), db, claimOne(), errors.New("bounced")))

	retry := mail
	retry.MaxAttempts = 3
	enqueue(retry)
	require.NoError(t, raptor.Fail(t.Context(), db, claimOne(), errors.New("smtp down")))

	running := mail
	running.Priority = 1
	enqueue(running)
	claimOne()

	enqueue(mail)
	enqueue(mail)
	delayed := mail
	delayed.VisibleAt = time.Now().Add(time.Hour)
	enqueue(delayed)
	expired := delayed
	expired.ExpiresAt = time.Now().Add(-time.Hour)
	enqueue(expired)

	require.NoError(t, raptor.Cancel(t.Context(), db, enqueue(raptor.EnqueueJob{Queue: "cleanup", Type: "cleanup.run"})))

	scheduler, err := raptor.NewScheduler(raptor.Schedule{
		Name: "nightly",
		Spec: "@every 1h",
		Job:  raptor.EnqueueJob{Queue: "cleanup", Type: "cleanup.run"},
	})
	require.NoError(t, err)
	require.NoError(t, scheduler.Sync(t.Context(), db))
}

func serve(t *testing.T, h http.Handler, method string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), method, "/", nil))
	return rec
}

func beginTx(t *testing.T, conn *pgx.Conn) pgx.Tx {
	t.Helper()

	tx, err := conn.Begin(t.Context())
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = tx.Rollback(context.Background())
	})

	return tx
}

func exec(t *testing.T, db pgx.Tx, sql string) {
	t.Helper()

	_, err := db.Exec(t.Context(), sql)
	require.NoError(t, err)
}

type failingDB struct {
	err error
}

func (d failingDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, d.err
}
