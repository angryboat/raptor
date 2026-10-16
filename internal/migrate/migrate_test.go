package migrate

import (
	"testing"

	"github.com/angryboat/raptor/internal/test"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUp(t *testing.T) {
	conn := test.CreateDB(t)

	err := Up(t.Context(), conn)
	assert.NoError(t, err)
}

func TestUp_DeadJobsPrimaryKeyKeepsLatestDuplicate(t *testing.T) {
	conn := test.CreateDB(t)
	require.NoError(t, Up(t.Context(), conn))

	_, err := conn.Exec(t.Context(), `
		ALTER TABLE "raptor_dead_jobs" DROP CONSTRAINT "raptor_dead_jobs_pkey";
		DROP INDEX "idx_raptor_dead_jobs_failed_at";
		DELETE FROM "raptor_migrations" WHERE "key" = '000008_dead_jobs_index_retention';
		INSERT INTO "raptor_dead_jobs" ("id", "queue_name", "job_type", "status", "failed_at", "failure_info") VALUES
			('00000000-0000-0000-0000-000000000001', 'q', 't', 'failed', now() - INTERVAL '2 days', '{"error":"older"}'),
			('00000000-0000-0000-0000-000000000001', 'q', 't', 'failed', now() - INTERVAL '1 day', '{"error":"newer"}'),
			('00000000-0000-0000-0000-000000000002', 'q', 't', 'failed', now(), '{"error":"single"}');
	`)
	require.NoError(t, err)

	require.NoError(t, Up(t.Context(), conn))

	rows, err := conn.Query(t.Context(), `SELECT "failure_info"->>'error' FROM "raptor_dead_jobs" ORDER BY "id"`)
	require.NoError(t, err)
	reasons, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	assert.Equal(t, []string{"newer", "single"}, reasons)
}
