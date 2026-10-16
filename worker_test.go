package raptor

import (
	"context"
	"testing"

	"github.com/angryboat/raptor/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValueWorkerFunc(t *testing.T) {
	conn := test.CreateDB(t)
	require.NoError(t, Setup(t.Context(), conn))

	t.Run("UnmarshalFailureIsPermanent", func(t *testing.T) {
		db := beginTx(t, conn)

		id, err := Enqueue(t.Context(), db, EnqueueJob{Queue: "default", Type: "test.job", Payload: "not an object", MaxAttempts: 10})
		require.NoError(t, err)

		var called bool
		worker := ValueWorkerFunc(func(context.Context, *Job, struct{ Name string }) error {
			called = true
			return nil
		})
		r := NewSerialRunner([]string{"default"}, map[string]Worker{"test.job": worker})

		require.NoError(t, r.Execute(t.Context(), db))

		assert.False(t, called)
		assert.True(t, isDead(t, db, id))

		var attempts int32
		var reason string
		err = db.QueryRow(t.Context(), `SELECT "attempts", "failure_info"->>'error' FROM "raptor_dead_jobs" WHERE "id" = $1`, id).Scan(&attempts, &reason)
		require.NoError(t, err)
		assert.Equal(t, int32(1), attempts)
		assert.Contains(t, reason, "cannot unmarshal")
	})
}
