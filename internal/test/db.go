package test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func CreateDB(t testing.TB) *pgx.Conn {
	t.Helper()

	dbName := dbNameFor(t)
	createDBWithName(t, dbName)

	conn, err := pgx.Connect(t.Context(), dsnFor(dbName))
	require.NoError(t, err)

	t.Cleanup(func() {
		err := conn.Close(context.Background())
		require.NoError(t, err)
	})

	err = conn.Ping(t.Context())
	require.NoError(t, err)

	return conn
}

// CreatePool is like CreateDB, but returns a connection pool instead of a
// single connection. Use it wherever the code under test issues concurrent
// queries — a bare *pgx.Conn only allows one in-flight query at a time and
// will fail the others with "conn busy".
//
// Like CreateDB, the returned pool is NOT migrated. raptor.Setup requires a
// *pgx.Conn, so migrate a pool with:
//
//	conn, err := pool.Acquire(t.Context())
//	require.NoError(t, err)
//	defer conn.Release()
//	require.NoError(t, raptor.Setup(t.Context(), conn.Conn()))
func CreatePool(t testing.TB) *pgxpool.Pool {
	t.Helper()

	dbName := dbNameFor(t)
	createDBWithName(t, dbName)

	pool, err := pgxpool.New(t.Context(), dsnFor(dbName))
	require.NoError(t, err)

	t.Cleanup(pool.Close)

	err = pool.Ping(t.Context())
	require.NoError(t, err)

	return pool
}

func dbNameFor(t testing.TB) string {
	t.Helper()

	_, f, _, _ := runtime.Caller(0)

	sum := sha256.New()
	_, err := sum.Write([]byte(t.Name()))
	require.NoError(t, err)
	_, err = sum.Write([]byte(f))
	require.NoError(t, err)

	return "raptor_test_" + hex.EncodeToString(sum.Sum(nil)[:16])
}

func dsnFor(dbName string) string {
	return "postgres://postgres:password@localhost:5432/" + dbName + "?sslmode=disable"
}

func createDBWithName(t testing.TB, dbName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, "postgres://postgres:password@localhost:5432/postgres?sslmode=disable")
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, "CREATE DATABASE "+dbName)
	require.NoError(t, err)
}
