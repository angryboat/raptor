package migrate

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
)

//go:embed files/*.sql
var fileFS embed.FS

const lockID int64 = 0x7261_7074_6f72

// Up applies any pending migrations. Concurrent callers, including those in
// other processes, are serialized by a Postgres advisory lock.
func Up(ctx context.Context, conn *pgx.Conn) (err error) {
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		return err
	}
	defer func() {
		_, unlockErr := conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, lockID)
		err = errors.Join(err, unlockErr)
	}()

	return up(ctx, conn)
}

func up(ctx context.Context, conn *pgx.Conn) error {
	k, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS "raptor_migrations" ("key" VARCHAR(32) PRIMARY KEY, "run_at" TIMESTAMPTZ NOT NULL DEFAULT NOW());`)
	if err != nil {
		return err
	}
	slog.DebugContext(ctx, "Setup Raptor Migrations", slog.Bool("created", k.RowsAffected() > 0))

	files, err := fs.Glob(fileFS, "files/*.sql")
	if err != nil {
		return err
	}

	for _, file := range files {
		if err := migrate(ctx, conn, file); err != nil {
			return err
		}
	}

	return nil
}

func migrate(ctx context.Context, conn *pgx.Conn, file string) error {
	key := strings.TrimSuffix(filepath.Base(file), ".sql")
	if len(key) > 32 {
		panic("migration key is too long: " + key)
	}

	t, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer t.Rollback(ctx)

	var exists bool
	err = t.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM "raptor_migrations" WHERE "key" = $1)`, key).Scan(&exists)
	if err != nil || exists {
		return err
	}
	slog.DebugContext(ctx, "Migrate", slog.String("key", key), slog.String("file", file))

	content, err := fileFS.ReadFile(file)
	if err != nil {
		return err
	}

	_, err = t.Exec(ctx, string(content))
	if err != nil {
		return err
	}

	_, err = t.Exec(ctx, `INSERT INTO "raptor_migrations" ("key") VALUES ($1)`, key)
	if err != nil {
		return err
	}

	return t.Commit(ctx)
}
