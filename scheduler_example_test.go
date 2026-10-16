package raptor_test

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/angryboat/raptor"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ExampleScheduler() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := raptor.SetupPool(ctx, pool); err != nil {
		log.Fatal(err)
	}

	scheduler, err := raptor.NewScheduler(
		raptor.Schedule{
			Name: "sweep",
			Spec: "@every 30s",
			Job:  raptor.EnqueueJob{Queue: "maintenance", Type: "maintenance.sweep", IdempotencyKey: "sweep"},
		},
		raptor.Schedule{
			Name: "daily-report",
			Spec: "CRON_TZ=America/Denver 0 9 * * 1-5",
			Job:  raptor.EnqueueJob{Queue: "default", Type: "report.daily"},
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := scheduler.Run(ctx, pool); err != nil {
		log.Fatal(err)
	}
}
