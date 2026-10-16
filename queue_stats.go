package raptor

import (
	"context"
	"time"
)

type QueueStat struct {
	Jobs map[string]JobStat
}

type JobStat struct {
	CompletedCount uint64
	FailedCount    uint64
	CancelledCount uint64
	DeadCount      uint64
	TotalDuration  time.Duration
}

func LoadQueueStat(ctx context.Context, db DB, queue string) (*QueueStat, error) {
	query := `SELECT "job_type", "completed_count", "failed_count", "cancelled_count", "dead_count", "total_duration_ms" FROM "raptor_job_stat_totals" WHERE "queue_name" = $1`
	rows, err := db.Query(ctx, query, queue)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stat := &QueueStat{
		Jobs: make(map[string]JobStat),
	}

	for rows.Next() {
		var jobType string
		var completedCount, failedCount, cancelledCount, deadCount, totalDurationMs uint64

		if err := rows.Scan(&jobType, &completedCount, &failedCount, &cancelledCount, &deadCount, &totalDurationMs); err != nil {
			return nil, err
		}

		stat.Jobs[jobType] = JobStat{
			CompletedCount: completedCount,
			FailedCount:    failedCount,
			CancelledCount: cancelledCount,
			DeadCount:      deadCount,
			TotalDuration:  time.Duration(totalDurationMs) * time.Millisecond,
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return stat, nil
}
