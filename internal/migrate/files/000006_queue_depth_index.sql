CREATE INDEX IF NOT EXISTS "idx_raptor_jobs_queue_depth" ON "raptor_jobs" (
  "queue_name",
  "status",
  "visible_at",
  "failed_at",
  "expires_at"
) WHERE "status" IN ('pending', 'claimed');
