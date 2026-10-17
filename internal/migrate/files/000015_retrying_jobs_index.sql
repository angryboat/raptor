CREATE INDEX IF NOT EXISTS "idx_raptor_jobs_retrying" ON "raptor_jobs" ("failed_at" DESC)
WHERE "status" = 'pending' AND "failed_at" IS NOT NULL;
