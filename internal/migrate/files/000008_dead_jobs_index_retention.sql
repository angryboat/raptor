LOCK TABLE "raptor_dead_jobs" IN ACCESS EXCLUSIVE MODE;

DELETE FROM "raptor_dead_jobs"
WHERE ctid IN (
  SELECT ctid FROM (
    SELECT ctid, row_number() OVER (PARTITION BY "id" ORDER BY "failed_at" DESC NULLS LAST, ctid DESC) AS "rank"
    FROM "raptor_dead_jobs"
  ) "ranked"
  WHERE "rank" > 1
);

ALTER TABLE "raptor_dead_jobs" ADD CONSTRAINT "raptor_dead_jobs_pkey" PRIMARY KEY ("id");

CREATE INDEX "idx_raptor_dead_jobs_failed_at" ON "raptor_dead_jobs" ("failed_at");

CREATE OR REPLACE FUNCTION "raptor_fail_job"(
  p_job_id UUID,
  p_worker_id VARCHAR(64),
  p_attempt INT,
  p_failure_info JSONB DEFAULT 'null'::jsonb,
  p_retry_delay INTERVAL DEFAULT '0'
) RETURNS TEXT AS $$
DECLARE
  v_job "raptor_jobs"%ROWTYPE;
BEGIN
  SELECT * INTO v_job FROM "raptor_jobs"
  WHERE "id" = p_job_id AND "status" = 'claimed' AND "claimed_by" = p_worker_id AND "attempts" = p_attempt
  FOR UPDATE;
  IF NOT FOUND THEN
    RETURN "raptor_claim_miss"(p_job_id);
  END IF;

  IF v_job.attempts >= v_job.max_attempts THEN
    -- A job re-inserted under a dead job's id replaces that record rather than violating the primary key.
    DELETE FROM "raptor_dead_jobs" WHERE "id" = p_job_id;

    WITH "moved" AS (
      DELETE FROM "raptor_jobs" WHERE "id" = p_job_id
      RETURNING *
    )
    INSERT INTO "raptor_dead_jobs" ("id", "queue_name", "job_type", "job_data", "priority", "status", "created_at", "visible_at", "expires_at", "claimed_at", "claimed_by", "claimed_ttl", "completed_at", "cancelled_at", "failed_at", "attempts", "max_attempts", "timeout_ms", "failure_info", "idempotency_key")
    SELECT "id", "queue_name", "job_type", "job_data", "priority", 'failed', "created_at", "visible_at", "expires_at", "claimed_at", "claimed_by", "claimed_ttl", "completed_at", "cancelled_at", now(), "attempts", "max_attempts", "timeout_ms", p_failure_info, "idempotency_key"
    FROM "moved";

    UPDATE "raptor_job_stats"
    SET "dead_count" = "dead_count" + 1
    WHERE "queue_name" = v_job.queue_name AND "job_type" = v_job.job_type;

    PERFORM "raptor_bump_stats"(p_total_dead => 1);

    RETURN 'DEAD';
  ELSE
    UPDATE "raptor_jobs"
    SET "status" = 'pending',
        "visible_at" = now() + GREATEST(p_retry_delay, INTERVAL '0'),
        "failed_at" = now(),
        "failure_info" = p_failure_info
    WHERE "id" = p_job_id AND "status" = 'claimed';
    IF NOT FOUND THEN
      RETURN 'NOT_FOUND';
    END IF;

    UPDATE "raptor_job_stats"
    SET "failed_count" = "failed_count" + 1
    WHERE "queue_name" = v_job.queue_name AND "job_type" = v_job.job_type;

    PERFORM "raptor_bump_stats"(p_total_failed => 1);

    RETURN 'RETRY_SCHEDULED';
  END IF;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS "raptor_cleanup_jobs"(VARCHAR, INTERVAL, INTERVAL);

CREATE OR REPLACE FUNCTION "raptor_cleanup_jobs"(
  p_queue VARCHAR(64) DEFAULT NULL,
  p_completed_retention INTERVAL DEFAULT '24 hours',
  p_cancelled_retention INTERVAL DEFAULT '2 hours',
  p_dead_retention INTERVAL DEFAULT '30 days'
) RETURNS INT AS $$
DECLARE
  v_deleted_count INT := 0;
  v_count INT;
BEGIN
  DELETE FROM "raptor_jobs"
  WHERE "status" = 'completed' AND "completed_at" <= now() - p_completed_retention
    AND (p_queue IS NULL OR "queue_name" = p_queue);
  GET DIAGNOSTICS v_count = ROW_COUNT;
  v_deleted_count := v_deleted_count + v_count;

  DELETE FROM "raptor_jobs"
  WHERE "status" = 'cancelled' AND "cancelled_at" <= now() - p_cancelled_retention
    AND (p_queue IS NULL OR "queue_name" = p_queue);
  GET DIAGNOSTICS v_count = ROW_COUNT;
  v_deleted_count := v_deleted_count + v_count;

  DELETE FROM "raptor_dead_jobs"
  WHERE "failed_at" <= now() - p_dead_retention
    AND (p_queue IS NULL OR "queue_name" = p_queue);
  GET DIAGNOSTICS v_count = ROW_COUNT;
  v_deleted_count := v_deleted_count + v_count;

  RETURN v_deleted_count;
END;
$$ LANGUAGE plpgsql;
