DROP FUNCTION "raptor_fail_job"(UUID, VARCHAR, INT, JSONB, INTERVAL);

CREATE FUNCTION "raptor_fail_job"(
  p_job_id UUID,
  p_worker_id VARCHAR(64),
  p_attempt INT,
  p_failure_info JSONB DEFAULT 'null'::jsonb,
  p_retry_delay INTERVAL DEFAULT '0',
  p_permanent BOOLEAN DEFAULT false
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

  IF p_permanent OR v_job.attempts >= v_job.max_attempts THEN
    -- A job re-inserted under a dead job's id replaces that record rather than violating the primary key.
    DELETE FROM "raptor_dead_jobs" WHERE "id" = p_job_id;

    WITH "moved" AS (
      DELETE FROM "raptor_jobs" WHERE "id" = p_job_id
      RETURNING *
    )
    INSERT INTO "raptor_dead_jobs" ("id", "queue_name", "job_type", "job_data", "priority", "status", "created_at", "visible_at", "expires_at", "claimed_at", "claimed_by", "claimed_ttl", "completed_at", "cancelled_at", "failed_at", "attempts", "max_attempts", "failure_info", "idempotency_key")
    SELECT "id", "queue_name", "job_type", "job_data", "priority", 'failed', "created_at", "visible_at", "expires_at", "claimed_at", "claimed_by", "claimed_ttl", "completed_at", "cancelled_at", now(), "attempts", "max_attempts", p_failure_info, "idempotency_key"
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
