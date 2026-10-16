DROP FUNCTION "raptor_fail_job"(UUID, VARCHAR, INT, JSONB, INT);

CREATE FUNCTION "raptor_fail_job"(
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
    INSERT INTO "raptor_dead_jobs" SELECT * FROM "raptor_jobs" WHERE "id" = p_job_id;

    UPDATE "raptor_dead_jobs"
    SET "status" = 'failed',
        "failed_at" = now(),
        "failure_info" = p_failure_info
    WHERE "id" = p_job_id;

    DELETE FROM "raptor_jobs" WHERE "id" = p_job_id;

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

CREATE OR REPLACE FUNCTION "raptor_reap_stuck_jobs"(
  p_queue VARCHAR(64) DEFAULT NULL,
  p_retry_delay_ms INT DEFAULT 30000
) RETURNS INT AS $$
DECLARE
  v_reaped_count INT := 0;
  v_job RECORD;
BEGIN
  FOR v_job IN
    SELECT "id", "claimed_by", "attempts" FROM "raptor_jobs"
    WHERE "status" = 'claimed' AND "claimed_at" <= now() - (claimed_ttl || ' seconds')::INTERVAL
      AND (p_queue IS NULL OR "queue_name" = p_queue)
    FOR UPDATE SKIP LOCKED
  LOOP
    PERFORM "raptor_fail_job"(v_job.id, v_job.claimed_by, v_job.attempts, '{"error":"claim timeout"}'::jsonb, p_retry_delay_ms * INTERVAL '1 millisecond');
    v_reaped_count := v_reaped_count + 1;
  END LOOP;

  RETURN v_reaped_count;
END;
$$ LANGUAGE plpgsql;
