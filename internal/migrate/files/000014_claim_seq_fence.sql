ALTER TABLE "raptor_jobs" ADD COLUMN "claim_seq" BIGINT NOT NULL DEFAULT 0;

ALTER TABLE "raptor_dead_jobs" ADD COLUMN "claim_seq" BIGINT NOT NULL DEFAULT 0;

-- Seeding from attempts keeps claims taken before this migration settleable by callers that still pass attempts.
UPDATE "raptor_jobs" SET "claim_seq" = "attempts" WHERE "status" IN ('pending', 'claimed');

-- One sequence for every row, so a job re-inserted under a reused id can't repeat an old fence; it starts above any INT attempts seeded above.
CREATE SEQUENCE "raptor_claim_seq" AS BIGINT START WITH 2147483648;

CREATE OR REPLACE FUNCTION "raptor_claim_jobs"(
  p_queue VARCHAR(64),
  p_worker_id VARCHAR(64),
  p_limit INT DEFAULT 1,
  p_claimed_ttl INT DEFAULT NULL
) RETURNS SETOF "raptor_jobs" AS $$
BEGIN
  RETURN QUERY
  WITH "claimed_jobs" AS (
    SELECT j."id" FROM "raptor_jobs" j
    WHERE j."queue_name" = p_queue
      AND j."status" = 'pending'
      AND j."visible_at" <= now()
      AND (j."expires_at" IS NULL OR j."expires_at" > now())
    ORDER BY j."priority" DESC, j."visible_at" ASC
    FOR UPDATE SKIP LOCKED
    LIMIT p_limit
  )
  UPDATE "raptor_jobs" j
    SET "status" = 'claimed',
        "claimed_at" = now(),
        "claimed_by" = p_worker_id,
        "claimed_ttl" = COALESCE(p_claimed_ttl, j."claimed_ttl"),
        "attempts" = j."attempts" + 1,
        "claim_seq" = nextval('"raptor_claim_seq"')
    FROM "claimed_jobs" cj
    WHERE j."id" = cj."id"
    RETURNING j.*;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION "raptor_complete_job"(UUID, VARCHAR, INT);

CREATE FUNCTION "raptor_complete_job"(
  p_job_id UUID,
  p_worker_id VARCHAR(64),
  p_claim_seq BIGINT
) RETURNS TEXT AS $$
DECLARE
  v_queue VARCHAR(64);
  v_type VARCHAR(64);
  v_duration_ms BIGINT;
BEGIN
  UPDATE "raptor_jobs"
  SET "status" = 'completed',
      "completed_at" = now()
  WHERE "id" = p_job_id AND "status" = 'claimed' AND "claimed_by" = p_worker_id AND "claim_seq" = p_claim_seq
  RETURNING
    "queue_name",
    "job_type",
    GREATEST(0, EXTRACT(EPOCH FROM (now() - "claimed_at")) * 1000)::BIGINT
  INTO v_queue, v_type, v_duration_ms;

  IF NOT FOUND THEN
    RETURN "raptor_claim_miss"(p_job_id);
  END IF;

  UPDATE "raptor_job_stats"
  SET "completed_count" = "completed_count" + 1,
      "total_duration_ms" = "total_duration_ms" + v_duration_ms
  WHERE "queue_name" = v_queue AND "job_type" = v_type;

  PERFORM "raptor_bump_stats"(p_total_completed => 1);

  RETURN 'COMPLETED';
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION "raptor_release_job"(UUID, VARCHAR, INT);

CREATE FUNCTION "raptor_release_job"(
  p_job_id UUID,
  p_worker_id VARCHAR(64),
  p_claim_seq BIGINT
) RETURNS TEXT AS $$
BEGIN
  UPDATE "raptor_jobs"
  SET "status" = 'pending',
      "claimed_at" = NULL,
      "claimed_by" = NULL,
      "attempts" = "attempts" - 1
  WHERE "id" = p_job_id AND "status" = 'claimed' AND "claimed_by" = p_worker_id AND "claim_seq" = p_claim_seq;

  IF NOT FOUND THEN
    RETURN "raptor_claim_miss"(p_job_id);
  END IF;

  RETURN 'RELEASED';
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION "raptor_fail_job"(UUID, VARCHAR, INT, JSONB, INTERVAL, BOOLEAN);

CREATE FUNCTION "raptor_fail_job"(
  p_job_id UUID,
  p_worker_id VARCHAR(64),
  p_claim_seq BIGINT,
  p_failure_info JSONB DEFAULT 'null'::jsonb,
  p_retry_delay INTERVAL DEFAULT '0',
  p_permanent BOOLEAN DEFAULT false
) RETURNS TEXT AS $$
DECLARE
  v_job "raptor_jobs"%ROWTYPE;
BEGIN
  SELECT * INTO v_job FROM "raptor_jobs"
  WHERE "id" = p_job_id AND "status" = 'claimed' AND "claimed_by" = p_worker_id AND "claim_seq" = p_claim_seq
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
    INSERT INTO "raptor_dead_jobs" ("id", "queue_name", "job_type", "job_data", "priority", "status", "created_at", "visible_at", "expires_at", "claimed_at", "claimed_by", "claimed_ttl", "completed_at", "cancelled_at", "failed_at", "attempts", "max_attempts", "failure_info", "idempotency_key", "claim_seq")
    SELECT "id", "queue_name", "job_type", "job_data", "priority", 'failed', "created_at", "visible_at", "expires_at", "claimed_at", "claimed_by", "claimed_ttl", "completed_at", "cancelled_at", now(), "attempts", "max_attempts", p_failure_info, "idempotency_key", "claim_seq"
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

CREATE OR REPLACE FUNCTION "raptor_reap_stuck_jobs"(
  p_queue VARCHAR(64) DEFAULT NULL,
  p_retry_delay_ms INT DEFAULT 30000
) RETURNS INT AS $$
DECLARE
  v_reaped_count INT := 0;
  v_job RECORD;
BEGIN
  FOR v_job IN
    SELECT "id", "claimed_by", "claim_seq" FROM "raptor_jobs"
    WHERE "status" = 'claimed' AND "claimed_at" <= now() - (claimed_ttl || ' seconds')::INTERVAL
      AND (p_queue IS NULL OR "queue_name" = p_queue)
    FOR UPDATE SKIP LOCKED
  LOOP
    PERFORM "raptor_fail_job"(v_job.id, v_job.claimed_by, v_job.claim_seq, '{"error":"claim timeout"}'::jsonb, p_retry_delay_ms * INTERVAL '1 millisecond');
    v_reaped_count := v_reaped_count + 1;
  END LOOP;

  RETURN v_reaped_count;
END;
$$ LANGUAGE plpgsql;
