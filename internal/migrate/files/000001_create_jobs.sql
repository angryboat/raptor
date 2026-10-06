CREATE TYPE "raptor_job_status" AS ENUM (
  'pending',
  'claimed',
  'completed',
  'failed',
  'cancelled'
);

CREATE TABLE "raptor_queues" (
  "name" VARCHAR(64) NOT NULL PRIMARY KEY,
  "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE "raptor_stats" (
  "date" DATE NOT NULL,
  "span" VARCHAR(16) NOT NULL CHECK ("span" IN ('day', 'week', 'month', 'all_time')),
  "total_jobs" BIGINT NOT NULL DEFAULT 0,
  "total_completed" BIGINT NOT NULL DEFAULT 0,
  "total_failed" BIGINT NOT NULL DEFAULT 0,
  "total_cancelled" BIGINT NOT NULL DEFAULT 0,
  "total_dead" BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY ("date", "span")
);

CREATE TABLE "raptor_jobs" (
  "id" UUID NOT NULL PRIMARY KEY DEFAULT uuidv7(),
  "queue_name" VARCHAR(64) NOT NULL,
  "job_type" VARCHAR(64) NOT NULL,
  "job_data" JSONB NOT NULL DEFAULT 'null'::jsonb,
  "priority" SMALLINT NOT NULL DEFAULT 0,
  "status" raptor_job_status NOT NULL DEFAULT 'pending',
  "created_at" TIMESTAMPTZ NOT NULL DEFAULT now(),
  "visible_at" TIMESTAMPTZ NOT NULL DEFAULT now(),
  "expires_at" TIMESTAMPTZ,
  "claimed_at" TIMESTAMPTZ,
  "claimed_by" VARCHAR(64),
  "claimed_ttl" INT NOT NULL DEFAULT 300,
  "completed_at" TIMESTAMPTZ,
  "cancelled_at" TIMESTAMPTZ,
  "failed_at" TIMESTAMPTZ,
  "attempts" INT NOT NULL DEFAULT 0,
  "max_attempts" INT NOT NULL DEFAULT 10,
  "timeout_ms" INT NOT NULL DEFAULT 60000,
  "failure_info" JSONB NOT NULL DEFAULT 'null'::jsonb,
  "idempotency_key" VARCHAR(128)
);

CREATE INDEX "idx_raptor_jobs_dequeue" ON "raptor_jobs" (
  "queue_name",
  "priority" DESC,
  "visible_at" ASC
) WHERE "status" = 'pending';

CREATE INDEX "idx_raptor_jobs_expired" ON "raptor_jobs" (
  "queue_name",
  "expires_at"
) WHERE "status" = 'pending' AND "expires_at" IS NOT NULL;

CREATE INDEX "idx_raptor_jobs_dead" ON "raptor_jobs" (
  "queue_name",
  "claimed_at"
) WHERE "status" = 'claimed' AND "claimed_at" IS NOT NULL;

CREATE INDEX "idx_raptor_jobs_completed_cleanup" ON "raptor_jobs" (
  "completed_at"
) WHERE "status" = 'completed';

CREATE INDEX "idx_raptor_jobs_cancelled_cleanup" ON "raptor_jobs" (
  "cancelled_at"
) WHERE "status" = 'cancelled';

CREATE UNIQUE INDEX "idx_raptor_jobs_idempotency_key" ON "raptor_jobs" (
  "queue_name",
  "job_type",
  "idempotency_key"
) WHERE "idempotency_key" IS NOT NULL AND "status" IN ('pending', 'claimed');

CREATE TABLE "raptor_dead_jobs" (LIKE "raptor_jobs" INCLUDING DEFAULTS);

CREATE TABLE "raptor_job_stats" (
  "queue_name" VARCHAR(64) NOT NULL,
  "job_type" VARCHAR(64) NOT NULL,
  "completed_count" BIGINT NOT NULL DEFAULT 0,
  "failed_count" BIGINT NOT NULL DEFAULT 0,
  "cancelled_count" BIGINT NOT NULL DEFAULT 0,
  "dead_count" BIGINT NOT NULL DEFAULT 0,
  "total_duration_ms" BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY ("queue_name", "job_type")
);

CREATE FUNCTION "raptor_bump_stats"(
  p_total_jobs BIGINT DEFAULT 0,
  p_total_completed BIGINT DEFAULT 0,
  p_total_failed BIGINT DEFAULT 0,
  p_total_cancelled BIGINT DEFAULT 0,
  p_total_dead BIGINT DEFAULT 0
) RETURNS VOID AS $$
BEGIN
  INSERT INTO "raptor_stats" ("date", "span", "total_jobs", "total_completed", "total_failed", "total_cancelled", "total_dead")
  VALUES
    (CURRENT_DATE, 'day', p_total_jobs, p_total_completed, p_total_failed, p_total_cancelled, p_total_dead),
    (date_trunc('week', now())::date, 'week', p_total_jobs, p_total_completed, p_total_failed, p_total_cancelled, p_total_dead),
    (date_trunc('month', now())::date, 'month', p_total_jobs, p_total_completed, p_total_failed, p_total_cancelled, p_total_dead),
    (DATE '1970-01-01', 'all_time', p_total_jobs, p_total_completed, p_total_failed, p_total_cancelled, p_total_dead)
  ON CONFLICT ("date", "span") DO UPDATE SET
    "total_jobs" = "raptor_stats"."total_jobs" + EXCLUDED."total_jobs",
    "total_completed" = "raptor_stats"."total_completed" + EXCLUDED."total_completed",
    "total_failed" = "raptor_stats"."total_failed" + EXCLUDED."total_failed",
    "total_cancelled" = "raptor_stats"."total_cancelled" + EXCLUDED."total_cancelled",
    "total_dead" = "raptor_stats"."total_dead" + EXCLUDED."total_dead";
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION "raptor_enqueue_job"(
  p_queue VARCHAR(64),
  p_type VARCHAR(64),
  p_data JSONB,
  p_priority SMALLINT DEFAULT 0,
  p_visible_at TIMESTAMPTZ DEFAULT now(),
  p_expires_at TIMESTAMPTZ DEFAULT NULL,
  p_max_attempts INT DEFAULT 10,
  p_idempotency_key VARCHAR(128) DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
  v_id UUID;
BEGIN
  INSERT INTO "raptor_job_stats" ("queue_name", "job_type") VALUES (p_queue, p_type) ON CONFLICT DO NOTHING;
  INSERT INTO "raptor_queues" ("name") VALUES (p_queue) ON CONFLICT DO NOTHING;

  INSERT INTO "raptor_jobs" ("queue_name", "job_type", "job_data", "priority", "visible_at", "expires_at", "max_attempts", "idempotency_key")
  VALUES (p_queue, p_type, p_data, p_priority, COALESCE(p_visible_at, now()), p_expires_at, p_max_attempts, p_idempotency_key)
  ON CONFLICT ("queue_name", "job_type", "idempotency_key") WHERE "idempotency_key" IS NOT NULL AND "status" IN ('pending', 'claimed')
  DO NOTHING
  RETURNING "id" INTO v_id;

  IF v_id IS NOT NULL THEN
    PERFORM "raptor_bump_stats"(p_total_jobs => 1);
  ELSIF p_idempotency_key IS NOT NULL THEN
    -- An active (pending/claimed) job already owns this key; hand back its id instead of erroring.
    SELECT "id" INTO v_id FROM "raptor_jobs"
    WHERE "queue_name" = p_queue AND "job_type" = p_type AND "idempotency_key" = p_idempotency_key AND "status" IN ('pending', 'claimed');
  END IF;

  RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION "raptor_claim_jobs"(
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
        "attempts" = j."attempts" + 1
    FROM "claimed_jobs" cj
    WHERE j."id" = cj."id"
    RETURNING j.*;
END;
$$ LANGUAGE plpgsql;

-- A fenced complete/fail missed; a still-active job means the caller's claim was reaped or reclaimed.
CREATE FUNCTION "raptor_claim_miss"(
  p_job_id UUID
) RETURNS TEXT AS $$
  SELECT CASE
    WHEN EXISTS (SELECT 1 FROM "raptor_jobs" WHERE "id" = p_job_id AND "status" IN ('pending', 'claimed')) THEN 'CLAIM_LOST'
    ELSE 'NOT_FOUND'
  END;
$$ LANGUAGE sql;

CREATE FUNCTION "raptor_complete_job"(
  p_job_id UUID,
  p_worker_id VARCHAR(64),
  p_attempt INT
) RETURNS TEXT AS $$
DECLARE
  v_queue VARCHAR(64);
  v_type VARCHAR(64);
  v_duration_ms BIGINT;
BEGIN
  UPDATE "raptor_jobs"
  SET "status" = 'completed',
      "completed_at" = now()
  WHERE "id" = p_job_id AND "status" = 'claimed' AND "claimed_by" = p_worker_id AND "attempts" = p_attempt
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

CREATE FUNCTION "raptor_fail_job"(
  p_job_id UUID,
  p_worker_id VARCHAR(64),
  p_attempt INT,
  p_failure_info JSONB DEFAULT 'null'::jsonb,
  p_retry_delay_ms INT DEFAULT 0
) RETURNS TEXT AS $$
DECLARE
  v_job "raptor_jobs"%ROWTYPE;
  v_delay_ms BIGINT;
BEGIN
  SELECT * INTO v_job FROM "raptor_jobs"
  WHERE "id" = p_job_id AND "status" = 'claimed' AND "claimed_by" = p_worker_id AND "attempts" = p_attempt
  FOR UPDATE;
  IF NOT FOUND THEN
    RETURN "raptor_claim_miss"(p_job_id);
  END IF;

  IF v_job.attempts >= v_job.max_attempts THEN
    -- Move to dead jobs
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
    -- Update job for retry
    v_delay_ms := GREATEST(0, p_retry_delay_ms);
    UPDATE "raptor_jobs"
    SET "status" = 'pending',
        "visible_at" = now() + (v_delay_ms || ' milliseconds')::INTERVAL,
        "failed_at" = now(),
        "failure_info" = p_failure_info
    WHERE "id" = p_job_id AND "status" = 'claimed';
    IF NOT FOUND THEN
      RETURN 'NOT_FOUND'; -- Job not found or not in claimed status
    END IF;

    UPDATE "raptor_job_stats"
    SET "failed_count" = "failed_count" + 1
    WHERE "queue_name" = v_job.queue_name AND "job_type" = v_job.job_type;

    PERFORM "raptor_bump_stats"(p_total_failed => 1);

    RETURN 'RETRY_SCHEDULED';
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION "raptor_release_job"(
  p_job_id UUID,
  p_worker_id VARCHAR(64),
  p_attempt INT
) RETURNS TEXT AS $$
BEGIN
  UPDATE "raptor_jobs"
  SET "status" = 'pending',
      "claimed_at" = NULL,
      "claimed_by" = NULL,
      "attempts" = "attempts" - 1
  WHERE "id" = p_job_id AND "status" = 'claimed' AND "claimed_by" = p_worker_id AND "attempts" = p_attempt;

  IF NOT FOUND THEN
    RETURN "raptor_claim_miss"(p_job_id);
  END IF;

  RETURN 'RELEASED';
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION "raptor_cancel_job"(
  p_job_id UUID
) RETURNS BOOLEAN AS $$
DECLARE
  v_queue VARCHAR(64);
  v_type VARCHAR(64);
BEGIN
  UPDATE "raptor_jobs"
  SET "status" = 'cancelled',
      "cancelled_at" = now()
  WHERE "id" = p_job_id AND "status" IN ('pending', 'claimed')
  RETURNING "queue_name", "job_type"
  INTO v_queue, v_type;

  IF NOT FOUND THEN
    RETURN FALSE; -- Job not found or already in a terminal status
  END IF;

  UPDATE "raptor_job_stats"
  SET "cancelled_count" = "cancelled_count" + 1
  WHERE "queue_name" = v_queue AND "job_type" = v_type;

  PERFORM "raptor_bump_stats"(p_total_cancelled => 1);

  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION "raptor_reap_stuck_jobs"(
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
    PERFORM "raptor_fail_job"(v_job.id, v_job.claimed_by, v_job.attempts, '{"error":"claim timeout"}'::jsonb, p_retry_delay_ms);
    v_reaped_count := v_reaped_count + 1;
  END LOOP;

  RETURN v_reaped_count;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION "raptor_expire_jobs"(
  p_queue VARCHAR(64) DEFAULT NULL
) RETURNS INT AS $$
DECLARE
  v_expired_count INT;
BEGIN
  WITH "expired" AS (
    UPDATE "raptor_jobs"
    SET "status" = 'cancelled',
        "cancelled_at" = now(),
        "failure_info" = '{"error":"expired"}'::jsonb
    WHERE "id" IN (
      SELECT "id" FROM "raptor_jobs"
      WHERE "status" = 'pending' AND "expires_at" IS NOT NULL AND "expires_at" <= now()
        AND (p_queue IS NULL OR "queue_name" = p_queue)
      FOR UPDATE SKIP LOCKED
    )
    RETURNING "queue_name", "job_type"
  ), "per_type" AS (
    SELECT "queue_name", "job_type", count(*) AS "n" FROM "expired" GROUP BY "queue_name", "job_type"
  ), "bumped" AS (
    UPDATE "raptor_job_stats" s
    SET "cancelled_count" = s."cancelled_count" + pt."n"
    FROM "per_type" pt
    WHERE s."queue_name" = pt."queue_name" AND s."job_type" = pt."job_type"
  )
  SELECT COALESCE(sum("n"), 0) INTO v_expired_count FROM "per_type";

  IF v_expired_count > 0 THEN
    PERFORM "raptor_bump_stats"(p_total_cancelled => v_expired_count);
  END IF;

  RETURN v_expired_count;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION "raptor_cleanup_jobs"(
  p_queue VARCHAR(64) DEFAULT NULL,
  p_completed_retention INTERVAL DEFAULT '24 hours',
  p_cancelled_retention INTERVAL DEFAULT '2 hours'
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

  RETURN v_deleted_count;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION "raptor_failed_jobs"(
  p_queue VARCHAR(64) DEFAULT NULL,
  p_limit INT DEFAULT 100
) RETURNS SETOF "raptor_jobs" AS $$
BEGIN
  RETURN QUERY
  SELECT * FROM "raptor_jobs"
  WHERE
    "queue_name" = p_queue AND
    "status" = 'pending' AND
    "failed_at" IS NOT NULL
  ORDER BY "failed_at" DESC
  LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;
