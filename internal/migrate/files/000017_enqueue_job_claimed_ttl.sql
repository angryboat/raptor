DROP FUNCTION "raptor_enqueue_job"(VARCHAR, VARCHAR, JSONB, SMALLINT, TIMESTAMPTZ, TIMESTAMPTZ, INT, VARCHAR);

CREATE FUNCTION "raptor_enqueue_job"(
  p_queue VARCHAR(64),
  p_type VARCHAR(64),
  p_data JSONB,
  p_priority SMALLINT DEFAULT 0,
  p_visible_at TIMESTAMPTZ DEFAULT now(),
  p_expires_at TIMESTAMPTZ DEFAULT NULL,
  p_max_attempts INT DEFAULT 10,
  p_idempotency_key VARCHAR(128) DEFAULT NULL,
  p_claimed_ttl INT DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
  v_id UUID;
BEGIN
  INSERT INTO "raptor_job_stats" ("queue_name", "job_type") VALUES (p_queue, p_type) ON CONFLICT DO NOTHING;
  INSERT INTO "raptor_queues" ("name") VALUES (p_queue) ON CONFLICT DO NOTHING;

  -- The job holding the key can finish between the insert and the lookup, freeing the key, so try the insert again.
  FOR v_attempt IN 1..3 LOOP
    INSERT INTO "raptor_jobs" ("queue_name", "job_type", "job_data", "priority", "visible_at", "expires_at", "max_attempts", "idempotency_key", "claimed_ttl")
    VALUES (p_queue, p_type, p_data, p_priority, COALESCE(p_visible_at, now()), p_expires_at, p_max_attempts, p_idempotency_key, COALESCE(p_claimed_ttl, 300))
    ON CONFLICT ("queue_name", "job_type", "idempotency_key") WHERE "idempotency_key" IS NOT NULL AND "status" IN ('pending', 'claimed')
    DO NOTHING
    RETURNING "id" INTO v_id;

    IF v_id IS NOT NULL THEN
      PERFORM "raptor_bump_stats"(p_total_jobs => 1);
      RETURN v_id;
    END IF;

    SELECT "id" INTO v_id FROM "raptor_jobs"
    WHERE "queue_name" = p_queue AND "job_type" = p_type AND "idempotency_key" = p_idempotency_key AND "status" IN ('pending', 'claimed');

    IF v_id IS NOT NULL THEN
      RETURN v_id;
    END IF;
  END LOOP;

  RAISE EXCEPTION 'raptor: enqueue neither inserted a job nor found one holding its idempotency key' USING ERRCODE = 'RA001';
END;
$$ LANGUAGE plpgsql;
