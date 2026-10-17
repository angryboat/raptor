DROP FUNCTION IF EXISTS "raptor_cleanup_jobs"(VARCHAR, INTERVAL, INTERVAL, INTERVAL);

CREATE FUNCTION "raptor_cleanup_jobs"(
  p_queue VARCHAR(64) DEFAULT NULL,
  p_completed_retention INTERVAL DEFAULT '24 hours',
  p_cancelled_retention INTERVAL DEFAULT '2 hours',
  p_dead_retention INTERVAL DEFAULT '30 days',
  p_limit INT DEFAULT NULL
) RETURNS INT AS $$
DECLARE
  v_deleted_count INT := 0;
  v_count INT;
BEGIN
  IF p_limit < 1 THEN
    RAISE EXCEPTION 'raptor: cleanup limit must be positive, got %', p_limit;
  END IF;

  DELETE FROM "raptor_jobs"
  WHERE "id" IN (
    SELECT "id" FROM "raptor_jobs"
    WHERE "status" = 'completed' AND "completed_at" <= now() - p_completed_retention
      AND (p_queue IS NULL OR "queue_name" = p_queue)
    LIMIT p_limit
    FOR UPDATE SKIP LOCKED
  );
  GET DIAGNOSTICS v_count = ROW_COUNT;
  v_deleted_count := v_deleted_count + v_count;

  IF p_limit IS NULL OR v_deleted_count < p_limit THEN
    DELETE FROM "raptor_jobs"
    WHERE "id" IN (
      SELECT "id" FROM "raptor_jobs"
      WHERE "status" = 'cancelled' AND "cancelled_at" <= now() - p_cancelled_retention
        AND (p_queue IS NULL OR "queue_name" = p_queue)
      LIMIT p_limit - v_deleted_count
      FOR UPDATE SKIP LOCKED
    );
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_deleted_count := v_deleted_count + v_count;
  END IF;

  IF p_limit IS NULL OR v_deleted_count < p_limit THEN
    DELETE FROM "raptor_dead_jobs"
    WHERE "id" IN (
      SELECT "id" FROM "raptor_dead_jobs"
      WHERE "failed_at" <= now() - p_dead_retention
        AND (p_queue IS NULL OR "queue_name" = p_queue)
      LIMIT p_limit - v_deleted_count
      FOR UPDATE SKIP LOCKED
    );
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_deleted_count := v_deleted_count + v_count;
  END IF;

  RETURN v_deleted_count;
END;
$$ LANGUAGE plpgsql;
