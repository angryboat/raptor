CREATE TABLE "raptor_stat_events" (
  "date" DATE NOT NULL DEFAULT CURRENT_DATE,
  "total_jobs" BIGINT NOT NULL DEFAULT 0,
  "total_completed" BIGINT NOT NULL DEFAULT 0,
  "total_failed" BIGINT NOT NULL DEFAULT 0,
  "total_cancelled" BIGINT NOT NULL DEFAULT 0,
  "total_dead" BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE "raptor_job_stat_events" (
  "queue_name" VARCHAR(64) NOT NULL,
  "job_type" VARCHAR(64) NOT NULL,
  "completed_count" BIGINT NOT NULL DEFAULT 0,
  "failed_count" BIGINT NOT NULL DEFAULT 0,
  "cancelled_count" BIGINT NOT NULL DEFAULT 0,
  "dead_count" BIGINT NOT NULL DEFAULT 0,
  "total_duration_ms" BIGINT NOT NULL DEFAULT 0
);

DELETE FROM "raptor_stats" WHERE "span" IN ('week', 'month');

ALTER TABLE "raptor_job_stats" RENAME TO "raptor_job_stat_rollups";
ALTER TABLE "raptor_job_stat_rollups" RENAME CONSTRAINT "raptor_job_stats_pkey" TO "raptor_job_stat_rollups_pkey";

-- Settle functions still write "raptor_job_stats"; the triggers below turn those writes into lock-free event inserts.
CREATE VIEW "raptor_job_stats" AS
SELECT "queue_name", "job_type", "completed_count", "failed_count", "cancelled_count", "dead_count", "total_duration_ms"
FROM "raptor_job_stat_rollups";

CREATE FUNCTION "raptor_job_stats_insert"() RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM "raptor_job_stat_rollups" WHERE "queue_name" = NEW."queue_name" AND "job_type" = NEW."job_type") THEN
    INSERT INTO "raptor_job_stat_rollups" ("queue_name", "job_type") VALUES (NEW."queue_name", NEW."job_type") ON CONFLICT DO NOTHING;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "raptor_job_stats_insert" INSTEAD OF INSERT ON "raptor_job_stats"
FOR EACH ROW EXECUTE FUNCTION "raptor_job_stats_insert"();

CREATE FUNCTION "raptor_job_stats_update"() RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO "raptor_job_stat_events" ("queue_name", "job_type", "completed_count", "failed_count", "cancelled_count", "dead_count", "total_duration_ms")
  VALUES (
    OLD."queue_name",
    OLD."job_type",
    NEW."completed_count" - OLD."completed_count",
    NEW."failed_count" - OLD."failed_count",
    NEW."cancelled_count" - OLD."cancelled_count",
    NEW."dead_count" - OLD."dead_count",
    NEW."total_duration_ms" - OLD."total_duration_ms"
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "raptor_job_stats_update" INSTEAD OF UPDATE ON "raptor_job_stats"
FOR EACH ROW EXECUTE FUNCTION "raptor_job_stats_update"();

CREATE VIEW "raptor_job_stat_totals" AS
SELECT "queue_name", "job_type",
  sum("completed_count")::BIGINT AS "completed_count",
  sum("failed_count")::BIGINT AS "failed_count",
  sum("cancelled_count")::BIGINT AS "cancelled_count",
  sum("dead_count")::BIGINT AS "dead_count",
  sum("total_duration_ms")::BIGINT AS "total_duration_ms"
FROM (
  SELECT "queue_name", "job_type", "completed_count", "failed_count", "cancelled_count", "dead_count", "total_duration_ms"
  FROM "raptor_job_stat_rollups"
  UNION ALL
  SELECT "queue_name", "job_type", "completed_count", "failed_count", "cancelled_count", "dead_count", "total_duration_ms"
  FROM "raptor_job_stat_events"
) s
GROUP BY "queue_name", "job_type";

CREATE OR REPLACE FUNCTION "raptor_bump_stats"(
  p_total_jobs BIGINT DEFAULT 0,
  p_total_completed BIGINT DEFAULT 0,
  p_total_failed BIGINT DEFAULT 0,
  p_total_cancelled BIGINT DEFAULT 0,
  p_total_dead BIGINT DEFAULT 0
) RETURNS VOID AS $$
BEGIN
  INSERT INTO "raptor_stat_events" ("total_jobs", "total_completed", "total_failed", "total_cancelled", "total_dead")
  VALUES (p_total_jobs, p_total_completed, p_total_failed, p_total_cancelled, p_total_dead);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION "raptor_expire_jobs"(
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
  ), "recorded" AS (
    INSERT INTO "raptor_job_stat_events" ("queue_name", "job_type", "cancelled_count")
    SELECT "queue_name", "job_type", count(*) FROM "expired" GROUP BY "queue_name", "job_type"
    RETURNING "cancelled_count"
  )
  SELECT COALESCE(sum("cancelled_count"), 0) INTO v_expired_count FROM "recorded";

  IF v_expired_count > 0 THEN
    PERFORM "raptor_bump_stats"(p_total_cancelled => v_expired_count);
  END IF;

  RETURN v_expired_count;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION "raptor_rollup_stats"() RETURNS INT AS $$
DECLARE
  v_stats INT;
  v_job_stats INT;
BEGIN
  IF NOT pg_try_advisory_xact_lock(hashtextextended('raptor_rollup_stats', 0)) THEN
    RETURN 0;
  END IF;

  WITH "rolled" AS (
    DELETE FROM "raptor_stat_events"
    RETURNING "date", "total_jobs", "total_completed", "total_failed", "total_cancelled", "total_dead"
  ), "buckets" AS (
    SELECT "date", 'day' AS "span",
      sum("total_jobs") AS "total_jobs", sum("total_completed") AS "total_completed", sum("total_failed") AS "total_failed",
      sum("total_cancelled") AS "total_cancelled", sum("total_dead") AS "total_dead"
    FROM "rolled"
    GROUP BY "date"
    UNION ALL
    SELECT DATE '1970-01-01', 'all_time',
      sum("total_jobs"), sum("total_completed"), sum("total_failed"), sum("total_cancelled"), sum("total_dead")
    FROM "rolled"
    HAVING count(*) > 0
  ), "upserted" AS (
    INSERT INTO "raptor_stats" ("date", "span", "total_jobs", "total_completed", "total_failed", "total_cancelled", "total_dead")
    SELECT "date", "span", "total_jobs", "total_completed", "total_failed", "total_cancelled", "total_dead"
    FROM "buckets"
    ORDER BY "date", "span"
    ON CONFLICT ("date", "span") DO UPDATE SET
      "total_jobs" = "raptor_stats"."total_jobs" + EXCLUDED."total_jobs",
      "total_completed" = "raptor_stats"."total_completed" + EXCLUDED."total_completed",
      "total_failed" = "raptor_stats"."total_failed" + EXCLUDED."total_failed",
      "total_cancelled" = "raptor_stats"."total_cancelled" + EXCLUDED."total_cancelled",
      "total_dead" = "raptor_stats"."total_dead" + EXCLUDED."total_dead"
  )
  SELECT count(*) INTO v_stats FROM "rolled";

  WITH "rolled" AS (
    DELETE FROM "raptor_job_stat_events"
    RETURNING "queue_name", "job_type", "completed_count", "failed_count", "cancelled_count", "dead_count", "total_duration_ms"
  ), "upserted" AS (
    INSERT INTO "raptor_job_stat_rollups" ("queue_name", "job_type", "completed_count", "failed_count", "cancelled_count", "dead_count", "total_duration_ms")
    SELECT "queue_name", "job_type",
      sum("completed_count"), sum("failed_count"), sum("cancelled_count"), sum("dead_count"), sum("total_duration_ms")
    FROM "rolled"
    GROUP BY "queue_name", "job_type"
    ORDER BY "queue_name", "job_type"
    ON CONFLICT ("queue_name", "job_type") DO UPDATE SET
      "completed_count" = "raptor_job_stat_rollups"."completed_count" + EXCLUDED."completed_count",
      "failed_count" = "raptor_job_stat_rollups"."failed_count" + EXCLUDED."failed_count",
      "cancelled_count" = "raptor_job_stat_rollups"."cancelled_count" + EXCLUDED."cancelled_count",
      "dead_count" = "raptor_job_stat_rollups"."dead_count" + EXCLUDED."dead_count",
      "total_duration_ms" = "raptor_job_stat_rollups"."total_duration_ms" + EXCLUDED."total_duration_ms"
  )
  SELECT count(*) INTO v_job_stats FROM "rolled";

  RETURN v_stats + v_job_stats;
END;
$$ LANGUAGE plpgsql;
