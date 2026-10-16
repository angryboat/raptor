CREATE OR REPLACE FUNCTION "raptor_claim_miss"(
  p_job_id UUID
) RETURNS TEXT AS $$
  SELECT CASE
    WHEN EXISTS (SELECT 1 FROM "raptor_jobs" WHERE "id" = p_job_id)
      OR EXISTS (SELECT 1 FROM "raptor_dead_jobs" WHERE "id" = p_job_id) THEN 'CLAIM_LOST'
    ELSE 'NOT_FOUND'
  END;
$$ LANGUAGE sql;
