ALTER TABLE "raptor_stat_events" ALTER COLUMN "date" SET DEFAULT (now() AT TIME ZONE 'UTC')::date;
