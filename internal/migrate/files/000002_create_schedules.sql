CREATE TABLE "raptor_schedules" (
  "name" VARCHAR(128) NOT NULL,
  "spec" VARCHAR(128) NOT NULL,
  "next_run_at" TIMESTAMPTZ NOT NULL,
  "last_run_at" TIMESTAMPTZ,
  "created_at" TIMESTAMPTZ NOT NULL DEFAULT now(),
  "updated_at" TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY ("name", "spec")
);
