-- Per-project opt-in Postgres databases (Vercel "Storage" tab equivalent).
--
-- One DB per project, MAX. Multi-DB-per-project would be a different table.
-- The password column is plaintext; the same value is also written to a
-- Secret in the project's tenant namespace, which is the source of truth
-- consumed by the running pod via DATABASE_URL env var. We keep a copy
-- here so the dashboard can render "Reveal password" without round-tripping
-- through the K8s API.

CREATE TABLE IF NOT EXISTS project_databases (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id          UUID NOT NULL UNIQUE REFERENCES projects(id) ON DELETE CASCADE,
    db_name             TEXT NOT NULL UNIQUE,
    role_name           TEXT NOT NULL UNIQUE,
    password            TEXT NOT NULL,
    host                TEXT NOT NULL,
    port                INT  NOT NULL DEFAULT 5432,
    secret_name         TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
