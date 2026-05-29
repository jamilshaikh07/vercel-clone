-- Schema v1 — minimal control-plane storage.
-- All tables are IF NOT EXISTS so the migration runner is idempotent
-- even if migration tracking somehow loses state.

-- GitHub App installations on accounts (users or orgs).
CREATE TABLE IF NOT EXISTS installations (
  id               BIGINT      PRIMARY KEY,         -- GitHub installation ID
  account_login    TEXT        NOT NULL,
  account_id       BIGINT      NOT NULL,
  target_type      TEXT        NOT NULL,            -- 'User' or 'Organization'
  suspended_at     TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Repos that an installation has granted us access to.
-- removed_at is set instead of deleting so we keep history.
CREATE TABLE IF NOT EXISTS installation_repos (
  installation_id  BIGINT      NOT NULL REFERENCES installations(id) ON DELETE CASCADE,
  repo_id          BIGINT      NOT NULL,
  full_name        TEXT        NOT NULL,
  private          BOOLEAN     NOT NULL DEFAULT FALSE,
  default_branch   TEXT,
  added_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  removed_at       TIMESTAMPTZ,
  PRIMARY KEY (installation_id, repo_id)
);

CREATE INDEX IF NOT EXISTS installation_repos_full_name_idx
  ON installation_repos(full_name)
  WHERE removed_at IS NULL;

-- A project is the user-facing deployable unit. Currently 1:1 with a repo.
CREATE TABLE IF NOT EXISTS projects (
  id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  installation_id   BIGINT      NOT NULL,
  repo_id           BIGINT      NOT NULL,
  full_name         TEXT        NOT NULL,
  slug              TEXT        NOT NULL UNIQUE,
  production_branch TEXT        NOT NULL DEFAULT 'main',
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (installation_id, repo_id)
);

-- Every push (or manual trigger) produces a deployment row.
-- status flow: queued -> building -> deploying -> ready | failed | cancelled
CREATE TABLE IF NOT EXISTS deployments (
  id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id        UUID        NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  commit_sha        TEXT        NOT NULL,
  ref               TEXT        NOT NULL,
  status            TEXT        NOT NULL DEFAULT 'queued',
  url               TEXT,
  image             TEXT,
  triggered_by      TEXT        NOT NULL,                 -- 'webhook' | 'manual' | 'rebuild'
  delivery_id       TEXT,                                 -- X-GitHub-Delivery for traceability
  build_started_at  TIMESTAMPTZ,
  build_ended_at    TIMESTAMPTZ,
  ready_at          TIMESTAMPTZ,
  error             TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS deployments_project_created_idx
  ON deployments (project_id, created_at DESC);

CREATE INDEX IF NOT EXISTS deployments_pending_idx
  ON deployments (created_at)
  WHERE status IN ('queued', 'building', 'deploying');

-- Audit log of every webhook GitHub has sent us.
-- delivery_id is GitHub's unique X-GitHub-Delivery — gives us idempotency
-- and lets us replay individual deliveries during debugging.
CREATE TABLE IF NOT EXISTS webhook_deliveries (
  delivery_id      TEXT        PRIMARY KEY,
  event            TEXT        NOT NULL,
  action           TEXT,
  installation_id  BIGINT,
  repo_full_name   TEXT,
  payload          JSONB       NOT NULL,
  received_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS webhook_deliveries_event_idx
  ON webhook_deliveries (event, received_at DESC);
