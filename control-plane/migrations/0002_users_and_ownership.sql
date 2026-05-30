-- Slice A: identities, sessions, and per-user ownership.
--
-- Up to here the control plane was effectively single-tenant: anyone could
-- view the dashboard, every project was visible to every viewer. This
-- migration introduces the minimum schema needed to flip the system into a
-- multi-tenant SaaS: a `users` row per GitHub identity, opaque-token
-- sessions, and ownership columns on the resources users should see.

CREATE TABLE IF NOT EXISTS users (
  id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  github_user_id   BIGINT      NOT NULL UNIQUE,
  github_login     TEXT        NOT NULL,
  email            TEXT,
  avatar_url       TEXT,
  -- The most recent OAuth user-to-server token. We keep it cached so we
  -- can call the GitHub API on the user's behalf (e.g. listing their
  -- installations on first login) without re-prompting. Tokens are
  -- refreshed on each login, so freshness is bounded by session lifetime.
  oauth_token      TEXT,
  oauth_expires_at TIMESTAMPTZ,
  -- Self-promoted via SQL — there's no UI for this. Used to gate
  -- /admin/* style endpoints if we ever add any.
  is_admin         BOOLEAN     NOT NULL DEFAULT FALSE,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS users_login_idx ON users (lower(github_login));

-- Opaque-token sessions. The plaintext token lives only in the user's
-- cookie; we store an HMAC of it so a leaked database row can't be
-- replayed against a live system without also knowing SESSION_SECRET.
CREATE TABLE IF NOT EXISTS sessions (
  token_hash    BYTEA       PRIMARY KEY,
  user_id       UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  user_agent    TEXT,
  ip            TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at    TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS sessions_user_idx    ON sessions (user_id);
CREATE INDEX IF NOT EXISTS sessions_expires_idx ON sessions (expires_at);

-- An installation belongs to whoever's GitHub account ID matches it. We
-- reconcile in two directions: on login we claim any installations whose
-- account_id matches the new user's github_user_id, and on
-- installation.created we link immediately if the user already exists.
ALTER TABLE installations
  ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS installations_owner_idx
  ON installations (owner_user_id);

-- Projects materialise the owner_user_id rather than always joining
-- through installations: the dashboard's "my projects" query stays a
-- single index range scan regardless of how many installations a user has.
ALTER TABLE projects
  ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS projects_owner_idx
  ON projects (owner_user_id);
