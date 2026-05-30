-- Per-project environment variables (Vercel "Environment Variables" tab).
--
-- The (project_id, name) pair is the natural primary key; we never need
-- to address a row by surrogate id. Values are stored plaintext, same
-- security posture as the OAuth tokens in `users` and the DB password in
-- `project_databases`: only the control-plane SA can reach this database
-- through its NetworkPolicy gate. Encryption-at-rest is a future
-- hardening item (envelope-encrypt with a KMS key) but out of scope here.
--
-- The CHECK on `name` enforces the POSIX-portable env-var grammar so we
-- don't generate Secrets that the kubelet would reject at projection time.
-- DATABASE_URL is reserved: the application layer rejects writes for it
-- because it's owned by the project_databases binding.

CREATE TABLE IF NOT EXISTS project_env_vars (
    project_id  UUID        NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name        TEXT        NOT NULL CHECK (name ~ '^[A-Z_][A-Z0-9_]*$' AND length(name) <= 128),
    value       TEXT        NOT NULL CHECK (length(value) <= 65536),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, name)
);

CREATE INDEX IF NOT EXISTS project_env_vars_project_idx
    ON project_env_vars (project_id);
