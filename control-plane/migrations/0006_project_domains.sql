-- Custom domains per project. Lets a user point e.g. blog.example.com at
-- their app instead of relying on the platform-provided <slug>.<zone>.
--
-- Verification flow:
--   1. User adds a hostname → we issue a verification_token.
--   2. User publishes a TXT record at `_paas-verify.<hostname>` whose
--      value equals the token.
--   3. User clicks "Verify" → we lookup the TXT record, set verified_at
--      to now() on match, and the next production deploy (or an
--      explicit re-publish) creates the Traefik IngressRoute that
--      actually routes the host to the project's production Service.
--
-- The hostname is the natural primary key — a single domain can only
-- map to ONE project (no multi-tenant aliasing) and the same project
-- can own many hostnames. CITEXT-style case-insensitivity is enforced
-- by normalising at the application layer (we lower-case before insert)
-- so we keep the column type simple TEXT.

CREATE TABLE IF NOT EXISTS project_domains (
    project_id          UUID        NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    hostname            TEXT        NOT NULL,
    verification_token  TEXT        NOT NULL,
    verified_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (hostname),
    -- Hostname grammar: RFC-952 / 1123 subset. Keeping the CHECK simple:
    -- letters, digits, dot, hyphen; 1..253 chars; not starting/ending
    -- with a dot or hyphen. The app layer does further validation
    -- (rejects single-label, our own zone, etc).
    CHECK (hostname ~ '^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$' AND length(hostname) BETWEEN 3 AND 253)
);

CREATE INDEX IF NOT EXISTS project_domains_project_idx
    ON project_domains (project_id);
