-- Public waitlist signups from the marketing landing page.
-- Approval is manual: operator adds the GitHub login to ALLOWED_GH_LOGINS.

CREATE TABLE IF NOT EXISTS waitlist_signups (
  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  email      TEXT        NOT NULL,
  use_case   TEXT        NOT NULL,
  name       TEXT,
  college    TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT waitlist_use_case_check CHECK (
    use_case IN (
      'final-year-project',
      'portfolio',
      'small-business',
      'hackathon',
      'side-project',
      'other'
    )
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS waitlist_signups_email_lower_idx
  ON waitlist_signups (lower(email));

CREATE INDEX IF NOT EXISTS waitlist_signups_created_idx
  ON waitlist_signups (created_at DESC);
