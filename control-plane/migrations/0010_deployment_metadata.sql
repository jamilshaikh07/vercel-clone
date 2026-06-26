-- Richer deployment metadata for dashboard KPIs, activity feed, and PR previews.

ALTER TABLE deployments
  ADD COLUMN IF NOT EXISTS commit_message TEXT,
  ADD COLUMN IF NOT EXISTS is_preview     BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS pr_number      INT;

CREATE INDEX IF NOT EXISTS deployments_preview_idx
  ON deployments (project_id, created_at DESC)
  WHERE is_preview = true;
