-- User-chosen label for the dashboard (e.g. "Urban Equestrian" instead of
-- jamilshaikh07-urbanequestrian). NULL → UI falls back to a friendly slug.

ALTER TABLE projects
  ADD COLUMN IF NOT EXISTS display_name TEXT;
