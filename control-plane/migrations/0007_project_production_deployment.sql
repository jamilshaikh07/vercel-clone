-- 0007_project_production_deployment.sql
--
-- Tracks which deployment row is the *current* production target for
-- each project — the one the Traefik production-alias IngressRoute
-- (prod-<slug>) is pointing at right now.
--
-- Before this column the dashboard inferred the live deployment from
-- "latest READY on production branch", which is correct after a fresh
-- push but wrong after an instant rollback: the alias may point at an
-- older commit while a newer READY row sits at the top of the list.
-- This column is the explicit source of truth, written by:
--   * builder.go runOne() when it applies the prod alias on a fresh
--     production-branch build.
--   * promote_handlers.go handlePromoteDeployment() when it re-points
--     the alias at an older commit (instant rollback).
--
-- Nullable because newly-created projects exist before they have any
-- ready deployment, and to survive cluster restores where the column
-- was added before any alias swap has been recorded.
--
-- ON DELETE SET NULL because deleting a deployment row (rare today,
-- but possible for cleanup) must not delete the project; the next
-- successful build will re-populate the pointer.

ALTER TABLE projects
  ADD COLUMN production_deployment_id uuid NULL
    REFERENCES deployments(id) ON DELETE SET NULL;

-- Backfill: every existing project's current alias points at the
-- newest READY deployment on its production branch. Same heuristic
-- the dashboard used before this column existed, applied once at
-- migration time so the column is correct on day one.
UPDATE projects p
   SET production_deployment_id = sub.deployment_id
  FROM (
    SELECT DISTINCT ON (d.project_id)
           d.project_id,
           d.id AS deployment_id
      FROM deployments d
      JOIN projects p ON p.id = d.project_id
     WHERE d.status = 'ready'
       AND d.ref = 'refs/heads/' || p.production_branch
     ORDER BY d.project_id, d.created_at DESC
  ) sub
 WHERE p.id = sub.project_id;
