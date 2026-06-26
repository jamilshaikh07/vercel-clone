package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// store is the (currently tiny) repo layer over Postgres.
// All methods are idempotent so retries from GitHub or restarts of
// the control plane never produce duplicates or partial writes.
type store struct {
	pool *pgxpool.Pool
}

func newStore(pool *pgxpool.Pool) *store { return &store{pool: pool} }

// RecordDelivery persists every webhook we receive, keyed by GitHub's
// X-GitHub-Delivery. Returns (false, nil) if the delivery was already
// recorded — useful so callers can short-circuit dispatch on retries.
func (s *store) RecordDelivery(
	ctx context.Context,
	deliveryID, event, action string,
	installationID int64,
	repoFullName string,
	payload []byte,
) (inserted bool, err error) {
	if deliveryID == "" {
		return false, errors.New("empty delivery id")
	}
	// Validate JSON before storing — pgx will reject garbage too but the
	// error is clearer here.
	if !json.Valid(payload) {
		return false, errors.New("payload is not valid JSON")
	}

	tag, err := s.pool.Exec(ctx, `
		INSERT INTO webhook_deliveries
			(delivery_id, event, action, installation_id, repo_full_name, payload)
		VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, 0), NULLIF($5, ''), $6::jsonb)
		ON CONFLICT (delivery_id) DO NOTHING
	`, deliveryID, event, action, installationID, repoFullName, string(payload))
	if err != nil {
		return false, fmt.Errorf("insert delivery: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

type installationUpsert struct {
	ID           int64
	AccountLogin string
	AccountID    int64
	TargetType   string
}

func (s *store) UpsertInstallation(ctx context.Context, in installationUpsert) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO installations (id, account_login, account_id, target_type)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE
		   SET account_login = EXCLUDED.account_login,
		       account_id    = EXCLUDED.account_id,
		       target_type   = EXCLUDED.target_type,
		       suspended_at  = NULL,
		       updated_at    = now()
	`, in.ID, in.AccountLogin, in.AccountID, in.TargetType)
	return err
}

func (s *store) SuspendInstallation(ctx context.Context, id int64, suspended bool) error {
	q := `UPDATE installations SET suspended_at = now(), updated_at = now() WHERE id = $1`
	if !suspended {
		q = `UPDATE installations SET suspended_at = NULL, updated_at = now() WHERE id = $1`
	}
	_, err := s.pool.Exec(ctx, q, id)
	return err
}

func (s *store) DeleteInstallation(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM installations WHERE id = $1`, id)
	return err
}

type repoRow struct {
	ID            int64
	FullName      string
	Private       bool
	DefaultBranch string
}

func (s *store) AddRepos(ctx context.Context, installationID int64, repos []repoRow, maxProjectsPerUser int) error {
	if len(repos) == 0 {
		return nil
	}

	var ownerID *string
	if maxProjectsPerUser > 0 {
		var owner sql.NullString
		if err := s.pool.QueryRow(ctx, `
			SELECT owner_user_id::text FROM installations WHERE id = $1
		`, installationID).Scan(&owner); err == nil && owner.Valid && owner.String != "" {
			ownerID = &owner.String
		}
	}

	existing := 0
	if ownerID != nil && *ownerID != "" && maxProjectsPerUser > 0 {
		var err error
		existing, err = s.CountProjectsForUser(ctx, *ownerID)
		if err != nil {
			return err
		}
	}

	batch := &pgx.Batch{}
	for _, r := range repos {
		batch.Queue(`
			INSERT INTO installation_repos
				(installation_id, repo_id, full_name, private, default_branch, removed_at)
			VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULL)
			ON CONFLICT (installation_id, repo_id) DO UPDATE
			   SET full_name      = EXCLUDED.full_name,
			       private        = EXCLUDED.private,
			       default_branch = EXCLUDED.default_branch,
			       removed_at     = NULL
		`, installationID, r.ID, r.FullName, r.Private, r.DefaultBranch)

		if maxProjectsPerUser > 0 && ownerID != nil && *ownerID != "" && existing >= maxProjectsPerUser {
			continue
		}

		batch.Queue(`
			INSERT INTO projects (installation_id, repo_id, full_name, slug,
			                     production_branch, owner_user_id)
			SELECT $1, $2, $3, $4, COALESCE(NULLIF($5, ''), 'main'), i.owner_user_id
			  FROM installations i
			 WHERE i.id = $1
			ON CONFLICT (installation_id, repo_id) DO NOTHING
		`, installationID, r.ID, r.FullName, slugify(r.FullName), r.DefaultBranch)
		if maxProjectsPerUser > 0 && ownerID != nil && *ownerID != "" {
			existing++
		}
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range batch.Len() {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) RemoveRepos(ctx context.Context, installationID int64, repoIDs []int64) error {
	if len(repoIDs) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE installation_repos
		   SET removed_at = now()
		 WHERE installation_id = $1 AND repo_id = ANY($2) AND removed_at IS NULL
	`, installationID, repoIDs)
	return err
}

// EnqueueDeployment creates a queued deployment row for a push event.
// Returns the project_id and deployment_id. If the project is not yet
// known (e.g. push arrived before installation_repositories.added),
// returns (zero, zero, nil) and the caller should log + skip — never
// auto-create projects from pushes alone, to avoid drift if access was
// later revoked.
type deploymentEnqueued struct {
	ProjectID    string
	DeploymentID string
	Slug         string
	// Deduped is true when we deliberately skipped creating a row because
	// this project + commit_sha was already built (and the push was not to
	// the production branch). ProjectID + Slug are still populated so the
	// caller can log a meaningful message; DeploymentID is empty.
	Deduped bool
}

func (s *store) EnqueueDeployment(
	ctx context.Context,
	installationID, repoID int64,
	commitSHA, ref, deliveryID, triggeredBy string,
) (*deploymentEnqueued, error) {
	// Audit-log values: 'webhook' (push event), 'install' (auto-deploy
	// fired after the GitHub App was installed on a repo), 'manual'
	// (dashboard 'Deploy now' button), 'redeploy' (re-run of an
	// existing deployment row; handled by EnqueueRedeploy, not here).
	// Default to 'webhook' if a caller forgets — keeps the column
	// non-NULL without forcing every call site to think about it.
	if triggeredBy == "" {
		triggeredBy = "webhook"
	}
	var (
		projectID        string
		slug             string
		productionBranch string
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id::text, slug, production_branch
		  FROM projects
		 WHERE installation_id = $1 AND repo_id = $2
	`, installationID, repoID).Scan(&projectID, &slug, &productionBranch)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup project: %w", err)
	}

	// Dedupe: a NON-production push whose commit SHA we've already built
	// for this project is redundant — the image and the per-commit preview
	// URL (<slug>-<sha>) are keyed by SHA, so rebuilding produces the exact
	// same artifact. The classic case is the "Open improvement PR" bot
	// forking a branch off the production HEAD: that ref-create push carries
	// a SHA main already built. Production-branch pushes are NEVER deduped
	// so the prod alias is always (re)promoted, even on a fast-forward or
	// rebase merge that reuses an already-built SHA.
	prodRef := ""
	if productionBranch != "" {
		prodRef = "refs/heads/" + productionBranch
	}
	// Only webhook-driven pushes are deduped. Manual 'Deploy now', install
	// auto-deploy, and redeploys are explicit user intent and must always
	// build, even if the SHA was seen before.
	if triggeredBy == "webhook" && ref != prodRef {
		var existing string
		derr := s.pool.QueryRow(ctx, `
			SELECT id::text
			  FROM deployments
			 WHERE project_id = $1::uuid AND commit_sha = $2
			 LIMIT 1
		`, projectID, commitSHA).Scan(&existing)
		switch {
		case derr == nil:
			return &deploymentEnqueued{ProjectID: projectID, Slug: slug, Deduped: true}, nil
		case errors.Is(derr, pgx.ErrNoRows):
			// Not seen before — fall through and build it.
		default:
			return nil, fmt.Errorf("dedupe lookup: %w", derr)
		}
	}

	var deploymentID string
	if err := s.pool.QueryRow(ctx, `
		INSERT INTO deployments (project_id, commit_sha, ref, triggered_by, delivery_id)
		VALUES ($1::uuid, $2, $3, $4, NULLIF($5, ''))
		RETURNING id::text
	`, projectID, commitSHA, ref, triggeredBy, deliveryID).Scan(&deploymentID); err != nil {
		return nil, fmt.Errorf("insert deployment: %w", err)
	}
	return &deploymentEnqueued{ProjectID: projectID, DeploymentID: deploymentID, Slug: slug}, nil
}

// projectDeployTarget is the minimum identity needed to bootstrap a build
// for a project that has no existing deployment rows to copy from: the
// GitHub installation + repo ID (to mint a clone token and address the
// repo on the API), the production branch (to look up HEAD), and the
// human-friendly slug (for log messages). Used by the manual
// "Deploy now" handler. Owner scoping is performed in-query so callers
// can't accidentally bypass it.
type projectDeployTarget struct {
	ProjectID        string
	Slug             string
	InstallationID   int64
	RepoID           int64
	RepoFullName     string
	ProductionBranch string
}

// GetProjectForDeploy returns the deploy-bootstrapping identity for a
// project the caller owns (or any project, if ownerUserID == "" — the
// admin sentinel used by server-internal call sites). Returns
// (nil, nil) when no such project exists or the user doesn't own it,
// so callers can map that to 404 without leaking which IDs exist.
func (s *store) GetProjectForDeploy(ctx context.Context, projectID, ownerUserID string) (*projectDeployTarget, error) {
	q := `
		SELECT p.id::text, p.slug, p.installation_id, p.repo_id,
		       p.full_name, p.production_branch
		  FROM projects p
		 WHERE p.id = $1::uuid
		   AND ($2 = '' OR p.owner_user_id = $2::uuid)
	`
	var t projectDeployTarget
	row := s.pool.QueryRow(ctx, q, projectID, ownerUserID)
	if err := row.Scan(&t.ProjectID, &t.Slug, &t.InstallationID,
		&t.RepoID, &t.RepoFullName, &t.ProductionBranch); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &t, nil
}

// UpdateProjectProductionBranch sets the project's production_branch and
// also writes the same value into installation_repos.default_branch so
// later auto-deploy paths (which read from that table) stay consistent.
// Used by the deploy-now handler when the cached branch turns out to be
// wrong and we've just resolved the real default from the GitHub API.
func (s *store) UpdateProjectProductionBranch(ctx context.Context, projectID, branch string) error {
	_, err := s.pool.Exec(ctx, `
		WITH updated AS (
			UPDATE projects
			   SET production_branch = $2
			 WHERE id = $1::uuid
			RETURNING installation_id, repo_id
		)
		UPDATE installation_repos r
		   SET default_branch = $2
		  FROM updated u
		 WHERE r.installation_id = u.installation_id
		   AND r.repo_id         = u.repo_id
	`, projectID, branch)
	return err
}

// EnqueueRedeploy inserts a new queued deployment row that mirrors the
// (project_id, commit_sha, ref) of an existing source deployment. The
// ownership filter is applied inline (via the join on projects) so a
// caller that wasn't gated upstream still can't enqueue against another
// tenant's project. Empty userID skips ownership (admin path).
//
// Returns (newDeploymentID, projectSlug, nil) on success. Returns
// ("", "", nil) when the source row doesn't exist or isn't owned —
// callers translate that to 404 so we don't leak which IDs exist.
//
// triggered_by is recorded as 'redeploy' so audit logs can distinguish
// dashboard-initiated rebuilds from real webhook pushes. delivery_id
// is NULL because there's no GitHub delivery behind this row.
func (s *store) EnqueueRedeploy(ctx context.Context, sourceDeploymentID, userID string) (string, string, error) {
	var (
		newID string
		slug  string
		err   error
	)
	if userID == "" {
		err = s.pool.QueryRow(ctx, `
			WITH src AS (
			  SELECT d.project_id, d.commit_sha, d.ref, p.slug
			    FROM deployments d
			    JOIN projects p ON p.id = d.project_id
			   WHERE d.id::text = $1
			)
			INSERT INTO deployments (project_id, commit_sha, ref, triggered_by, delivery_id)
			SELECT project_id, commit_sha, ref, 'redeploy', NULL FROM src
			RETURNING id::text, (SELECT slug FROM src)
		`, sourceDeploymentID).Scan(&newID, &slug)
	} else {
		err = s.pool.QueryRow(ctx, `
			WITH src AS (
			  SELECT d.project_id, d.commit_sha, d.ref, p.slug
			    FROM deployments d
			    JOIN projects p ON p.id = d.project_id
			   WHERE d.id::text = $1
			     AND p.owner_user_id = $2::uuid
			)
			INSERT INTO deployments (project_id, commit_sha, ref, triggered_by, delivery_id)
			SELECT project_id, commit_sha, ref, 'redeploy', NULL FROM src
			RETURNING id::text, (SELECT slug FROM src)
		`, sourceDeploymentID, userID).Scan(&newID, &slug)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}
	return newID, slug, nil
}

type deploymentRow struct {
	ID         string  `json:"id"`
	ProjectID  string  `json:"project_id"`
	Slug       string  `json:"project_slug"`
	CommitSHA  string  `json:"commit_sha"`
	Ref        string  `json:"ref"`
	Status     string  `json:"status"`
	URL        *string `json:"url,omitempty"`
	Image      *string `json:"image,omitempty"`
	CreatedAt  string  `json:"created_at"`
	DeliveryID *string `json:"delivery_id,omitempty"`
}

func (s *store) ListRecentDeployments(ctx context.Context, limit int) ([]deploymentRow, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT d.id::text, d.project_id::text, p.slug,
		       d.commit_sha, d.ref, d.status, d.url, d.image,
		       to_char(d.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		       d.delivery_id
		  FROM deployments d
		  JOIN projects p ON p.id = d.project_id
		 ORDER BY d.created_at DESC
		 LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []deploymentRow
	for rows.Next() {
		var d deploymentRow
		if err := rows.Scan(&d.ID, &d.ProjectID, &d.Slug, &d.CommitSHA, &d.Ref,
			&d.Status, &d.URL, &d.Image, &d.CreatedAt, &d.DeliveryID); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// projectRow is the dashboard-facing summary of a project. We intentionally
// don't expose installation_id or repo_id — those are GitHub-internal IDs.
type projectRow struct {
	ID               string  `json:"id"`
	Slug             string  `json:"slug"`
	FullName         string  `json:"full_name"`
	DisplayName      string  `json:"display_name,omitempty"`
	ProductionBranch string  `json:"production_branch"`
	CreatedAt        string  `json:"created_at"`
	// ProductionDeploymentID is the deployment whose Service the
	// prod-<slug> Traefik IngressRoute is currently pointing at. NULL
	// for projects that have never had a ready production build (e.g.
	// brand-new repo, only failed builds so far). The dashboard uses
	// this to pick the 'current' row in the deployments list — this
	// is the source of truth after an instant rollback, not the
	// 'latest READY by created_at' heuristic.
	ProductionDeploymentID *string `json:"production_deployment_id,omitempty"`
}

// ListProjectsForUser returns the projects owned by one user. Pass an
// empty userID to list everything (admin/dev path only — caller is
// responsible for the gate).
func (s *store) ListProjectsForUser(ctx context.Context, userID string) ([]projectRow, error) {
	var rows pgx.Rows
	var err error
	if userID == "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id::text, slug, full_name,
			       COALESCE(display_name, ''), production_branch,
			       to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
			       production_deployment_id::text
			  FROM projects
			 ORDER BY created_at DESC
		`)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id::text, slug, full_name,
			       COALESCE(display_name, ''), production_branch,
			       to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
			       production_deployment_id::text
			  FROM projects
			 WHERE owner_user_id = $1
			 ORDER BY created_at DESC
		`, userID)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []projectRow
	for rows.Next() {
		var p projectRow
		var prodDepID *string
		if err := rows.Scan(&p.ID, &p.Slug, &p.FullName, &p.DisplayName, &p.ProductionBranch, &p.CreatedAt, &prodDepID); err != nil {
			return nil, err
		}
		p.ProductionDeploymentID = prodDepID
		out = append(out, p)
	}
	return out, rows.Err()
}

// projectWithTenant is a thin variant of projectRow that also carries the
// GitHub installation login — required to derive the tenant namespace
// (paas-tenant-<login>) for telemetry lookups.
type projectWithTenant struct {
	ID          string
	Slug        string
	TenantLogin string
}

// ListProjectsWithTenant returns id/slug/tenant_login per project for one
// user (or all, when userID==""). Smaller select than ListProjectsForUser
// — used by the telemetry rollup which doesn't need full_name etc.
func (s *store) ListProjectsWithTenant(ctx context.Context, userID string) ([]projectWithTenant, error) {
	var rows pgx.Rows
	var err error
	const q = `
		SELECT p.id::text, p.slug, i.account_login
		  FROM projects p
		  JOIN installations i ON i.id = p.installation_id
		%s
		 ORDER BY p.slug
	`
	if userID == "" {
		rows, err = s.pool.Query(ctx, fmt.Sprintf(q, ""))
	} else {
		rows, err = s.pool.Query(ctx, fmt.Sprintf(q, "WHERE p.owner_user_id = $1::uuid"), userID)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []projectWithTenant
	for rows.Next() {
		var p projectWithTenant
		if err := rows.Scan(&p.ID, &p.Slug, &p.TenantLogin); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ListDeploymentsForProjects returns deployments grouped by project_id, capped
// at perProject rows per project. One round-trip via a window function so the
// dashboard renders in O(1) queries regardless of project count.
func (s *store) ListDeploymentsForProjects(ctx context.Context, projectIDs []string, perProject int) (map[string][]deploymentRow, error) {
	if len(projectIDs) == 0 {
		return map[string][]deploymentRow{}, nil
	}
	if perProject <= 0 || perProject > 50 {
		perProject = 10
	}
	rows, err := s.pool.Query(ctx, `
		WITH ranked AS (
			SELECT d.id::text AS id, d.project_id::text AS project_id,
			       p.slug, d.commit_sha, d.ref, d.status, d.url, d.image,
			       to_char(d.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
			       d.delivery_id,
			       row_number() OVER (PARTITION BY d.project_id ORDER BY d.created_at DESC) AS rn
			  FROM deployments d
			  JOIN projects p ON p.id = d.project_id
			 WHERE d.project_id::text = ANY($1)
		)
		SELECT id, project_id, slug, commit_sha, ref, status, url, image,
		       created_at, delivery_id
		  FROM ranked
		 WHERE rn <= $2
		 ORDER BY project_id, created_at DESC
	`, projectIDs, perProject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string][]deploymentRow, len(projectIDs))
	for rows.Next() {
		var d deploymentRow
		if err := rows.Scan(&d.ID, &d.ProjectID, &d.Slug, &d.CommitSHA, &d.Ref,
			&d.Status, &d.URL, &d.Image, &d.CreatedAt, &d.DeliveryID); err != nil {
			return nil, err
		}
		out[d.ProjectID] = append(out[d.ProjectID], d)
	}
	return out, rows.Err()
}

// GetDeployment returns the single deployment row for an ID, or (nil, nil) if
// not found. Used by the SSE log handler to validate the ID and surface
// current status to clients before/after streaming.
func (s *store) GetDeployment(ctx context.Context, id string) (*deploymentRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT d.id::text, d.project_id::text, p.slug,
		       d.commit_sha, d.ref, d.status, d.url, d.image,
		       to_char(d.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		       d.delivery_id
		  FROM deployments d
		  JOIN projects p ON p.id = d.project_id
		 WHERE d.id::text = $1
		 LIMIT 1
	`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	var d deploymentRow
	if err := rows.Scan(&d.ID, &d.ProjectID, &d.Slug, &d.CommitSHA, &d.Ref,
		&d.Status, &d.URL, &d.Image, &d.CreatedAt, &d.DeliveryID); err != nil {
		return nil, err
	}
	return &d, nil
}

// GetDeploymentForUser returns the deployment only if the user owns the
// project it belongs to. Empty userID is a wildcard match (admin-only path,
// caller's responsibility to gate). Returns (nil, nil) when not found or
// not owned — callers translate this to 404 so we don't leak existence.
func (s *store) GetDeploymentForUser(ctx context.Context, id, userID string) (*deploymentRow, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if userID == "" {
		return s.GetDeployment(ctx, id)
	}
	rows, err = s.pool.Query(ctx, `
		SELECT d.id::text, d.project_id::text, p.slug,
		       d.commit_sha, d.ref, d.status, d.url, d.image,
		       to_char(d.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		       d.delivery_id
		  FROM deployments d
		  JOIN projects p ON p.id = d.project_id
		 WHERE d.id::text = $1
		   AND p.owner_user_id = $2
		 LIMIT 1
	`, id, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	var d deploymentRow
	if err := rows.Scan(&d.ID, &d.ProjectID, &d.Slug, &d.CommitSHA, &d.Ref,
		&d.Status, &d.URL, &d.Image, &d.CreatedAt, &d.DeliveryID); err != nil {
		return nil, err
	}
	return &d, nil
}

// ListRecentDeploymentsForUser is the user-scoped variant. Empty userID
// falls back to the unscoped list.
func (s *store) ListRecentDeploymentsForUser(ctx context.Context, userID string, limit int) ([]deploymentRow, error) {
	if userID == "" {
		return s.ListRecentDeployments(ctx, limit)
	}
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT d.id::text, d.project_id::text, p.slug, d.commit_sha, d.ref,
		       d.status, d.url, d.image,
		       to_char(d.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		       d.delivery_id
		  FROM deployments d
		  JOIN projects p ON p.id = d.project_id
		 WHERE p.owner_user_id = $1
		 ORDER BY d.created_at DESC
		 LIMIT $2
	`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []deploymentRow
	for rows.Next() {
		var d deploymentRow
		if err := rows.Scan(&d.ID, &d.ProjectID, &d.Slug, &d.CommitSHA, &d.Ref,
			&d.Status, &d.URL, &d.Image, &d.CreatedAt, &d.DeliveryID); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// GetDeploymentTenantLogin returns the GitHub login of the installation
// that owns this deployment — the same value used to derive the tenant
// namespace via tenantNamespaceFor(). Returns ("", nil) when the row is
// not found or not owned by userID. Passing "" for userID skips the
// ownership filter (admin path only — caller must gate).
func (s *store) GetDeploymentTenantLogin(ctx context.Context, deploymentID, userID string) (string, error) {
	var login string
	var err error
	if userID == "" {
		err = s.pool.QueryRow(ctx, `
			SELECT i.account_login
			  FROM deployments d
			  JOIN projects p      ON p.id = d.project_id
			  JOIN installations i ON i.id = p.installation_id
			 WHERE d.id::text = $1
			 LIMIT 1
		`, deploymentID).Scan(&login)
	} else {
		err = s.pool.QueryRow(ctx, `
			SELECT i.account_login
			  FROM deployments d
			  JOIN projects p      ON p.id = d.project_id
			  JOIN installations i ON i.id = p.installation_id
			 WHERE d.id::text = $1
			   AND p.owner_user_id = $2
			 LIMIT 1
		`, deploymentID, userID).Scan(&login)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return login, nil
}

// --- users + sessions (Slice A) -----------------------------------------

// sessionUser is the materialised view of a user attached to a request.
// It carries enough to answer 'who am I' without an extra round-trip.
type sessionUser struct {
	ID           string
	GitHubUserID int64
	GitHubLogin  string
	Email        string
	AvatarURL    string
	IsAdmin      bool
}

type userUpsert struct {
	GitHubUserID int64
	Login        string
	Email        string
	AvatarURL    string
	OAuthToken   string
	OAuthExpires time.Time
}

// UpsertUserFromGitHub creates or refreshes the user row keyed by their
// GitHub user ID. Returns the internal UUID. Login can change on GitHub —
// we always keep our copy aligned.
func (s *store) UpsertUserFromGitHub(ctx context.Context, in userUpsert) (string, error) {
	var id string
	var expires any
	if !in.OAuthExpires.IsZero() {
		expires = in.OAuthExpires
	}
	err := s.pool.QueryRow(ctx, `
		INSERT INTO users (github_user_id, github_login, email, avatar_url,
		                   oauth_token, oauth_expires_at, last_login_at)
		VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), $6, now())
		ON CONFLICT (github_user_id) DO UPDATE
		   SET github_login     = EXCLUDED.github_login,
		       email            = COALESCE(EXCLUDED.email, users.email),
		       avatar_url       = COALESCE(EXCLUDED.avatar_url, users.avatar_url),
		       oauth_token      = COALESCE(EXCLUDED.oauth_token, users.oauth_token),
		       oauth_expires_at = COALESCE(EXCLUDED.oauth_expires_at, users.oauth_expires_at),
		       last_login_at    = now()
		RETURNING id::text
	`, in.GitHubUserID, in.Login, in.Email, in.AvatarURL, in.OAuthToken, expires).Scan(&id)
	if err != nil {
		return "", err
	}
	return id, nil
}

type sessionRow struct {
	TokenHash []byte
	UserID    string
	UserAgent string
	IP        string
	ExpiresAt time.Time
}

func (s *store) CreateSession(ctx context.Context, in sessionRow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO sessions (token_hash, user_id, user_agent, ip, expires_at)
		VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5)
	`, in.TokenHash, in.UserID, in.UserAgent, in.IP, in.ExpiresAt)
	return err
}

// LookupSession returns the user behind a token hash, or (nil, nil) if no
// matching active session exists. Expired sessions are excluded but not
// reaped here — a periodic sweep can do that.
func (s *store) LookupSession(ctx context.Context, tokenHash []byte) (*sessionUser, error) {
	u := &sessionUser{}
	err := s.pool.QueryRow(ctx, `
		SELECT u.id::text, u.github_user_id, u.github_login,
		       COALESCE(u.email, ''), COALESCE(u.avatar_url, ''),
		       u.is_admin
		  FROM sessions s
		  JOIN users u ON u.id = s.user_id
		 WHERE s.token_hash = $1
		   AND s.expires_at > now()
	`, tokenHash).Scan(&u.ID, &u.GitHubUserID, &u.GitHubLogin, &u.Email, &u.AvatarURL, &u.IsAdmin)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return u, nil
}

func (s *store) DeleteSession(ctx context.Context, tokenHash []byte) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM sessions WHERE token_hash = $1`, tokenHash)
	return err
}

// ClaimInstallationsForUser links any unowned installations whose
// GitHub account_id matches this user's github_user_id, AND propagates
// the ownership to all projects under those installations. Returns the
// number of installations claimed. Idempotent.
func (s *store) ClaimInstallationsForUser(ctx context.Context, userID string, githubAccountID int64) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE installations
		   SET owner_user_id = $1, updated_at = now()
		 WHERE account_id = $2
		   AND owner_user_id IS DISTINCT FROM $1
	`, userID, githubAccountID)
	if err != nil {
		return 0, err
	}
	// Backfill any projects that were enqueued before this user logged in.
	if _, err := tx.Exec(ctx, `
		UPDATE projects p
		   SET owner_user_id = $1
		  FROM installations i
		 WHERE p.installation_id = i.id
		   AND i.owner_user_id = $1
		   AND p.owner_user_id IS DISTINCT FROM $1
	`, userID); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// LinkInstallationByGitHubAccount sets the installation's owner_user_id to
// whichever user has a matching github_user_id, if any. No-op if no such
// user exists or the installation is already linked correctly. Called from
// the installation.created webhook so an existing dashboard user
// immediately sees the new installation.
func (s *store) LinkInstallationByGitHubAccount(ctx context.Context, installationID, githubAccountID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE installations i
		   SET owner_user_id = u.id, updated_at = now()
		  FROM users u
		 WHERE i.id = $1
		   AND u.github_user_id = $2
		   AND i.owner_user_id IS DISTINCT FROM u.id
	`, installationID, githubAccountID)
	return err
}

// SetProjectOwnerByInstallation propagates an installation's current
// owner to all of its projects. Called from the push-webhook path so
// projects created lazily on push pick up the right owner.
func (s *store) SetProjectOwnerByInstallation(ctx context.Context, installationID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE projects p
		   SET owner_user_id = i.owner_user_id
		  FROM installations i
		 WHERE p.installation_id = i.id
		   AND i.id = $1
		   AND i.owner_user_id IS NOT NULL
		   AND p.owner_user_id IS DISTINCT FROM i.owner_user_id
	`, installationID)
	return err
}

// --- worker side: claim + transition deployments --------------------------

// claimedDeployment carries everything the builder needs in one fetch so
// the worker never has to round-trip back to projects/installation_repos.
type claimedDeployment struct {
	DeploymentID     string
	ProjectID        string
	Slug             string
	RepoFullName     string
	InstallationID   int64
	CommitSHA        string
	Ref              string
	ProductionBranch string
	// TenantLogin is the GitHub account name (user or org) that installed
	// the App. It's the source of truth for which tenant namespace this
	// deployment belongs in, even before the GitHub user has signed up
	// for our dashboard.
	TenantLogin string
}

// ClaimNextQueued atomically transitions the oldest 'queued' deployment to
// 'building' and returns it. FOR UPDATE SKIP LOCKED lets multiple control
// plane replicas coexist later without double-building. Returns (nil, nil)
// when the queue is empty.
func (s *store) ClaimNextQueued(ctx context.Context) (*claimedDeployment, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var (
		c              claimedDeployment
		installationID int64
		repoFullName   string
	)
	err = tx.QueryRow(ctx, `
		WITH next AS (
			SELECT id
			  FROM deployments
			 WHERE status = 'queued'
			 ORDER BY created_at
			 FOR UPDATE SKIP LOCKED
			 LIMIT 1
		)
		UPDATE deployments d
		   SET status = 'building',
		       build_started_at = now()
		  FROM next, projects p
		 WHERE d.id = next.id
		   AND p.id = d.project_id
		RETURNING d.id::text, d.project_id::text, p.slug, p.full_name,
		          p.installation_id, d.commit_sha, d.ref, p.production_branch,
		          (SELECT account_login FROM installations WHERE id = p.installation_id)
	`).Scan(&c.DeploymentID, &c.ProjectID, &c.Slug, &repoFullName,
		&installationID, &c.CommitSHA, &c.Ref, &c.ProductionBranch, &c.TenantLogin)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.RepoFullName = repoFullName
	c.InstallationID = installationID

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *store) MarkDeploying(ctx context.Context, deploymentID, image string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deployments
		   SET status = 'deploying',
		       build_ended_at = now(),
		       image = $2
		 WHERE id = $1::uuid
	`, deploymentID, image)
	return err
}

// SetProductionDeployment records which deployment the production-alias
// Traefik IngressRoute (prod-<slug>) now points at. Called from:
//   * builder.go runOne() after applying the alias on a fresh
//     production-branch build.
//   * promote_handlers.go handlePromoteDeployment() after an instant
//     rollback re-points the alias at an older commit.
// The dashboard reads this field on every /v1/projects load to pick
// the 'current' row in the deployments list — so a rollback shows
// up immediately in the UI even though the rolled-back commit is
// older than the topmost READY row.
func (s *store) SetProductionDeployment(ctx context.Context, projectID, deploymentID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE projects
		   SET production_deployment_id = $2::uuid
		 WHERE id = $1::uuid
	`, projectID, deploymentID)
	return err
}

func (s *store) MarkReady(ctx context.Context, deploymentID, url string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deployments
		   SET status = 'ready',
		       ready_at = now(),
		       url = $2
		 WHERE id = $1::uuid
	`, deploymentID, url)
	return err
}

type inFlightDeployment struct {
	ID        string
	Status    string
	StartedAt time.Time
}

// ListInFlightDeployments returns rows the build reconciler should watch.
func (s *store) ListInFlightDeployments(ctx context.Context) ([]inFlightDeployment, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, status, COALESCE(build_started_at, created_at)
		  FROM deployments
		 WHERE status IN ('building', 'deploying')
		 ORDER BY COALESCE(build_started_at, created_at)
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []inFlightDeployment
	for rows.Next() {
		var d inFlightDeployment
		if err := rows.Scan(&d.ID, &d.Status, &d.StartedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// RequeueDeployment resets a stuck in-flight row back to queued so the
// worker can retry. Returns whether a row was updated.
func (s *store) RequeueDeployment(ctx context.Context, deploymentID, reason string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE deployments
		   SET status = 'queued',
		       build_started_at = NULL,
		       build_ended_at = NULL,
		       error = $2
		 WHERE id = $1::uuid
		   AND status IN ('building', 'deploying')
	`, deploymentID, reason)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// RequeueStale requeues any deployment that's been in a non-terminal state
// longer than maxAge. Called periodically by the worker so a crashed or
// terminated worker (rollout race, OOM, etc.) can't permanently strand a row.
func (s *store) RequeueStale(ctx context.Context, maxAge time.Duration) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE deployments
		   SET status = 'queued',
		       build_started_at = NULL,
		       build_ended_at = NULL,
		       error = NULL
		 WHERE status IN ('building', 'deploying')
		   AND COALESCE(build_started_at, created_at) < now() - $1::interval
	`, fmt.Sprintf("%d seconds", int(maxAge.Seconds())))
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// RequeueStuck resets non-terminal deployments to 'queued' so a fresh
// worker picks them up on boot. The build path is idempotent: if a
// matching Kaniko Job is still running we attach instead of redoing it.
func (s *store) RequeueStuck(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE deployments
		   SET status = 'queued',
		       build_started_at = NULL,
		       build_ended_at = NULL,
		       error = NULL
		 WHERE status IN ('building', 'deploying')
	`)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *store) MarkFailed(ctx context.Context, deploymentID, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deployments
		   SET status = 'failed',
		       build_ended_at = COALESCE(build_ended_at, now()),
		       error = $2
		 WHERE id = $1::uuid
	`, deploymentID, reason)
	return err
}

// MarkFailedIfInFlight marks a deployment failed only if it is still
// building or deploying. The build reconciler may have already requeued
// the row when it deleted a stuck Kaniko Job.
func (s *store) MarkFailedIfInFlight(ctx context.Context, deploymentID, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deployments
		   SET status = 'failed',
		       build_ended_at = COALESCE(build_ended_at, now()),
		       error = $2
		 WHERE id = $1::uuid
		   AND status IN ('building', 'deploying')
	`, deploymentID, reason)
	return err
}

// ----- per-project databases (Slice C) -----

type projectDatabase struct {
	ID         string
	ProjectID  string
	DBName     string
	RoleName   string
	Password   string
	Host       string
	Port       int
	SecretName string
}

// GetProjectDatabaseForOwner returns the row for a project the caller owns
// (or NULL ownership filter if userID is the admin sentinel ""). Returns
// (nil, nil) when no DB has been provisioned yet — that is NOT an error.
func (s *store) GetProjectDatabaseForOwner(ctx context.Context, projectID, ownerUserID string) (*projectDatabase, error) {
	q := `
		SELECT pd.id::text, pd.project_id::text, pd.db_name, pd.role_name,
		       pd.password, pd.host, pd.port, pd.secret_name
		  FROM project_databases pd
		  JOIN projects p ON p.id = pd.project_id
		 WHERE pd.project_id = $1::uuid
		   AND ($2 = '' OR p.owner_user_id = $2::uuid)
	`
	row := s.pool.QueryRow(ctx, q, projectID, ownerUserID)
	var pd projectDatabase
	err := row.Scan(&pd.ID, &pd.ProjectID, &pd.DBName, &pd.RoleName,
		&pd.Password, &pd.Host, &pd.Port, &pd.SecretName)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &pd, nil
}

// GetProjectDatabase is the worker-side variant used by the builder. It does
// NOT check ownership — the worker is trusted server-side code.
func (s *store) GetProjectDatabase(ctx context.Context, projectID string) (*projectDatabase, error) {
	return s.GetProjectDatabaseForOwner(ctx, projectID, "")
}

// CreateProjectDatabase persists the metadata row. The actual CREATE ROLE /
// CREATE DATABASE happens in db_provisioner.go BEFORE this call so a row
// only ever exists when the underlying objects exist. Returns ErrAlreadyExists
// if the project already has a DB.
func (s *store) CreateProjectDatabase(ctx context.Context, pd projectDatabase) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO project_databases
			(project_id, db_name, role_name, password, host, port, secret_name)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)
	`, pd.ProjectID, pd.DBName, pd.RoleName, pd.Password, pd.Host, pd.Port, pd.SecretName)
	return err
}

// GetProjectForOwner returns minimal project info used by the DB-create
// endpoint to know the tenant namespace + slug for naming the K8s Secret.
type projectInfo struct {
	ID            string
	Slug          string
	TenantLogin   string
	OwnerUserID   string
}

// GetProjectDeleteMeta returns fields needed to tear down K8s + DB rows.
func (s *store) GetProjectDeleteMeta(ctx context.Context, projectID, ownerUserID string) (*projectDeleteMeta, error) {
	q := `
		SELECT p.id::text, p.slug, p.full_name, p.installation_id, p.repo_id,
		       COALESCE(i.account_login, '')
		  FROM projects p
		  LEFT JOIN installations i ON i.id = p.installation_id
		 WHERE p.id = $1::uuid
		   AND ($2 = '' OR p.owner_user_id = $2::uuid)
	`
	var m projectDeleteMeta
	if err := s.pool.QueryRow(ctx, q, projectID, ownerUserID).
		Scan(&m.ID, &m.Slug, &m.FullName, &m.InstallationID, &m.RepoID, &m.TenantLogin); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &m, nil
}

// DeleteProject removes a project row. Related rows CASCADE via FK.
func (s *store) DeleteProject(ctx context.Context, projectID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM projects WHERE id = $1::uuid`, projectID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// ListDeploymentIDsForProject returns every deployment UUID for build-secret cleanup.
func (s *store) ListDeploymentIDsForProject(ctx context.Context, projectID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id::text FROM deployments WHERE project_id = $1::uuid
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *store) GetProjectForOwner(ctx context.Context, projectID, ownerUserID string) (*projectInfo, error) {
	q := `
		SELECT p.id::text, p.slug, COALESCE(i.account_login, ''), COALESCE(p.owner_user_id::text, '')
		  FROM projects p
		  LEFT JOIN installations i ON i.id = p.installation_id
		 WHERE p.id = $1::uuid
		   AND ($2 = '' OR p.owner_user_id = $2::uuid)
	`
	row := s.pool.QueryRow(ctx, q, projectID, ownerUserID)
	var p projectInfo
	if err := row.Scan(&p.ID, &p.Slug, &p.TenantLogin, &p.OwnerUserID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &p, nil
}

// --- Project env vars (Slice "Env Vars UI") -----------------------------

// projectEnvVar is the dashboard-facing shape of a row. We return the
// plaintext value to authenticated owners; the UI is responsible for the
// click-to-reveal interaction.
type projectEnvVar struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	UpdatedAt string `json:"updated_at"`
}

// ReservedEnvVarNames lists keys the user is NOT allowed to set through
// the env-vars API. They're managed by other subsystems (e.g.
// DATABASE_URL is written from project_databases when a DB is bound).
var ReservedEnvVarNames = map[string]struct{}{
	"DATABASE_URL": {},
}

// ListProjectEnv returns all env vars for a project, alphabetically by
// name. Ownership is enforced by the caller, which always passes a
// project_id it has already authorised.
func (s *store) ListProjectEnv(ctx context.Context, projectID string) ([]projectEnvVar, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, value,
		       to_char(updated_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
		  FROM project_env_vars
		 WHERE project_id = $1::uuid
		 ORDER BY name
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []projectEnvVar{}
	for rows.Next() {
		var v projectEnvVar
		if err := rows.Scan(&v.Name, &v.Value, &v.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// MapProjectEnv is the build-worker variant: returns a flat
// {name: value} map ready for k8s.applySecret. Equivalent to ListProjectEnv
// minus the timestamps. Returns an empty (non-nil) map when no vars exist
// so the caller can use len() == 0 to skip Secret creation.
func (s *store) MapProjectEnv(ctx context.Context, projectID string) (map[string]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, value
		  FROM project_env_vars
		 WHERE project_id = $1::uuid
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

// UpsertProjectEnv writes one env var (insert or replace) and returns
// the row's updated_at so the caller can echo it back to the UI.
func (s *store) UpsertProjectEnv(ctx context.Context, projectID, name, value string) (string, error) {
	var updatedAt string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO project_env_vars (project_id, name, value, updated_at)
		VALUES ($1::uuid, $2, $3, now())
		ON CONFLICT (project_id, name) DO UPDATE
		   SET value      = EXCLUDED.value,
		       updated_at = now()
		RETURNING to_char(updated_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
	`, projectID, name, value).Scan(&updatedAt)
	return updatedAt, err
}

// DeleteProjectEnv removes one env var by (project_id, name). Returns
// true if a row was deleted, false if the key didn't exist (caller may
// translate to 404).
func (s *store) DeleteProjectEnv(ctx context.Context, projectID, name string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM project_env_vars
		 WHERE project_id = $1::uuid AND name = $2
	`, projectID, name)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// slugify turns "jamilshaikh07/paas-sample-hello" into
// "jamilshaikh07-paas-sample-hello" (URL-safe, lower-case, no double dashes).
var slugUnsafe = regexp.MustCompile(`[^a-z0-9-]+`)

func slugify(s string) string {
	s = strings.ToLower(s)
	s = slugUnsafe.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	return s
}

// --- Waitlist -------------------------------------------------------------

func (s *store) UpsertWaitlistSignup(ctx context.Context, email, useCase, name, college string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO waitlist_signups (email, use_case, name, college)
		VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''))
		ON CONFLICT ((lower(email))) DO UPDATE
		   SET use_case   = EXCLUDED.use_case,
		       name       = COALESCE(EXCLUDED.name, waitlist_signups.name),
		       college    = COALESCE(EXCLUDED.college, waitlist_signups.college),
		       updated_at = now()
	`, email, useCase, name, college)
	return err
}

func (s *store) ListWaitlistSignups(ctx context.Context) ([]waitlistSignupRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, email, use_case,
		       COALESCE(name, ''), COALESCE(college, ''),
		       created_at, updated_at
		  FROM waitlist_signups
		 ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []waitlistSignupRow
	for rows.Next() {
		var r waitlistSignupRow
		var created, updated time.Time
		if err := rows.Scan(&r.ID, &r.Email, &r.UseCase, &r.Name, &r.College, &created, &updated); err != nil {
			return nil, err
		}
		r.CreatedAt = created.UTC().Format(time.RFC3339)
		r.UpdatedAt = updated.UTC().Format(time.RFC3339)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *store) CountProjectsForUser(ctx context.Context, userID string) (int, error) {
	if userID == "" {
		return 0, nil
	}
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)::int FROM projects WHERE owner_user_id = $1::uuid
	`, userID).Scan(&n)
	return n, err
}

func (s *store) UpdateProjectDisplayName(ctx context.Context, projectID, name string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE projects SET display_name = NULLIF($2, '') WHERE id = $1::uuid
	`, projectID, name)
	return err
}

// orphanRepo is an installation_repos row with no matching projects row.
type orphanRepo struct {
	InstallationID int64
	RepoID         int64
	FullName       string
	DefaultBranch  string
}

// ReconcileOrphanRepos creates project rows for repos the user explicitly
// added to the GitHub App but that were skipped when MAX_PROJECTS_PER_USER
// blocked AddRepos. Ignores the cap — the user chose these repos.
func (s *store) ReconcileOrphanRepos(ctx context.Context) ([]orphanRepo, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ir.installation_id, ir.repo_id, ir.full_name,
		       COALESCE(ir.default_branch, '')
		  FROM installation_repos ir
		  LEFT JOIN projects p
		    ON p.installation_id = ir.installation_id AND p.repo_id = ir.repo_id
		 WHERE ir.removed_at IS NULL AND p.id IS NULL
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var created []orphanRepo
	for rows.Next() {
		var o orphanRepo
		if err := rows.Scan(&o.InstallationID, &o.RepoID, &o.FullName, &o.DefaultBranch); err != nil {
			return nil, err
		}
		branch := o.DefaultBranch
		if branch == "" {
			branch = "main"
		}
		tag, err := s.pool.Exec(ctx, `
			INSERT INTO projects (installation_id, repo_id, full_name, slug,
			                     production_branch, owner_user_id)
			SELECT $1, $2, $3, $4, $5, i.owner_user_id
			  FROM installations i WHERE i.id = $1
			ON CONFLICT (installation_id, repo_id) DO NOTHING
		`, o.InstallationID, o.RepoID, o.FullName, slugify(o.FullName), branch)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() > 0 {
			created = append(created, o)
		}
	}
	return created, rows.Err()
}
