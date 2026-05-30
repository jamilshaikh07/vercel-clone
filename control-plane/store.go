package main

import (
	"context"
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

func (s *store) AddRepos(ctx context.Context, installationID int64, repos []repoRow) error {
	if len(repos) == 0 {
		return nil
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

		// Lazy-create a project per repo so first push doesn't have to do it.
		// Pick up the installation's current owner so the project is visible
		// to its rightful user from minute zero. NULL owner is fine and gets
		// fixed up on the user's first login via ClaimInstallationsForUser.
		batch.Queue(`
			INSERT INTO projects (installation_id, repo_id, full_name, slug,
			                     production_branch, owner_user_id)
			SELECT $1, $2, $3, $4, COALESCE(NULLIF($5, ''), 'main'), i.owner_user_id
			  FROM installations i
			 WHERE i.id = $1
			ON CONFLICT (installation_id, repo_id) DO NOTHING
		`, installationID, r.ID, r.FullName, slugify(r.FullName), r.DefaultBranch)
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
}

func (s *store) EnqueueDeployment(
	ctx context.Context,
	installationID, repoID int64,
	commitSHA, ref, deliveryID string,
) (*deploymentEnqueued, error) {
	var (
		projectID string
		slug      string
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id::text, slug
		  FROM projects
		 WHERE installation_id = $1 AND repo_id = $2
	`, installationID, repoID).Scan(&projectID, &slug)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup project: %w", err)
	}

	var deploymentID string
	if err := s.pool.QueryRow(ctx, `
		INSERT INTO deployments (project_id, commit_sha, ref, triggered_by, delivery_id)
		VALUES ($1::uuid, $2, $3, 'webhook', NULLIF($4, ''))
		RETURNING id::text
	`, projectID, commitSHA, ref, deliveryID).Scan(&deploymentID); err != nil {
		return nil, fmt.Errorf("insert deployment: %w", err)
	}
	return &deploymentEnqueued{ProjectID: projectID, DeploymentID: deploymentID, Slug: slug}, nil
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
	ID               string `json:"id"`
	Slug             string `json:"slug"`
	FullName         string `json:"full_name"`
	ProductionBranch string `json:"production_branch"`
	CreatedAt        string `json:"created_at"`
}

// ListProjectsForUser returns the projects owned by one user. Pass an
// empty userID to list everything (admin/dev path only — caller is
// responsible for the gate).
func (s *store) ListProjectsForUser(ctx context.Context, userID string) ([]projectRow, error) {
	var rows pgx.Rows
	var err error
	if userID == "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id::text, slug, full_name, production_branch,
			       to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
			  FROM projects
			 ORDER BY created_at DESC
		`)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id::text, slug, full_name, production_branch,
			       to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
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
		if err := rows.Scan(&p.ID, &p.Slug, &p.FullName, &p.ProductionBranch, &p.CreatedAt); err != nil {
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
		          p.installation_id, d.commit_sha, d.ref, p.production_branch
	`).Scan(&c.DeploymentID, &c.ProjectID, &c.Slug, &repoFullName,
		&installationID, &c.CommitSHA, &c.Ref, &c.ProductionBranch)
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
