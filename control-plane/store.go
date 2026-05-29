package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

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
		batch.Queue(`
			INSERT INTO projects (installation_id, repo_id, full_name, slug, production_branch)
			VALUES ($1, $2, $3, $4, COALESCE(NULLIF($5, ''), 'main'))
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
