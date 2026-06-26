package main

// Store layer for custom project domains.
//
// Kept in its own file (alongside env_store / database_store etc) so the
// domains feature is self-contained and easy to reason about.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type projectDomain struct {
	ProjectID         string     `json:"project_id"`
	Hostname          string     `json:"hostname"`
	VerificationToken string     `json:"verification_token"`
	VerifiedAt        *time.Time `json:"verified_at,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
}

// generateDomainToken returns a hex string used by the user as the value
// of the verification TXT record. 24 bytes → 48 hex chars; small enough
// to publish but plenty of entropy to make brute force pointless.
func generateDomainToken() (string, error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "paas-verify-" + hex.EncodeToString(b[:]), nil
}

// normaliseHostname lower-cases + trims. The DB's CHECK constraint is
// case-insensitive only because we always pass lower-case through; the
// app layer is the source of truth for canonical form.
func normaliseHostname(h string) string {
	return strings.ToLower(strings.TrimSpace(h))
}

// ListProjectDomains returns every custom domain registered for a project,
// most-recently-added first. Caller is responsible for ownership checks.
func (s *store) ListProjectDomains(ctx context.Context, projectID string) ([]projectDomain, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id::text, hostname, verification_token, verified_at, created_at
		  FROM project_domains
		 WHERE project_id = $1::uuid
		 ORDER BY created_at DESC
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []projectDomain
	for rows.Next() {
		var d projectDomain
		if err := rows.Scan(&d.ProjectID, &d.Hostname, &d.VerificationToken,
			&d.VerifiedAt, &d.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// AddProjectDomain inserts a new (unverified) custom domain row. Returns
// the row including the freshly-minted verification token. Conflicts on
// the hostname PK are surfaced as a typed error so the handler can
// return a clean 409.
var errDomainAlreadyExists = errors.New("hostname already registered")

func (s *store) AddProjectDomain(ctx context.Context, projectID, hostname string) (*projectDomain, error) {
	hostname = normaliseHostname(hostname)
	token, err := generateDomainToken()
	if err != nil {
		return nil, fmt.Errorf("mint token: %w", err)
	}
	var d projectDomain
	err = s.pool.QueryRow(ctx, `
		INSERT INTO project_domains (project_id, hostname, verification_token)
		VALUES ($1::uuid, $2, $3)
		RETURNING project_id::text, hostname, verification_token, verified_at, created_at
	`, projectID, hostname, token).Scan(
		&d.ProjectID, &d.Hostname, &d.VerificationToken, &d.VerifiedAt, &d.CreatedAt,
	)
	if err != nil {
		// 23505 = unique_violation (PG SQLSTATE).
		if isUniqueViolation(err) {
			return nil, errDomainAlreadyExists
		}
		return nil, err
	}
	return &d, nil
}

// MarkDomainVerified stamps verified_at = now() on a single row, scoped
// by project_id to prevent confused-deputy verification of another
// project's domain (the user-facing handler also re-checks ownership).
// Returns (false, nil) when no row matched — caller treats that as 404.
func (s *store) MarkDomainVerified(ctx context.Context, projectID, hostname string) (bool, error) {
	hostname = normaliseHostname(hostname)
	tag, err := s.pool.Exec(ctx, `
		UPDATE project_domains
		   SET verified_at = now()
		 WHERE project_id = $1::uuid
		   AND hostname = $2
		   AND verified_at IS NULL
	`, projectID, hostname)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// DeleteProjectDomain removes a row. Returns (false, nil) when no row
// matched — used by the handler for 404 semantics. The Traefik
// IngressRoute it created (if any) is cleaned up separately by the
// caller; the DB row is the source of truth either way.
func (s *store) DeleteProjectDomain(ctx context.Context, projectID, hostname string) (bool, error) {
	hostname = normaliseHostname(hostname)
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM project_domains
		 WHERE project_id = $1::uuid
		   AND hostname = $2
	`, projectID, hostname)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// GetProjectDomain returns one domain row by (project_id, hostname).
// nil + nil = not found, so handlers can branch cleanly on the value.
func (s *store) GetProjectDomain(ctx context.Context, projectID, hostname string) (*projectDomain, error) {
	hostname = normaliseHostname(hostname)
	var d projectDomain
	err := s.pool.QueryRow(ctx, `
		SELECT project_id::text, hostname, verification_token, verified_at, created_at
		  FROM project_domains
		 WHERE project_id = $1::uuid
		   AND hostname = $2
	`, projectID, hostname).Scan(
		&d.ProjectID, &d.Hostname, &d.VerificationToken, &d.VerifiedAt, &d.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// ListVerifiedDomainsForProject returns just the verified hostnames for
// a project. Used by the builder on every production deploy to re-apply
// the IngressRoutes that target the new Service.
func (s *store) ListVerifiedDomainsForProject(ctx context.Context, projectID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT hostname
		  FROM project_domains
		 WHERE project_id = $1::uuid
		   AND verified_at IS NOT NULL
		 ORDER BY created_at
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// ListVerifiedDomainsForProjects returns verified hostnames keyed by project ID.
func (s *store) ListVerifiedDomainsForProjects(ctx context.Context, projectIDs []string) (map[string][]string, error) {
	out := make(map[string][]string, len(projectIDs))
	if len(projectIDs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT project_id::text, hostname
		  FROM project_domains
		 WHERE project_id = ANY($1::uuid[])
		   AND verified_at IS NOT NULL
		 ORDER BY created_at
	`, projectIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var pid, host string
		if err := rows.Scan(&pid, &host); err != nil {
			return nil, err
		}
		out[pid] = append(out[pid], host)
	}
	return out, rows.Err()
}

// isUniqueViolation pattern-matches the error string for SQLSTATE 23505.
// pgx exposes a structured *pgconn.PgError but we keep this lightweight
// and string-based to avoid importing a second pg dep purely for one
// error class. Good enough — the message format is documented.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "SQLSTATE 23505") || strings.Contains(s, "unique constraint")
}
