package main

// deploy_handlers.go: dashboard-initiated bootstrap deploys.
//
// POST /v1/projects/{id}/deploy is the "Deploy now" button: it asks
// GitHub for the HEAD SHA of the project's production_branch and
// enqueues a build for it. The endpoint is idempotent at the webhook
// level (the builder dedupes by (project, sha) already isn't a goal
// — clicking twice will produce two deployment rows for the same
// commit, the second of which will short-circuit to ready once the
// first publishes its image; that mirrors the existing "redeploy"
// behaviour and keeps the audit trail honest).
//
// Distinct from POST /v1/deployments/{id}/redeploy (which clones an
// existing deployment row) because there's no source row to clone
// when the project has zero deployments — the very thing that makes
// this endpoint necessary.

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

func (s *server) handleDeployNow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidRE.MatchString(id) {
		http.Error(w, "invalid project id", http.StatusBadRequest)
		return
	}
	u := userFromCtx(r.Context())
	if u == nil {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	// Admin sentinel "" lets the lookup skip the owner filter, matching
	// the pattern used everywhere else in the project handlers.
	scope := u.ID
	if u.IsAdmin {
		scope = ""
	}

	// Slightly longer timeout than redeploy because we have to do a
	// GitHub round-trip to resolve HEAD before we can write the row.
	// 8s covers the worst-case GitHub p99 of ~2s plus our own write.
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()

	proj, err := s.store.GetProjectForDeploy(ctx, id, scope)
	if err != nil {
		s.log.Error("deploy-now lookup failed", "project_id", id, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if proj == nil {
		// 404 for "doesn't exist" *and* "not your project" — same as
		// every other tenant endpoint, no enumeration via timing.
		http.NotFound(w, r)
		return
	}

	branch := strings.TrimSpace(proj.ProductionBranch)
	if branch == "" {
		branch = "main"
	}

	// Self-heal: projects created from the installation_repositories
	// webhook get a hardcoded "main" production_branch because that
	// webhook payload omits default_branch. When that turns out to be
	// wrong (e.g. the repo defaults to "master"), the first fetch
	// returns 404 — at which point we ask GitHub for the real default,
	// persist it, and retry once. After this the project row reflects
	// reality and subsequent deploys hit the fast path.
	sha, err := s.gh.fetchBranchSHA(ctx, proj.InstallationID, proj.RepoFullName, branch)
	if err != nil && strings.Contains(err.Error(), "HTTP 404") {
		if def, derr := s.gh.fetchRepoDefaultBranch(ctx, proj.InstallationID, proj.RepoFullName); derr == nil && def != branch {
			s.log.Info("deploy-now: production_branch corrected from API",
				"project_id", id, "repo", proj.RepoFullName, "old", branch, "new", def)
			if uerr := s.store.UpdateProjectProductionBranch(ctx, id, def); uerr != nil {
				s.log.Warn("deploy-now: update production_branch failed",
					"project_id", id, "err", uerr)
			}
			branch = def
			sha, err = s.gh.fetchBranchSHA(ctx, proj.InstallationID, proj.RepoFullName, branch)
		}
	}
	if err != nil {
		// 422 (not 502): Cloudflare's "Always Show Origin Error Page"
		// is off by default, so 5xx responses get replaced with CF's
		// own HTML — which means the dashboard never sees our useful
		// message. 422 ("Unprocessable Entity") is passed through and
		// is semantically correct: the request was well-formed but
		// referred to a branch GitHub doesn't have.
		s.log.Warn("deploy-now: fetch HEAD sha failed",
			"project_id", id, "repo", proj.RepoFullName, "branch", branch, "err", err)
		http.Error(w, "couldn't resolve branch HEAD on GitHub — does the branch exist and is the App still installed?", http.StatusUnprocessableEntity)
		return
	}

	res, err := s.store.EnqueueDeployment(ctx,
		proj.InstallationID, proj.RepoID, sha, "refs/heads/"+branch, "", "manual")
	if err != nil {
		s.log.Error("deploy-now enqueue failed", "project_id", id, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if res == nil {
		// GetProjectForDeploy just succeeded, so the project must
		// exist; the only way EnqueueDeployment returns nil here is a
		// race with the project being deleted concurrently. Treat as
		// 404 for the caller — same surface they'd see if they'd
		// hit this with a stale ID.
		http.NotFound(w, r)
		return
	}

	s.log.Info("deploy-now queued",
		"deployment_id", res.DeploymentID,
		"project_id", res.ProjectID,
		"slug", res.Slug,
		"repo", proj.RepoFullName,
		"branch", branch,
		"sha", sha,
		"user", u.GitHubLogin,
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"deployment_id": res.DeploymentID,
		"commit_sha":    sha,
		"branch":        branch,
		"status":        "queued",
	})
}
