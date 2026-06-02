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

	sha, err := s.gh.fetchBranchSHA(ctx, proj.InstallationID, proj.RepoFullName, branch)
	if err != nil {
		// 502: we couldn't reach GitHub or the branch doesn't exist.
		// Surface a short, actionable message — the dashboard will
		// show this to the user in a toast.
		s.log.Warn("deploy-now: fetch HEAD sha failed",
			"project_id", id, "repo", proj.RepoFullName, "branch", branch, "err", err)
		http.Error(w, "couldn't resolve branch HEAD on GitHub — does the branch exist and is the App still installed?", http.StatusBadGateway)
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
