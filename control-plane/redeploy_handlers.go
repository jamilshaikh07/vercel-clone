package main

// redeploy_handlers.go: dashboard-initiated rebuilds.
//
// POST /v1/deployments/{id}/redeploy re-enqueues a queued deployment row
// for the same (project, commit_sha, ref) as the source row. The build
// worker's poller picks it up within ~3s on its next tick.
//
// The single endpoint does double duty:
//   * Redeploy the *latest* commit  — typically after editing env vars,
//     no need to make a junk git commit just to redeliver Secrets.
//   * Redeploy an *older* commit    — effectively a rollback. Use the
//     dashboard UI to click an older READY deployment and hit Redeploy.
//
// We don't try to cancel an already in-flight build for the same project;
// the queue is FIFO and the new row will simply land after the current
// one finishes. This keeps the implementation small and avoids the
// surprise of "I clicked Redeploy and my failing build kept running".

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

func (s *server) handleRedeploy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidRE.MatchString(id) {
		http.Error(w, "invalid deployment id", http.StatusBadRequest)
		return
	}
	u := userFromCtx(r.Context())
	if u == nil {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	scope := u.ID
	if u.IsAdmin {
		scope = ""
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	newID, slug, err := s.store.EnqueueRedeploy(ctx, id, scope)
	if err != nil {
		s.log.Error("redeploy enqueue failed", "source_deployment_id", id, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if newID == "" {
		// Source row not found or not owned by this user; identical 404
		// for both so we don't leak which IDs exist.
		http.NotFound(w, r)
		return
	}

	s.log.Info("redeploy queued",
		"new_deployment_id", newID,
		"source_deployment_id", id,
		"slug", slug,
		"user", u.GitHubLogin,
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"deployment_id": newID,
		"status":        "queued",
	})
}
