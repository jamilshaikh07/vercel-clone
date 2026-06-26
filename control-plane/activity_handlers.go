package main

// GET /v1/activity — cross-app activity feed for the enterprise dashboard.
// Merges recent deployments and webhook deliveries scoped to the user's projects.

import (
	"context"
	"net/http"
	"time"
)

type activityItem struct {
	Kind        string  `json:"kind"` // deployment | webhook
	At          string  `json:"at"`
	ProjectID   string  `json:"project_id,omitempty"`
	ProjectSlug string  `json:"project_slug,omitempty"`
	ProjectName string  `json:"project_name,omitempty"`
	Title       string  `json:"title"`
	Detail      string  `json:"detail,omitempty"`
	Status      string  `json:"status,omitempty"`
	Ref         string  `json:"ref,omitempty"`
	CommitSHA   string  `json:"commit_sha,omitempty"`
	DeploymentID string `json:"deployment_id,omitempty"`
	Event       string  `json:"event,omitempty"`
	Action      string  `json:"action,omitempty"`
	IsPreview   bool    `json:"is_preview,omitempty"`
}

func (s *server) handleActivity(w http.ResponseWriter, r *http.Request) {
	u := userFromCtx(r.Context())
	if u == nil {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	limit := 40
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	items, err := s.store.ListActivityForUser(ctx, u.ID, limit)
	if err != nil {
		s.log.Error("list activity failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"as_of": time.Now().UTC().Format(time.RFC3339),
	})
}
