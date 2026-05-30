package main

// Dashboard HTTP surface.
//
// We deliberately do NOT ship a separate SPA. The dashboard is a single
// embedded HTML file served from the control plane itself at
// paas.jamilshaikh.in/, talking to a small REST surface on the same origin:
//
//   GET /v1/projects           — projects with their 10 most recent deployments
//   GET /v1/deployments        — flat list, 50 most recent (debugging)
//   GET /v1/deployments/{id}/logs — SSE stream (see logs.go)
//
// Same-origin keeps CORS out of the picture and lets EventSource work without
// any plumbing. When we eventually add auth, it lives in one place.

import (
	_ "embed"
	"context"
	"encoding/json"
	"net/http"
	"time"
)

//go:embed static/dashboard.html
var dashboardHTML []byte

// projectWithDeployments mirrors the JSON shape consumed by the dashboard JS.
type projectWithDeployments struct {
	projectRow
	Deployments []deploymentRow `json:"deployments"`
}

func (s *server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	// Only serve the dashboard on the root path — anything else under "/"
	// that isn't a registered route should 404.
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	_, _ = w.Write(dashboardHTML)
}

func (s *server) handleListProjects(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	projects, err := s.store.ListProjects(ctx)
	if err != nil {
		s.log.Error("list projects failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	ids := make([]string, 0, len(projects))
	for _, p := range projects {
		ids = append(ids, p.ID)
	}
	depsByProject, err := s.store.ListDeploymentsForProjects(ctx, ids, 10)
	if err != nil {
		s.log.Error("list deployments for projects failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	out := make([]projectWithDeployments, 0, len(projects))
	for _, p := range projects {
		out = append(out, projectWithDeployments{
			projectRow:  p,
			Deployments: depsByProject[p.ID],
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"projects": out})
}
