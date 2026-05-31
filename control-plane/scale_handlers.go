package main

// Start/Stop endpoint for tenant apps.
//
// "Stop" means: scale every Deployment that belongs to the project to 0
// replicas, terminating its pods. The Service + IngressRoute stay in
// place, so HTTP requests still hit Traefik — which then returns 503
// (no available endpoints). The user can flip the app back on at any
// time without re-pushing.
//
// "Start" means: scale every Deployment back up to 1 replica. We don't
// remember per-Deployment custom replica counts because (a) the only
// way to ever set replicas != 1 today is via this endpoint, and (b) we
// scale ALL deployments of the project, not just the production one.
// If/when we add HPAs or per-deployment scale, this gets revisited.
//
// State is intentionally NOT persisted to the DB. The "current state"
// is "what's actually in the cluster right now", read fresh on every
// status call. This means:
//   * A new git push after Stop will create a brand-new Deployment with
//     replicas=1 (the worker doesn't know to respect Stop). That's
//     considered correct: pushing code is an explicit intent to deploy.
//   * On control-plane restart we don't lose state — k8s holds it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
)

type scaleStateResponse struct {
	ProjectID   string `json:"project_id"`
	Slug        string `json:"slug"`
	Running     bool   `json:"running"`     // any deployment with replicas>0
	Replicas    int    `json:"replicas"`    // sum across all deployments
	Deployments int    `json:"deployments"` // total deployments seen
}

type scaleRequest struct {
	Action string `json:"action"` // "start" | "stop"
}

// handleProjectStatus returns the current running/stopped state of an
// app by reading spec.replicas off every Deployment owned by the
// project. Sum-replicas > 0 → Running; otherwise Stopped.
func (s *server) handleProjectStatus(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	state, err := s.readProjectScale(r.Context(), proj)
	if err != nil {
		s.log.Error("read project scale failed", "project_id", proj.ID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

// handleProjectScale flips an app on or off. We fan out one PUT-to-
// /scale per Deployment in parallel so a project with many per-commit
// Deployments doesn't take O(N) seconds to stop.
func (s *server) handleProjectScale(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	var req scaleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	var replicas int
	switch req.Action {
	case "start":
		replicas = 1
	case "stop":
		replicas = 0
	default:
		http.Error(w, "action must be 'start' or 'stop'", http.StatusBadRequest)
		return
	}
	if proj.TenantLogin == "" {
		http.Error(w, "project has no tenant namespace yet", http.StatusBadRequest)
		return
	}
	ns := tenantNamespaceFor(proj.TenantLogin)
	names, err := s.k8s.listDeployments(r.Context(), ns, "paas.project="+proj.Slug)
	if err != nil {
		s.log.Error("list deployments failed", "ns", ns, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if len(names) == 0 {
		http.Error(w, "no deployments exist for this project yet", http.StatusBadRequest)
		return
	}
	if err := s.fanoutScale(r.Context(), ns, names, replicas); err != nil {
		s.log.Error("fanout scale failed", "ns", ns, "err", err)
		http.Error(w, "scale failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.log.Info("scaled project",
		"project_id", proj.ID, "slug", proj.Slug,
		"action", req.Action, "deployments", len(names), "replicas", replicas)
	state, err := s.readProjectScale(r.Context(), proj)
	if err != nil {
		// Scale succeeded but read-back failed — return a best-effort
		// echo of the requested intent so the UI can update immediately.
		writeJSON(w, http.StatusOK, scaleStateResponse{
			ProjectID:   proj.ID,
			Slug:        proj.Slug,
			Running:     replicas > 0,
			Replicas:    replicas * len(names),
			Deployments: len(names),
		})
		return
	}
	writeJSON(w, http.StatusOK, state)
}

// readProjectScale lists every Deployment for the project and sums
// spec.replicas. A project with zero deployments returns Running=false,
// Replicas=0, Deployments=0 — the UI can use that to render a "not yet
// deployed" hint.
func (s *server) readProjectScale(ctx context.Context, proj *projectInfo) (*scaleStateResponse, error) {
	out := &scaleStateResponse{ProjectID: proj.ID, Slug: proj.Slug}
	if proj.TenantLogin == "" {
		return out, nil
	}
	ns := tenantNamespaceFor(proj.TenantLogin)
	names, err := s.k8s.listDeployments(ctx, ns, "paas.project="+proj.Slug)
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	out.Deployments = len(names)
	for _, name := range names {
		replicas, exists, err := s.k8s.getDeploymentReplicas(ctx, ns, name)
		if err != nil {
			return nil, fmt.Errorf("get %s/%s replicas: %w", ns, name, err)
		}
		if !exists {
			continue
		}
		out.Replicas += replicas
	}
	out.Running = out.Replicas > 0
	return out, nil
}

// fanoutScale issues replica patches to every Deployment in parallel.
// Errors are collected — we still try every Deployment so a single
// transient API blip doesn't leave the app half-on / half-off.
func (s *server) fanoutScale(ctx context.Context, namespace string, names []string, replicas int) error {
	if len(names) == 0 {
		return nil
	}
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, name := range names {
		name := name
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.k8s.patchDeploymentScale(ctx, namespace, name, replicas); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("%s: %w", name, err))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}
