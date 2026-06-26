package main

// GET /v1/home/summary — cluster rollup for the enterprise home page.

import (
	"context"
	"net/http"
	"time"
)

type homeSummaryResponse struct {
	Apps            int     `json:"apps"`
	LiveApps        int     `json:"live_apps"`
	TotalDeploys    int     `json:"total_deployments"`
	BuildsToday     int     `json:"builds_today"`
	Failed24h       int     `json:"failed_24h"`
	SuccessRatePct  float64 `json:"success_rate_pct"`
	AvgBuildSec     *int    `json:"avg_build_sec,omitempty"`
	TotalRequests   int64   `json:"total_requests"`
	TotalPods       int     `json:"total_pods"`
	AsOf            string  `json:"as_of"`
}

func (s *server) handleHomeSummary(w http.ResponseWriter, r *http.Request) {
	u := userFromCtx(r.Context())
	if u == nil {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	projects, err := s.store.ListProjectsForUser(ctx, u.ID)
	if err != nil {
		s.log.Error("home summary list projects failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	ids := make([]string, 0, len(projects))
	for _, p := range projects {
		ids = append(ids, p.ID)
	}
	depsByProject, err := s.store.ListDeploymentsForProjects(ctx, ids, 10)
	if err != nil {
		s.log.Error("home summary list deployments failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	stats, err := s.store.DeploymentStatsForUser(ctx, u.ID)
	if err != nil {
		s.log.Error("home summary deployment stats failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	live := 0
	totalDeploys := 0
	for _, p := range projects {
		deps := depsByProject[p.ID]
		totalDeploys += len(deps)
		for _, d := range deps {
			if d.Status == "ready" {
				live++
				break
			}
		}
	}

	var telem struct {
		TotalRequests float64
		TotalPods     int
	}
	if roll, err := s.telemetryRollupForUser(ctx, u.ID); err == nil {
		telem.TotalRequests = roll.TotalReqs
		telem.TotalPods = roll.TotalPods
	}

	writeJSON(w, http.StatusOK, homeSummaryResponse{
		Apps:           len(projects),
		LiveApps:       live,
		TotalDeploys:   totalDeploys,
		BuildsToday:    stats.BuildsToday,
		Failed24h:      stats.Failed24h,
		SuccessRatePct: stats.SuccessRatePct,
		AvgBuildSec:    stats.AvgBuildSec,
		TotalRequests:  int64(telem.TotalRequests),
		TotalPods:      telem.TotalPods,
		AsOf:           time.Now().UTC().Format(time.RFC3339),
	})
}
