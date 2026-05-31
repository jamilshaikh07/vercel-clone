package main

// telemetry_handlers.go: dashboard-facing JSON endpoints that shape the
// raw TelemetrySnapshot into per-app rollups the UI can render directly.
//
//   GET /v1/telemetry
//     Cluster-wide: one row per user-owned project (admin sees all),
//     with CPU / memory / request totals + recent-build counts.
//
//   GET /v1/projects/{id}/telemetry
//     Per-project drill-down: each running pod's CPU/mem, plus the
//     Traefik service traffic associated with that project.
//
// Both endpoints share a single TelemetrySnapshot via the cache, so a
// dashboard refresh that loads both Global Telemetry and Per-App
// Telemetry still only triggers ONE metrics-server hit + ONE Traefik
// scrape per 5-second window.

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
)

// appTelemetry is the dashboard-facing per-project row.
type appTelemetry struct {
	ProjectID string `json:"project_id"`
	Slug      string `json:"slug"`
	Pods      int    `json:"pods"`
	CPUm      int64  `json:"cpu_m"`
	MemoryMi  int64  `json:"memory_mi"`
	Requests  float64 `json:"requests"`
	Errors    float64 `json:"errors"`
	// AvgLatencyMs is best-effort: requires both _sum and _count from
	// Traefik. Zero means "no data yet" rather than "0ms".
	AvgLatencyMs int64 `json:"avg_latency_ms"`
}

type globalTelemetryResponse struct {
	AsOf         string         `json:"as_of"`
	Degraded     string         `json:"degraded,omitempty"`
	TotalCPUm    int64          `json:"total_cpu_m"`
	TotalMemMi   int64          `json:"total_memory_mi"`
	TotalPods    int            `json:"total_pods"`
	TotalReqs    float64        `json:"total_requests"`
	Apps         []appTelemetry `json:"apps"`
}

func (s *server) handleGlobalTelemetry(w http.ResponseWriter, r *http.Request) {
	u := userFromCtx(r.Context())
	if u == nil {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	scope := u.ID
	if u.IsAdmin {
		scope = ""
	}

	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()

	// List the user's projects so we can map tenant namespaces → slug
	// and filter the snapshot to apps this user owns.
	rows, err := s.store.ListProjectsWithTenant(ctx, scope)
	if err != nil {
		s.log.Error("telemetry: list projects failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	snap := s.tcache.get(ctx, s.k8s)

	out := globalTelemetryResponse{
		AsOf:     snap.AsOf.UTC().Format(time.RFC3339),
		Degraded: snap.Err,
	}
	for _, p := range rows {
		ns := "paas-tenant-" + p.TenantLogin
		app := appTelemetry{
			ProjectID: p.ID,
			Slug:      p.Slug,
		}
		for _, pod := range snap.Pods[ns] {
			// Match pods that belong to THIS project by slug prefix
			// (deploy names are `<slug>-<sha7>`).
			if !strings.HasPrefix(pod.Name, p.Slug+"-") {
				continue
			}
			app.Pods++
			app.CPUm += pod.CPUm
			app.MemoryMi += pod.MemoryMi
		}
		// Aggregate Traefik counters matching this project's services.
		// IngressRoutes name their backing services after the deployment
		// (`<deployName>-svc@kubernetescrd`), so the slug appears as a
		// substring. Aggregation across deployments is exactly what we
		// want for an app-level rollup.
		var durSum, durCnt float64
		for svcName, t := range snap.Services {
			if !strings.Contains(svcName, p.Slug) {
				continue
			}
			app.Requests += t.Requests
			app.Errors += t.Errors
			durSum += t.Duration
			durCnt += t.Count
		}
		if durCnt > 0 {
			app.AvgLatencyMs = int64((durSum / durCnt) * 1000)
		}

		out.Apps = append(out.Apps, app)
		out.TotalCPUm += app.CPUm
		out.TotalMemMi += app.MemoryMi
		out.TotalPods += app.Pods
		out.TotalReqs += app.Requests
	}
	// Most active apps first.
	sort.Slice(out.Apps, func(i, j int) bool {
		if out.Apps[i].Requests != out.Apps[j].Requests {
			return out.Apps[i].Requests > out.Apps[j].Requests
		}
		return out.Apps[i].CPUm > out.Apps[j].CPUm
	})

	writeJSON(w, http.StatusOK, out)
}

type projectTelemetryResponse struct {
	AsOf         string      `json:"as_of"`
	Degraded     string      `json:"degraded,omitempty"`
	ProjectID    string      `json:"project_id"`
	Slug         string      `json:"slug"`
	Pods         []PodMetric `json:"pods"`
	TotalCPUm    int64       `json:"total_cpu_m"`
	TotalMemMi   int64       `json:"total_memory_mi"`
	Requests     float64     `json:"requests"`
	Errors       float64     `json:"errors"`
	AvgLatencyMs int64       `json:"avg_latency_ms"`
	// ByClass holds the lifetime cumulative count per HTTP status class
	// ("2xx","3xx","4xx","5xx"). Lets the dashboard donut show true
	// lifetime ratios instead of summing the rolling 60-min window.
	ByClass map[string]float64 `json:"by_class,omitempty"`
}

func (s *server) handleProjectTelemetry(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()

	snap := s.tcache.get(ctx, s.k8s)
	out := projectTelemetryResponse{
		AsOf:      snap.AsOf.UTC().Format(time.RFC3339),
		Degraded:  snap.Err,
		ProjectID: proj.ID,
		Slug:      proj.Slug,
	}
	ns := "paas-tenant-" + proj.TenantLogin
	for _, pod := range snap.Pods[ns] {
		if !strings.HasPrefix(pod.Name, proj.Slug+"-") {
			continue
		}
		out.Pods = append(out.Pods, pod)
		out.TotalCPUm += pod.CPUm
		out.TotalMemMi += pod.MemoryMi
	}
	var durSum, durCnt float64
	byClass := map[string]float64{}
	for svcName, t := range snap.Services {
		if !strings.Contains(svcName, proj.Slug) {
			continue
		}
		out.Requests += t.Requests
		out.Errors += t.Errors
		durSum += t.Duration
		durCnt += t.Count
		for cls, v := range t.ByClass {
			byClass[cls] += v
		}
	}
	if durCnt > 0 {
		out.AvgLatencyMs = int64((durSum / durCnt) * 1000)
	}
	if len(byClass) > 0 {
		out.ByClass = byClass
	}
	writeJSON(w, http.StatusOK, out)
}

// handleProjectSeries returns the per-project HTTP-status-code time
// series — a stream of {t, counts{class:count}} points captured by the
// sampler goroutine on every 30s tick.
//
// The response is small (< 10KB for a fully-populated hour) and the
// caller polls it on the per-app traffic page. We don't paginate or
// cache here; the seriesStore.RWMutex makes the read effectively free.
type seriesResponse struct {
	ProjectID  string        `json:"project_id"`
	Slug       string        `json:"slug"`
	IntervalS  int           `json:"interval_s"`
	Points     []seriesPoint `json:"points"`
}

func (s *server) handleProjectSeries(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	pts := s.series.pointsForServices(matcherForProject(proj.Slug))
	writeJSON(w, http.StatusOK, seriesResponse{
		ProjectID: proj.ID,
		Slug:      proj.Slug,
		IntervalS: int(seriesInterval / 1e9),
		Points:    pts,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
