package main

// In-memory uptime probe results from the synthetic monitor.

import (
	"context"
	"net/http"
	"sync"
	"time"
)

const uptimeHistoryLen = 48 // ~24 h at 30 min effective granularity (one slot per probe cycle)

type uptimeSample struct {
	At       time.Time `json:"at"`
	OK       bool      `json:"ok"`
	LatencyMs int      `json:"latency_ms"`
	Status   int       `json:"status,omitempty"`
}

type uptimeTracker struct {
	mu   sync.RWMutex
	byProject map[string][]uptimeSample // project_id → newest last
}

func newUptimeTracker() *uptimeTracker {
	return &uptimeTracker{byProject: make(map[string][]uptimeSample)}
}

func (t *uptimeTracker) record(projectID string, ok bool, latencyMs, status int) {
	if projectID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	hist := t.byProject[projectID]
	hist = append(hist, uptimeSample{
		At:        time.Now().UTC(),
		OK:        ok,
		LatencyMs: latencyMs,
		Status:    status,
	})
	if len(hist) > uptimeHistoryLen {
		hist = hist[len(hist)-uptimeHistoryLen:]
	}
	t.byProject[projectID] = hist
}

func (t *uptimeTracker) snapshot(projectID string) []uptimeSample {
	t.mu.RLock()
	defer t.mu.RUnlock()
	src := t.byProject[projectID]
	out := make([]uptimeSample, len(src))
	copy(out, src)
	return out
}

type uptimeProjectSummary struct {
	ProjectID   string         `json:"project_id"`
	Slug        string         `json:"slug"`
	UptimePct   float64        `json:"uptime_pct"`
	AvgLatencyMs int           `json:"avg_latency_ms"`
	LastOK      bool           `json:"last_ok"`
	LastChecked string         `json:"last_checked,omitempty"`
	Samples     []uptimeSample `json:"samples"`
}

func (s *server) handleUptime(w http.ResponseWriter, r *http.Request) {
	u := userFromCtx(r.Context())
	if u == nil {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	projects, err := s.store.ListProjectsForUser(ctx, u.ID)
	if err != nil {
		s.log.Error("uptime list projects failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	out := make([]uptimeProjectSummary, 0, len(projects))
	for _, p := range projects {
		samples := s.uptime.snapshot(p.ID)
		sum := summarizeUptime(samples)
		sum.ProjectID = p.ID
		sum.Slug = p.Slug
		sum.Samples = samples
		out = append(out, sum)
	}
	writeJSON(w, http.StatusOK, map[string]any{"projects": out, "as_of": time.Now().UTC().Format(time.RFC3339)})
}

func summarizeUptime(samples []uptimeSample) uptimeProjectSummary {
	if len(samples) == 0 {
		return uptimeProjectSummary{UptimePct: 100, LastOK: true}
	}
	ok := 0
	var latSum int
	for _, s := range samples {
		if s.OK {
			ok++
		}
		latSum += s.LatencyMs
	}
	last := samples[len(samples)-1]
	return uptimeProjectSummary{
		UptimePct:    float64(ok) / float64(len(samples)) * 100,
		AvgLatencyMs: latSum / len(samples),
		LastOK:       last.OK,
		LastChecked:  last.At.Format(time.RFC3339),
	}
}
