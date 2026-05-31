package main

// Endpoint that surfaces the rich per-project time series.
//
// GET /v1/projects/{id}/metrics returns up to 120 data points (60 min
// at 30 s cadence) of CPU, memory, RPS (split by status class) and
// latency percentiles. The dashboard's Telemetry + Traffic pages build
// every chart from this single response — keeping the contract narrow
// is intentional so the frontend can render with no extra round-trips.

import (
	"net/http"
)

type projectMetricsResponse struct {
	ProjectID string          `json:"project_id"`
	Slug      string          `json:"slug"`
	IntervalS int             `json:"interval_s"`
	Points    []projectMetric `json:"points"`
}

func (s *server) handleProjectMetrics(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	pts := s.pseries.projectPoints(proj.Slug)
	writeJSON(w, http.StatusOK, projectMetricsResponse{
		ProjectID: proj.ID,
		Slug:      proj.Slug,
		IntervalS: int(seriesInterval.Seconds()),
		Points:    pts,
	})
}
