package main

// Endpoint that surfaces the rich per-project time series.
//
// GET /v1/projects/{id}/metrics?range=<key> returns the slice of points
// covering the requested window. Two backing buffers feed it:
//
//   - The native 30 s ring (60 min capacity) for short ranges
//     (5m / 15m / 1h). The handler slices the tail to the requested
//     number of points.
//   - The downsampled 10-min ring (7 d capacity) for long ranges
//     (1d / 1w). Same logic, just a coarser cadence.
//
// IntervalS in the response is the cadence of the buffer that fed it,
// so the frontend's x-axis labels and incident-detector tick-math work
// transparently across resolutions.

import (
	"net/http"
)

type projectMetricsResponse struct {
	ProjectID string          `json:"project_id"`
	Slug      string          `json:"slug"`
	IntervalS int             `json:"interval_s"`
	RangeKey  string          `json:"range"`
	Points    []projectMetric `json:"points"`
}

// rangeSpec maps the user-facing key to the buffer + slice depth that
// satisfies it. UseLong picks between projectPoints (native 30 s) and
// projectPointsLong (downsampled 10 min). PointsTail caps how many
// tail points to return — short ranges only need a sliver of the
// native ring, long ranges return the entire downsampled ring.
type rangeSpec struct {
	UseLong    bool
	PointsTail int
}

// rangeSpecs is the closed set of supported windows. Defaults to "1h"
// when the client doesn't pass a range so existing UIs keep working
// (this matches what the dashboard previously assumed implicitly).
var rangeSpecs = map[string]rangeSpec{
	"5m":  {UseLong: false, PointsTail: 10},   // 10 * 30s
	"15m": {UseLong: false, PointsTail: 30},   // 30 * 30s
	"1h":  {UseLong: false, PointsTail: 120},  // full native ring
	"1d":  {UseLong: true, PointsTail: 144},   // 144 * 10min
	"1w":  {UseLong: true, PointsTail: 1008},  // full long ring
}

func (s *server) handleProjectMetrics(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	key := r.URL.Query().Get("range")
	spec, ok := rangeSpecs[key]
	if !ok {
		key = "1h"
		spec = rangeSpecs[key]
	}

	var pts []projectMetric
	var intervalS int
	if spec.UseLong {
		pts = s.pseries.projectPointsLong(proj.Slug)
		intervalS = int(longBucketSeconds)
	} else {
		pts = s.pseries.projectPoints(proj.Slug)
		intervalS = int(seriesInterval.Seconds())
	}
	if len(pts) > spec.PointsTail {
		pts = pts[len(pts)-spec.PointsTail:]
	}

	writeJSON(w, http.StatusOK, projectMetricsResponse{
		ProjectID: proj.ID,
		Slug:      proj.Slug,
		IntervalS: intervalS,
		RangeKey:  key,
		Points:    pts,
	})
}
