package main

// HTTP-status-code time-series sampler.
//
// Traefik exposes lifetime counters via traefik_service_requests_total.
// To draw a meaningful "requests per minute" chart we need rates, which
// requires snapshotting the counters at a fixed cadence and computing
// deltas between adjacent snapshots.
//
// Storage: an in-memory ring buffer per service name, holding the last
// N (default 120) data points. At a 30-second cadence that's 60 minutes
// of history per service. Memory cost: ~50 bytes/point × 4 classes × 120
// points × ~100 services worst-case = ~2.5 MB. Comfortably fits in the
// control-plane's 128Mi limit.
//
// The buffer is volatile — on pod restart we start fresh. Persistence
// would need a Prometheus sidecar (or our own TSDB) which we haven't
// budgeted for in MVP. The sampler stays fresh again ~30s after restart.
//
// Status codes are bucketed by family (2xx/3xx/4xx/5xx). Per-individual-
// code granularity ratios up the storage 10× without delivering useful
// signal at this scale — the user's question is almost always "am I
// serving errors?" not "is it specifically 418 or 422?".

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

const (
	seriesInterval = 30 * time.Second
	seriesMaxLen   = 120 // → 60 minutes of history at 30s cadence
)

// seriesPoint is one snapshot in the ring buffer. Counts are delta from
// the previous tick (i.e. requests in the last ~30s window).
type seriesPoint struct {
	T      int64            `json:"t"`      // unix seconds
	Counts map[string]int64 `json:"counts"` // {"2xx": 12, "4xx": 1, ...}
}

// serviceBuffer is one service's history + last-raw state for delta calc.
type serviceBuffer struct {
	Points  []seriesPoint
	LastRaw map[string]float64 // last seen raw counter per class
}

type seriesStore struct {
	mu     sync.RWMutex
	byName map[string]*serviceBuffer // key: traefik service name
}

func newSeriesStore() *seriesStore {
	return &seriesStore{byName: map[string]*serviceBuffer{}}
}

// classifyCode collapses an individual HTTP status code into its family
// label. Anything outside 100-599 (rare; usually missing/empty) becomes "other".
func classifyCode(code string) string {
	if len(code) != 3 {
		return "other"
	}
	switch code[0] {
	case '1':
		return "1xx"
	case '2':
		return "2xx"
	case '3':
		return "3xx"
	case '4':
		return "4xx"
	case '5':
		return "5xx"
	}
	return "other"
}

// recordSnapshot ingests one fresh Traefik scrape and appends a delta
// point to every service's buffer. Called from the sampler tick.
//
// Note we walk the FULL service set fresh each tick rather than tracking
// joins/leaves — services come and go (every deployment creates new ones)
// and the simple approach matches Traefik's view of the world.
func (s *seriesStore) recordSnapshot(snap *TelemetrySnapshot) {
	now := snap.AsOf.Unix()

	// Step 1: roll up raw lifetime counters by (service, class). ByClass
	// is already populated by parseTraefikMetrics — we just clone the
	// reference so the goroutine that updates the snapshot next doesn't
	// race with us reading it later.
	rawByService := map[string]map[string]float64{}
	for serviceName, st := range snap.Services {
		if st == nil || st.ByClass == nil {
			continue
		}
		raw := make(map[string]float64, len(st.ByClass))
		for cls, v := range st.ByClass {
			raw[cls] = v
		}
		rawByService[serviceName] = raw
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for svcName, raw := range rawByService {
		buf, ok := s.byName[svcName]
		if !ok {
			buf = &serviceBuffer{LastRaw: map[string]float64{}}
			s.byName[svcName] = buf
		}
		// First sample after process start: don't emit a point, just seed.
		// Subsequent samples produce real deltas.
		if len(buf.LastRaw) == 0 {
			buf.LastRaw = raw
			continue
		}
		pt := seriesPoint{T: now, Counts: map[string]int64{}}
		for cls, cur := range raw {
			prev := buf.LastRaw[cls]
			d := cur - prev
			// Counter resets are negative — show 0 (Traefik restart, or
			// service decommissioned + re-registered).
			if d < 0 {
				d = 0
			}
			pt.Counts[cls] = int64(d)
		}
		buf.Points = append(buf.Points, pt)
		if len(buf.Points) > seriesMaxLen {
			// Trim from the front. cap of 120 means at most 1 alloc per tick.
			buf.Points = buf.Points[len(buf.Points)-seriesMaxLen:]
		}
		buf.LastRaw = raw
	}
}

// pointsForServices merges the histories of multiple services (an app
// typically has 1 service per deployment, so "all deployments of one
// app" = multiple services to sum). Points are aligned by timestamp;
// missing samples on either side count as zero.
func (s *seriesStore) pointsForServices(serviceMatcher func(string) bool) []seriesPoint {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect all matching buffers.
	var bufs []*serviceBuffer
	for name, buf := range s.byName {
		if serviceMatcher(name) {
			bufs = append(bufs, buf)
		}
	}
	if len(bufs) == 0 {
		return nil
	}

	// Index every (timestamp -> class -> count) for fast merge.
	merged := map[int64]map[string]int64{}
	for _, b := range bufs {
		for _, p := range b.Points {
			m, ok := merged[p.T]
			if !ok {
				m = map[string]int64{}
				merged[p.T] = m
			}
			for cls, n := range p.Counts {
				m[cls] += n
			}
		}
	}
	if len(merged) == 0 {
		return nil
	}
	// Sort by timestamp ascending. We use the oldest sample's t as the
	// chart's left edge and the newest as the right edge.
	ts := make([]int64, 0, len(merged))
	for t := range merged {
		ts = append(ts, t)
	}
	sortInt64Asc(ts)
	out := make([]seriesPoint, 0, len(ts))
	for _, t := range ts {
		out = append(out, seriesPoint{T: t, Counts: merged[t]})
	}
	return out
}

func sortInt64Asc(a []int64) {
	// 60-120 ints — insertion sort is fine and avoids an import.
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

// startTelemetrySampler runs the sampler ticker for the lifetime of ctx.
// One goroutine, fires every seriesInterval, fetches a fresh snapshot
// through the existing telemetryCache (which honors its own 5s TTL —
// so back-to-back calls within 5s share the same scrape).
func startTelemetrySampler(ctx context.Context, srv *server, log *slog.Logger) {
	go func() {
		log = log.With("subsystem", "telemetry-sampler")
		log.Info("starting telemetry sampler", "interval", seriesInterval)
		t := time.NewTicker(seriesInterval)
		defer t.Stop()
		for {
			// Force a fresh fetch by invalidating the cache, otherwise we'd
			// re-record the same numbers from a hot read. Simplest: call the
			// raw collect function rather than the cache.
			tickCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
			snap := collectTelemetry(tickCtx, srv.k8s)
			// List projects on every tick so a freshly-installed app starts
			// collecting series data within one window — no restart needed.
			// "" scope = all projects (admin-equivalent; this is a system
			// sampler, not a user-facing query).
			projects, err := srv.store.ListProjectsWithTenant(tickCtx, "")
			cancel()
			srv.series.recordSnapshot(snap)
			if err != nil {
				log.Warn("list projects for series sampler failed", "err", err)
			} else {
				srv.pseries.recordSnapshot(snap, projects)
			}

			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

// matcherForProject builds a predicate that picks Traefik service names
// belonging to a given project slug. IngressRoutes are named after the
// deployment (which embeds the slug), so a substring check is enough.
func matcherForProject(slug string) func(string) bool {
	return func(name string) bool {
		return strings.Contains(name, slug)
	}
}
