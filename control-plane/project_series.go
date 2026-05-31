package main

// Per-project rich time-series.
//
// The existing seriesStore (telemetry_series.go) is keyed by Traefik
// service name and tracks only status-code class counts. That's enough
// for the legacy /telemetry/series endpoint but too thin for a real
// observability dashboard — we want CPU/memory over time, request rate
// over time, and proper latency percentiles (p50/p95/p99) so students
// looking at this can recognise the same shape they'd see in Grafana.
//
// projectSeries is a parallel ring-buffer keyed by project slug. Every
// sampler tick (30 s) we compute one projectMetric per project by
// combining:
//
//   - Pod metrics from metrics-server (CPU mCPU sum, memory MiB sum)
//   - Per-class request delta from Traefik's _total counter
//   - Per-window avg latency from sum/count delta
//   - p50/p95/p99 from the delta histogram (real, not running-avg)
//
// One ring of 120 points × ~120 bytes/point × N projects = a few KB
// per project — comfortably fits in the control-plane's memory budget.

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const projectSeriesMaxLen = 120 // 60 minutes at 30 s cadence

// projectMetric is one window-aligned data point for an app. All counter
// fields are DELTAS over the last `seriesInterval` (typically 30 s); all
// gauge fields are SNAPSHOTS at AsOf. Latency values are milliseconds.
// Zero means "no signal in this window" rather than "0 ms" — the UI
// treats them as gaps.
type projectMetric struct {
	T            int64 `json:"t"`
	Pods         int   `json:"pods"`
	CPUm         int64 `json:"cpu_m"`
	MemoryMi     int64 `json:"memory_mi"`
	Req2xx       int64 `json:"req_2xx"`
	Req3xx       int64 `json:"req_3xx"`
	Req4xx       int64 `json:"req_4xx"`
	Req5xx       int64 `json:"req_5xx"`
	LatencyAvgMs int64 `json:"latency_avg_ms"`
	LatencyP50Ms int64 `json:"latency_p50_ms"`
	LatencyP95Ms int64 `json:"latency_p95_ms"`
	LatencyP99Ms int64 `json:"latency_p99_ms"`
}

// projectBuf is one project's history plus the bookkeeping needed to
// compute deltas on the next tick. Keys are Traefik service names — a
// project can have many (one per per-commit Deployment) and we sum.
type projectBuf struct {
	Points      []projectMetric
	LastDurSum  map[string]float64                // svc -> running duration_sum
	LastDurCnt  map[string]float64                // svc -> running duration_count
	LastByClass map[string]map[string]float64     // svc -> class -> running count
	LastBuckets map[string]map[string]float64     // svc -> le -> running cum count
}

type projectSeries struct {
	mu     sync.RWMutex
	bySlug map[string]*projectBuf
}

func newProjectSeries() *projectSeries {
	return &projectSeries{bySlug: map[string]*projectBuf{}}
}

// recordSnapshot adds one point per project. Called by the sampler
// immediately after the underlying seriesStore.recordSnapshot, so both
// time-series stay aligned on the same tick timestamps.
func (s *projectSeries) recordSnapshot(snap *TelemetrySnapshot, projects []projectWithTenant) {
	if snap == nil {
		return
	}
	now := snap.AsOf.Unix()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range projects {
		buf, ok := s.bySlug[p.Slug]
		if !ok {
			buf = &projectBuf{
				LastDurSum:  map[string]float64{},
				LastDurCnt:  map[string]float64{},
				LastByClass: map[string]map[string]float64{},
				LastBuckets: map[string]map[string]float64{},
			}
			s.bySlug[p.Slug] = buf
		}
		pt := projectMetric{T: now}

		// --- Pod metrics: gauge snapshot, not delta ---
		ns := "paas-tenant-" + p.TenantLogin
		for _, pod := range snap.Pods[ns] {
			if !strings.HasPrefix(pod.Name, p.Slug+"-") {
				continue
			}
			pt.Pods++
			pt.CPUm += pod.CPUm
			pt.MemoryMi += pod.MemoryMi
		}

		// --- Traffic: per-class request counters (delta) + latency
		//     (sum/count delta) + bucket histogram (per-le delta) ---
		var totalDurSumD, totalDurCntD float64
		aggBucketDelta := map[string]float64{}

		// Snapshot of service names that match this project's slug. We
		// scan the full set once so a service that disappeared since the
		// last tick (decommissioned deploy) doesn't leave stale state.
		seen := map[string]struct{}{}
		for svcName, t := range snap.Services {
			if !strings.Contains(svcName, p.Slug) {
				continue
			}
			seen[svcName] = struct{}{}

			// Class deltas. First time we see this service we record
			// the raw and emit zero — there's no prior to delta against.
			prevClass := buf.LastByClass[svcName]
			if prevClass != nil {
				for cls, raw := range t.ByClass {
					d := raw - prevClass[cls]
					if d < 0 {
						d = 0 // counter reset (Traefik restart)
					}
					switch cls {
					case "2xx":
						pt.Req2xx += int64(d)
					case "3xx":
						pt.Req3xx += int64(d)
					case "4xx":
						pt.Req4xx += int64(d)
					case "5xx":
						pt.Req5xx += int64(d)
					}
				}
			}
			buf.LastByClass[svcName] = cloneFloat(t.ByClass)

			// Latency sum/count delta.
			dSum := t.Duration - buf.LastDurSum[svcName]
			dCnt := t.Count - buf.LastDurCnt[svcName]
			if dSum < 0 {
				dSum = 0
			}
			if dCnt < 0 {
				dCnt = 0
			}
			totalDurSumD += dSum
			totalDurCntD += dCnt
			buf.LastDurSum[svcName] = t.Duration
			buf.LastDurCnt[svcName] = t.Count

			// Bucket deltas across this service.
			if prev := buf.LastBuckets[svcName]; prev != nil {
				for le, raw := range t.Buckets {
					d := raw - prev[le]
					if d < 0 {
						d = 0
					}
					aggBucketDelta[le] += d
				}
			}
			buf.LastBuckets[svcName] = cloneFloat(t.Buckets)
		}
		// Garbage-collect bookkeeping for services that vanished — over
		// time we'd accumulate keys forever as old per-SHA deployments
		// get reaped.
		for k := range buf.LastByClass {
			if _, ok := seen[k]; !ok {
				delete(buf.LastByClass, k)
				delete(buf.LastDurSum, k)
				delete(buf.LastDurCnt, k)
				delete(buf.LastBuckets, k)
			}
		}

		if totalDurCntD > 0 {
			pt.LatencyAvgMs = int64((totalDurSumD / totalDurCntD) * 1000)
		}
		if totalDurCntD > 0 && len(aggBucketDelta) > 0 {
			pt.LatencyP50Ms = histogramQuantileMs(aggBucketDelta, totalDurCntD, 0.50)
			pt.LatencyP95Ms = histogramQuantileMs(aggBucketDelta, totalDurCntD, 0.95)
			pt.LatencyP99Ms = histogramQuantileMs(aggBucketDelta, totalDurCntD, 0.99)
		}

		buf.Points = append(buf.Points, pt)
		if len(buf.Points) > projectSeriesMaxLen {
			buf.Points = buf.Points[len(buf.Points)-projectSeriesMaxLen:]
		}
	}
}

// projectPoints returns a copy of the buffered points for one project
// (oldest first). Empty slice if the project hasn't been recorded yet.
// We return a copy so callers can serialize / modify without locking.
func (s *projectSeries) projectPoints(slug string) []projectMetric {
	s.mu.RLock()
	defer s.mu.RUnlock()
	buf, ok := s.bySlug[slug]
	if !ok {
		return nil
	}
	out := make([]projectMetric, len(buf.Points))
	copy(out, buf.Points)
	return out
}

// cloneFloat is a tiny helper to deep-copy the lifetime-counter maps
// before stashing them as "previous". Without this we'd be holding a
// reference to the snapshot's map, which the next sampler tick mutates.
func cloneFloat(m map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// histogramQuantileMs computes the q-quantile in milliseconds from a
// delta histogram. `buckets` is le-string -> cumulative count delta in
// the window. `total` is the count delta over the same window (==
// the +Inf bucket).
//
// This is Prometheus' standard linear-interpolation algorithm: find
// the first bucket where the cumulative count >= q*total, then
// interpolate the position within that bucket. Buckets are assumed
// cumulative ("le" semantics — count of requests <= le seconds).
func histogramQuantileMs(buckets map[string]float64, total, q float64) int64 {
	if total <= 0 || len(buckets) == 0 {
		return 0
	}
	type bucket struct {
		le float64
		v  float64
	}
	arr := make([]bucket, 0, len(buckets))
	for k, v := range buckets {
		var le float64
		if k == "+Inf" {
			le = math.Inf(1)
		} else {
			le, _ = strconv.ParseFloat(k, 64)
		}
		arr = append(arr, bucket{le, v})
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].le < arr[j].le })

	target := q * total
	var prevLe, prevV float64
	for _, b := range arr {
		if b.v >= target {
			// +Inf upper bound — return the previous bucket's le as a
			// best-effort estimate, since interpolation across infinity
			// is meaningless.
			if math.IsInf(b.le, 1) {
				return int64(prevLe * 1000)
			}
			if b.v == prevV {
				return int64(b.le * 1000)
			}
			frac := (target - prevV) / (b.v - prevV)
			le := prevLe + frac*(b.le-prevLe)
			return int64(le * 1000)
		}
		prevLe, prevV = b.le, b.v
	}
	// All bucket counts < target (means cumulative never reached target,
	// which implies counter mismatch). Return the highest finite bucket.
	last := arr[len(arr)-1]
	if math.IsInf(last.le, 1) {
		return int64(prevLe * 1000)
	}
	return int64(last.le * 1000)
}

// Scheduling for the project-series sampler lives in telemetry_series.go
// (startTelemetrySampler) so the kube-API snapshot is fetched once per
// tick and shared between the two stores.
