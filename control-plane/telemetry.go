package main

// Telemetry collector: fuses two sources into a single snapshot the
// dashboard renders.
//
//   1. metrics-server (metrics.k8s.io/v1beta1) — per-pod CPU + memory
//      readings, refreshed every ~30s by the server. We list ALL pods
//      in a single call and filter to paas-tenant-* namespaces in-go.
//
//   2. Traefik /metrics — Prometheus text exposition on :9100. We
//      discover a Running Traefik pod via the kube API, then GET its
//      pod IP directly (control-plane is in-cluster, no auth required
//      for the metrics endpoint). We only consume two metrics:
//        - traefik_service_requests_total  (per-service request count)
//        - traefik_service_request_duration_seconds_sum / _count (avg latency)
//
// A small in-memory TTL cache (5s) absorbs multi-tab polling without
// hammering the metrics-server or Traefik. Computing fresh costs ~50-150ms
// (two short k8s API calls + one in-cluster HTTP fetch), so even without
// the cache this would be cheap — the cache is purely for politeness.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PodMetric is the per-pod view from metrics.k8s.io. CPU is reported in
// millicores and memory in MiB to keep the dashboard math simple.
type PodMetric struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	CPUm      int64  `json:"cpu_m"`
	MemoryMi  int64  `json:"memory_mi"`
}

// ServiceTraffic accumulates request counts and timings for one Traefik
// service across all status codes. The Service field is the raw Traefik
// service name (e.g. "paas-tenant-jamilshaikh07-...@kubernetescrd").
//
// ByClass keeps lifetime per-class totals so the time-series sampler can
// compute deltas per class (2xx/3xx/4xx/5xx) between adjacent ticks
// without re-scraping. Requests + Errors stay for the dashboard tables
// that don't care about class breakdown.
type ServiceTraffic struct {
	Service  string             `json:"service"`
	Requests float64            `json:"requests"`     // total since Traefik start
	Errors   float64            `json:"errors"`       // requests with 4xx/5xx code
	Duration float64            `json:"duration_sum"` // sum of request_duration_seconds
	Count    float64            `json:"duration_cnt"` // count for averaging
	ByClass  map[string]float64 `json:"by_class"`     // {"2xx":..,"3xx":..,"4xx":..,"5xx":..,"1xx":..}
}

// TelemetrySnapshot is the cached read model returned by the cache. Always
// non-nil from the cache so handlers can short-circuit on .Err.
type TelemetrySnapshot struct {
	AsOf     time.Time                   `json:"as_of"`
	Pods     map[string][]PodMetric      `json:"pods"`     // ns -> pods
	Services map[string]*ServiceTraffic  `json:"services"` // service -> traffic
	Err      string                      `json:"err,omitempty"`
}

const telemetryTTL = 5 * time.Second

type telemetryCache struct {
	mu       sync.Mutex
	snapshot *TelemetrySnapshot
}

func (c *telemetryCache) get(ctx context.Context, k *kubeClient) *TelemetrySnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snapshot != nil && time.Since(c.snapshot.AsOf) < telemetryTTL {
		return c.snapshot
	}
	snap := collectTelemetry(ctx, k)
	c.snapshot = snap
	return snap
}

// collectTelemetry fetches both sources fresh. Partial failure is acceptable
// — we always return a snapshot, with .Err set so the caller can surface
// degraded mode rather than show nothing.
func collectTelemetry(ctx context.Context, k *kubeClient) *TelemetrySnapshot {
	snap := &TelemetrySnapshot{
		AsOf:     time.Now(),
		Pods:     map[string][]PodMetric{},
		Services: map[string]*ServiceTraffic{},
	}
	var errs []string

	if err := loadPodMetrics(ctx, k, snap); err != nil {
		errs = append(errs, "metrics: "+err.Error())
	}
	if err := loadTraefikMetrics(ctx, k, snap); err != nil {
		errs = append(errs, "traefik: "+err.Error())
	}
	if len(errs) > 0 {
		snap.Err = strings.Join(errs, "; ")
	}
	return snap
}

// loadPodMetrics fills snap.Pods from metrics-server. Filters to
// paas-tenant-* namespaces — we don't expose internal cluster pods on
// the dashboard.
func loadPodMetrics(ctx context.Context, k *kubeClient, snap *TelemetrySnapshot) error {
	status, body, err := k.do(ctx, "GET", "/apis/metrics.k8s.io/v1beta1/pods", nil)
	if err != nil {
		return err
	}
	if status != 200 {
		return fmt.Errorf("HTTP %d: %s", status, snippet(body))
	}
	var pm struct {
		Items []struct {
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
			Containers []struct {
				Usage map[string]string `json:"usage"`
			} `json:"containers"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &pm); err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	for _, p := range pm.Items {
		if !strings.HasPrefix(p.Metadata.Namespace, "paas-tenant-") {
			continue
		}
		var cpuM, memMi int64
		for _, c := range p.Containers {
			cpuM += parseCPUm(c.Usage["cpu"])
			memMi += parseMemMi(c.Usage["memory"])
		}
		snap.Pods[p.Metadata.Namespace] = append(snap.Pods[p.Metadata.Namespace], PodMetric{
			Namespace: p.Metadata.Namespace,
			Name:      p.Metadata.Name,
			CPUm:      cpuM,
			MemoryMi:  memMi,
		})
	}
	return nil
}

// parseCPUm normalises a metrics.k8s.io CPU quantity into integer
// millicores. metrics-server returns nanocore strings like "12345n"; we
// also handle "10m" and bare cores ("0.5", "1") for robustness.
func parseCPUm(v string) int64 {
	if v == "" {
		return 0
	}
	if strings.HasSuffix(v, "n") {
		n, _ := strconv.ParseInt(strings.TrimSuffix(v, "n"), 10, 64)
		return n / 1_000_000 // nano -> milli
	}
	if strings.HasSuffix(v, "u") {
		u, _ := strconv.ParseInt(strings.TrimSuffix(v, "u"), 10, 64)
		return u / 1000 // micro -> milli
	}
	if strings.HasSuffix(v, "m") {
		m, _ := strconv.ParseInt(strings.TrimSuffix(v, "m"), 10, 64)
		return m
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return int64(f * 1000)
	}
	return 0
}

// parseMemMi normalises a metrics.k8s.io memory quantity into MiB.
// metrics-server typically returns Ki (kibibytes). We support the
// kubernetes-standard binary (Ki/Mi/Gi) and decimal (K/M/G) suffixes.
func parseMemMi(v string) int64 {
	if v == "" {
		return 0
	}
	for _, suf := range []string{"Ki", "Mi", "Gi", "Ti"} {
		if strings.HasSuffix(v, suf) {
			n, _ := strconv.ParseInt(strings.TrimSuffix(v, suf), 10, 64)
			switch suf {
			case "Ki":
				return n / 1024
			case "Mi":
				return n
			case "Gi":
				return n * 1024
			case "Ti":
				return n * 1024 * 1024
			}
		}
	}
	for _, suf := range []string{"K", "M", "G", "T"} {
		if strings.HasSuffix(v, suf) {
			n, _ := strconv.ParseInt(strings.TrimSuffix(v, suf), 10, 64)
			switch suf {
			case "K":
				return n / 1000
			case "M":
				return n
			case "G":
				return n * 1000
			case "T":
				return n * 1000 * 1000
			}
		}
	}
	// Bare bytes.
	n, _ := strconv.ParseInt(v, 10, 64)
	return n / (1024 * 1024)
}

// loadTraefikMetrics finds a Running Traefik pod, fetches its
// Prometheus exposition, and accumulates per-service request totals.
func loadTraefikMetrics(ctx context.Context, k *kubeClient, snap *TelemetrySnapshot) error {
	podIP, err := findTraefikPodIP(ctx, k)
	if err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", "http://"+podIP+":9100/metrics", nil)
	if err != nil {
		return err
	}
	// Plain HTTP, in-cluster pod IP — no auth.
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return parseTraefikMetrics(resp.Body, snap)
}

// findTraefikPodIP returns the IP of any Running Traefik pod. All replicas
// share state via the kubernetes-CRD provider so any pod's metrics view is
// representative for the cluster.
func findTraefikPodIP(ctx context.Context, k *kubeClient) (string, error) {
	sel := "app.kubernetes.io/name=traefik"
	path := "/api/v1/namespaces/traefik-system/pods?labelSelector=" + url.QueryEscape(sel)
	status, body, err := k.do(ctx, "GET", path, nil)
	if err != nil {
		return "", err
	}
	if status != 200 {
		return "", fmt.Errorf("list pods HTTP %d: %s", status, snippet(body))
	}
	var pl struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Status struct {
				PodIP string `json:"podIP"`
				Phase string `json:"phase"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &pl); err != nil {
		return "", err
	}
	for _, p := range pl.Items {
		if p.Status.Phase == "Running" && p.Status.PodIP != "" {
			return p.Status.PodIP, nil
		}
	}
	return "", fmt.Errorf("no running traefik pod found")
}

// parseTraefikMetrics scans the Prometheus text-exposition response for the
// two families we care about. Everything else is skipped — we don't want
// to retain the full ~700-line dump for every snapshot.
//
// Sample input lines:
//   traefik_service_requests_total{code="200",method="GET",protocol="web",service="..."}  1234
//   traefik_service_request_duration_seconds_sum{...}                                     12.345
//   traefik_service_request_duration_seconds_count{...}                                   1234
func parseTraefikMetrics(r io.Reader, snap *TelemetrySnapshot) error {
	s := bufio.NewScanner(r)
	// Some Prometheus lines (especially with long label sets) can exceed
	// bufio's 64K default. Bump to 1MB to be safe.
	s.Buffer(make([]byte, 1<<16), 1<<20)
	for s.Scan() {
		line := s.Text()
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		var (
			isCount     bool
			isDurSum    bool
			isDurCount  bool
		)
		switch {
		case strings.HasPrefix(line, "traefik_service_requests_total{"):
			isCount = true
		case strings.HasPrefix(line, "traefik_service_request_duration_seconds_sum{"):
			isDurSum = true
		case strings.HasPrefix(line, "traefik_service_request_duration_seconds_count{"):
			isDurCount = true
		default:
			continue
		}
		labels, valStr, ok := splitMetricLine(line)
		if !ok {
			continue
		}
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}
		var service, code string
		for _, kv := range splitLabels(labels) {
			eq := strings.Index(kv, "=")
			if eq == -1 {
				continue
			}
			key := kv[:eq]
			v := strings.Trim(kv[eq+1:], `"`)
			switch key {
			case "service":
				service = v
			case "code":
				code = v
			}
		}
		if service == "" {
			continue
		}
		st, ok := snap.Services[service]
		if !ok {
			st = &ServiceTraffic{Service: service, ByClass: map[string]float64{}}
			snap.Services[service] = st
		}
		switch {
		case isCount:
			st.Requests += val
			if len(code) == 3 && (code[0] == '4' || code[0] == '5') {
				st.Errors += val
			}
			// Per-class lifetime totals — used by the time-series sampler
			// to compute deltas per (service, class) between snapshots.
			st.ByClass[classifyCode(code)] += val
		case isDurSum:
			st.Duration += val
		case isDurCount:
			st.Count += val
		}
	}
	return s.Err()
}

// splitMetricLine separates the "labels}" block from the trailing value.
// Returns (labels, value, ok).
func splitMetricLine(line string) (string, string, bool) {
	open := strings.IndexByte(line, '{')
	close := strings.IndexByte(line, '}')
	if open == -1 || close == -1 || close <= open {
		return "", "", false
	}
	labels := line[open+1 : close]
	rest := strings.TrimSpace(line[close+1:])
	// rest is "<value>" or "<value> <timestamp>" — keep only the value.
	if sp := strings.IndexByte(rest, ' '); sp != -1 {
		rest = rest[:sp]
	}
	return labels, rest, true
}

// splitLabels handles Prometheus's comma-separated key="value" list,
// respecting quotes (Traefik label values can contain @, /, dashes, etc.).
func splitLabels(s string) []string {
	var out []string
	var cur []byte
	inQ := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' {
			inQ = !inQ
		}
		if c == ',' && !inQ {
			out = append(out, string(cur))
			cur = cur[:0]
			continue
		}
		cur = append(cur, c)
	}
	if len(cur) > 0 {
		out = append(out, string(cur))
	}
	return out
}

