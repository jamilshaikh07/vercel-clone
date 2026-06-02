package main

// Synthetic-traffic monitor.
//
// Every `synthInterval` seconds we hit each project's latest READY URL
// a handful of times. This serves two purposes:
//
//   1. UX: the dashboard's Telemetry + Traffic pages are powered by
//      real Traefik counters and metrics-server readings. With zero
//      organic traffic those charts stay flat — visually the user
//      thinks the system isn't working. Self-traffic populates them
//      with honest, real-time data: pod CPU spikes briefly, Traefik
//      records 2xx, the time-series sampler picks it up at the next
//      tick.
//
//   2. Synthetic monitoring: hitting the live URL also acts as a
//      blackbox uptime probe. A failure surfaces as a 5xx blip on the
//      Traffic chart and as a request that didn't complete — useful
//      signal even if we don't yet wire alerting.
//
// Disable with PAAS_DISABLE_SYNTHETIC=1 (e.g. in CI/dev).
//
// Cost is intentionally modest: HEAD-style probes against ~5 endpoints
// every 30 s. At ~1 KiB per request and Traefik already in the request
// path, the steady-state overhead is negligible.

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	synthInterval     = 30 * time.Second
	synthMaxHitsPerApp = 4   // upper bound per cycle per app
	synthMinHitsPerApp = 2   // lower bound — guarantees at least 2 points / 30s
	synthRequestTimeout = 6 * time.Second
)

func startSyntheticMonitor(ctx context.Context, srv *server, log *slog.Logger) {
	if strings.EqualFold(os.Getenv("PAAS_DISABLE_SYNTHETIC"), "1") {
		log.Info("synthetic monitor disabled via PAAS_DISABLE_SYNTHETIC=1")
		return
	}
	log = log.With("subsystem", "synthetic-monitor")
	log.Info("starting synthetic monitor", "interval", synthInterval)

	// Shared HTTP client. Short timeouts so a hung tenant doesn't pin
	// the goroutine for a full minute; redirects followed up to 3 hops
	// since some apps will 301 from `/` to `/index.html` etc.
	client := &http.Client{
		Timeout: synthRequestTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}

	go func() {
		// Stagger first run so we don't compete with the build worker
		// and self-rebuilder for kube-API attention right after boot.
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}

		t := time.NewTicker(synthInterval)
		defer t.Stop()
		for {
			runSyntheticCycle(ctx, srv, client, log)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

// runSyntheticCycle does one pass: list live apps, hit each one a few
// times. Independent per-app failures don't stop the cycle — one broken
// tenant shouldn't stop us from collecting metrics on the others.
func runSyntheticCycle(parent context.Context, srv *server, client *http.Client, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(parent, synthInterval-2*time.Second)
	defer cancel()

	urls, err := liveAppURLs(ctx, srv)
	if err != nil {
		log.Warn("list live apps failed", "err", err)
		return
	}
	for _, u := range urls {
		hitApp(ctx, client, u, log)
	}
}

// hitApp fires synthMinHitsPerApp..synthMaxHitsPerApp GETs at one URL,
// spaced ~200 ms apart. The spacing makes the resulting RPS look more
// like organic browsing on the dashboard than a synchronized burst,
// AND gives metrics-server a couple of scrape windows to register a
// CPU bump on the pod.
func hitApp(ctx context.Context, client *http.Client, target string, log *slog.Logger) {
	n := synthMinHitsPerApp + rand.Intn(synthMaxHitsPerApp-synthMinHitsPerApp+1)
	for i := 0; i < n; i++ {
		req, err := http.NewRequestWithContext(ctx, "GET", target, nil)
		if err != nil {
			return
		}
		// Identify ourselves so it's traceable from tenant access logs
		// that this traffic is the monitor and not a real user. Also
		// keeps tenant analytics dashboards clean (most filter on UA).
		req.Header.Set("User-Agent", "spinup-synthetic-monitor/1.0 (+https://spinup.in)")
		req.Header.Set("Accept", "text/html,application/xhtml+xml,*/*")
		resp, err := client.Do(req)
		if err != nil {
			// Don't log every flake — only at debug. A real outage will
			// show up as 5xx volume on the dashboard's traffic chart.
			log.Debug("synthetic probe error", "url", target, "err", err)
			return
		}
		// Read up to a few KiB to make Traefik record a duration_sum.
		// Without ANY body read, some Traefik versions short-circuit
		// the duration histogram. Discarding limits memory + the read
		// also gives the tenant pod a realistic request lifecycle.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8192))
		resp.Body.Close()
		// Inter-request gap. Skip the sleep on the last iteration so
		// we don't waste the cycle's final 200ms.
		if i < n-1 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// liveAppURLs joins ListProjectsWithTenant (every project) with the
// latest READY deployment URL per project. We use the existing helpers
// instead of a dedicated query to keep the data path symmetric with
// what the dashboard already reads.
func liveAppURLs(ctx context.Context, srv *server) ([]string, error) {
	projects, err := srv.store.ListProjectsWithTenant(ctx, "")
	if err != nil {
		return nil, err
	}
	if len(projects) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(projects))
	for _, p := range projects {
		ids = append(ids, p.ID)
	}
	deps, err := srv.store.ListDeploymentsForProjects(ctx, ids, 5)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(projects))
	for _, p := range projects {
		for _, d := range deps[p.ID] {
			if d.Status == "ready" && d.URL != nil && *d.URL != "" {
				out = append(out, *d.URL)
				break
			}
		}
	}
	return out, nil
}
