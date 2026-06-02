package main

// Real-time build log streaming over Server-Sent Events.
//
// GET /v1/deployments/{id}/logs returns an SSE stream with three event types:
//
//   event: status   data: {"status":"<state>","commit":"…","url":"…"}
//   event: log      data: {"c":"<container>","m":"<line>"}
//   event: end      data: {"status":"<final>"}
//
// The handler tails the clone init-container, then the kaniko main container,
// of the Job pod created by the builder. For deployments that are already in
// a terminal state we still emit logs if the pod (and its container logs)
// are still around — Kubernetes garbage-collects them per ttlSecondsAfterFinished
// on the Job, which is 30 minutes for builds.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"time"
)

// uuidRE — relaxed UUIDv4-ish guard so we never interpolate untrusted path
// segments into a label selector.
var uuidRE = regexp.MustCompile(`^[0-9a-fA-F-]{36}$`)

// buildContainers is the ordered list of containers in a build pod that the
// client wants to see streamed. Matches buildJobManifest.
var buildContainers = []string{"clone", "kaniko"}

// sseBufferBust is a 2KB SSE comment written immediately after the response
// headers on every log stream. Many reverse proxies (Cloudflare Tunnel,
// some Traefik configurations, nginx-ingress without proxy_buffering off)
// will hold small responses in a buffer until either (a) the buffer fills
// or (b) some inactivity timeout fires — even when the application has
// already called Flush. Pre-loading the response with 2KB of padding
// pushes the buffer past those thresholds on the first write, so the
// per-line Flush()es that follow actually make it to the client in real
// time. The leading ':' marks the whole block as an SSE comment, which
// the browser's EventSource API silently discards.
var sseBufferBust = func() []byte {
	buf := make([]byte, 0, 2050)
	buf = append(buf, ':')
	for i := 0; i < 2048; i++ {
		buf = append(buf, ' ')
	}
	buf = append(buf, '\n', '\n')
	return buf
}()

func (s *server) handleDeploymentLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidRE.MatchString(id) {
		http.Error(w, "invalid deployment id", http.StatusBadRequest)
		return
	}

	// Auth: an unauthenticated stranger must never tail another user's
	// build logs. requireUser middleware guarantees this for /v1/* but
	// we recheck here so a routing mistake can't silently downgrade it.
	u := userFromCtx(r.Context())
	if u == nil {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}

	// Ownership-scoped lookup; admins bypass via empty filter. Anything
	// else is a 404 so we never leak which IDs exist.
	scope := u.ID
	if u.IsAdmin {
		scope = ""
	}
	lookupCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	dep, err := s.store.GetDeploymentForUser(lookupCtx, id, scope)
	cancel()
	if err != nil {
		s.log.Error("get deployment failed", "id", id, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if dep == nil {
		http.NotFound(w, r)
		return
	}

	// SSE headers + disable the server-wide WriteTimeout for this connection.
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})

	flush := func() { _ = rc.Flush() }
	send := func(event string, payload any) bool {
		buf, err := json.Marshal(payload)
		if err != nil {
			return false
		}
		if _, err := w.Write([]byte("event: " + event + "\ndata: ")); err != nil {
			return false
		}
		if _, err := w.Write(buf); err != nil {
			return false
		}
		if _, err := w.Write([]byte("\n\n")); err != nil {
			return false
		}
		flush()
		return true
	}
	// Heartbeat: SSE comments are ignored by EventSource but force any
	// intermediate proxy (Cloudflare Tunnel in particular) to forward
	// bytes rather than buffer the response. Without this, an idle
	// build (e.g. a stuck kaniko push) shows as "streaming…" in the
	// browser forever with zero log lines actually delivered.
	heartbeat := func() bool {
		if _, err := w.Write([]byte(": hb\n\n")); err != nil {
			return false
		}
		flush()
		return true
	}
	// 2KB padding preamble — forces intermediate proxies (Cloudflare
	// Tunnel, Traefik) to commit to streaming this response instead
	// of buffering small writes until some threshold. Without this,
	// individual log lines (~100B each) get coalesced upstream and
	// arrive in 15s bursts coincident with the heartbeat tick rather
	// than in real time. The space-padded SSE comment is ignored by
	// the browser's EventSource and adds <1ms latency to first byte.
	w.Write(sseBufferBust)
	heartbeat()

	// Initial status snapshot — UI can render "queued"/"building"/etc.
	// immediately, even if the pod doesn't exist yet.
	send("status", statusPayload(dep))

	// Background ticker keeps the stream warm during idle gaps inside
	// kaniko / clone / waitForBuildPod and — critically — forces a flush
	// every 2s so the worst-case visible gap between a log line being
	// written on the pod and it appearing in the dashboard is bounded
	// to ~2s even when an intermediary is still coalescing.
	hbCtx, hbCancel := context.WithCancel(r.Context())
	defer hbCancel()
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				if !heartbeat() {
					return
				}
			}
		}
	}()

	ctx := r.Context()

	// Fast path: if the deployment is already in a terminal status, the
	// build Job (and its pod) has almost certainly been garbage-collected
	// by the Job TTL. Without this fast path the client would hang for 2
	// minutes inside waitForBuildPod before getting an 'end' event for a
	// build that finished an hour ago.
	switch dep.Status {
	case "ready", "failed", "cancelled":
		// Best-effort: if the pod is still around (rare), tail it; else
		// send a friendly "no logs available" message and exit.
		lookupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		pod, _ := s.k8s.findBuildPod(lookupCtx, buildNamespace, id)
		cancel()
		if pod == "" {
			send("log", map[string]string{
				"c": "system",
				"m": "Build artefacts for this deployment have been cleaned up. Logs are only retained while the build pod exists (~1h after completion).",
			})
			send("end", map[string]string{"status": dep.Status})
			return
		}
		for _, c := range buildContainers {
			if ctx.Err() != nil {
				return
			}
			_ = s.k8s.streamPodLog(ctx, buildNamespace, pod, c, func(line string) {
				send("log", map[string]string{"c": c, "m": line})
			})
		}
		send("end", map[string]string{"status": dep.Status})
		return
	}

	// Wait up to 2 minutes for the build pod to be created. Deployments stay
	// in 'queued' until the worker claims them, which is typically <3s but
	// can be longer right after a control-plane restart.
	pod, err := s.waitForBuildPod(ctx, id, 2*time.Minute)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Warn("wait for build pod failed", "id", id, "err", err)
		}
		send("end", map[string]string{"reason": "no build pod"})
		return
	}
	if pod == "" {
		send("end", map[string]string{"reason": "no build pod"})
		return
	}

	// Tail each container in order. streamPodLog handles "still waiting"
	// internally so calling it for kaniko while clone is running is fine.
	for _, c := range buildContainers {
		if ctx.Err() != nil {
			return
		}
		err := s.k8s.streamPodLog(ctx, buildNamespace, pod, c, func(line string) {
			send("log", map[string]string{"c": c, "m": line})
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("stream log failed", "id", id, "container", c, "err", err)
			send("log", map[string]string{"c": c, "m": "[control-plane] log stream ended: " + err.Error()})
			// Don't return — let the loop try the next container.
		}
	}

	// Poll the row for up to 30s after the build pod exits — the worker
	// still has to apply Deployment/Service/IngressRoute and flip the
	// status to ready/failed.
	final := s.waitForTerminal(r.Context(), id, 30*time.Second)
	if final == nil {
		// Fall back to a one-shot lookup so we always send something useful.
		oneCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		final, _ = s.store.GetDeployment(oneCtx, id)
		cancel()
	}
	if final != nil {
		send("status", statusPayload(final))
	}
	send("end", map[string]string{"status": valueOr(final, "status", dep.Status)})
}

// waitForTerminal returns the deployment once status is ready/failed, or nil
// if the deadline elapsed.
func (s *server) waitForTerminal(ctx context.Context, id string, maxWait time.Duration) *deploymentRow {
	deadline := time.Now().Add(maxWait)
	for {
		lookupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		d, err := s.store.GetDeployment(lookupCtx, id)
		cancel()
		if err == nil && d != nil && (d.Status == "ready" || d.Status == "failed") {
			return d
		}
		if time.Now().After(deadline) {
			return d
		}
		select {
		case <-ctx.Done():
			return d
		case <-time.After(1500 * time.Millisecond):
		}
	}
}

// waitForBuildPod polls for the build pod every 2s up to maxWait. Returns
// (name, nil) on success, ("", nil) if the deadline elapsed.
func (s *server) waitForBuildPod(ctx context.Context, deploymentID string, maxWait time.Duration) (string, error) {
	deadline := time.Now().Add(maxWait)
	for {
		lookupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		pod, err := s.k8s.findBuildPod(lookupCtx, buildNamespace, deploymentID)
		cancel()
		if err != nil {
			return "", err
		}
		if pod != "" {
			return pod, nil
		}
		if time.Now().After(deadline) {
			return "", nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func statusPayload(d *deploymentRow) map[string]any {
	out := map[string]any{
		"id":     d.ID,
		"status": d.Status,
		"commit": d.CommitSHA,
		"slug":   d.Slug,
	}
	if d.URL != nil {
		out["url"] = *d.URL
	}
	if d.Image != nil {
		out["image"] = *d.Image
	}
	return out
}

func valueOr(d *deploymentRow, field, fallback string) string {
	if d == nil {
		return fallback
	}
	switch field {
	case "status":
		return d.Status
	}
	return fallback
}
