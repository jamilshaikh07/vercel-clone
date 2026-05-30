package main

// Runtime log streaming (Slice "Runtime Logs"). Counterpart to logs.go,
// which tails the kaniko build pod; this handler tails the tenant pod
// that's serving the deployed image so users can see what their running
// app is printing right now.
//
// GET /v1/deployments/{id}/runtime-logs returns an SSE stream with the
// same event vocabulary as the build-log endpoint:
//
//   event: status   data: {"status":"<state>","pod":"<name>"}
//   event: log      data: {"c":"app","m":"<line>"}
//   event: end      data: {"reason":"<…>"}
//
// Heartbeat comments (": hb\n\n") are sent every 15s on both endpoints
// — see also logs.go. This prevents Cloudflare Tunnel (and other
// reverse proxies) from buffering an idle SSE stream until the first
// byte of real data finally appears.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// tenantContainer is the always-named app container in deploymentManifest.
// If we ever support multi-process tenant pods, this becomes a query param.
const tenantContainer = "app"

func (s *server) handleDeploymentRuntimeLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidRE.MatchString(id) {
		http.Error(w, "invalid deployment id", http.StatusBadRequest)
		return
	}
	u := userFromCtx(r.Context())
	if u == nil {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}

	scope := u.ID
	if u.IsAdmin {
		scope = ""
	}

	// Two ownership-checked lookups: the deployment row (for status/sha)
	// and the tenant login (for namespace resolution). Both go through
	// the same user filter so we never leak across tenants.
	lookupCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	dep, err := s.store.GetDeploymentForUser(lookupCtx, id, scope)
	if err != nil {
		cancel()
		s.log.Error("get deployment failed", "id", id, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if dep == nil {
		cancel()
		http.NotFound(w, r)
		return
	}
	tenantLogin, err := s.store.GetDeploymentTenantLogin(lookupCtx, id, scope)
	cancel()
	if err != nil {
		s.log.Error("get deployment tenant failed", "id", id, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if tenantLogin == "" {
		// Owned deployment without a tenant login is a data-model bug —
		// every deployment has an installation. Treat as not-found so we
		// don't 500 a benign-looking SSE.
		http.NotFound(w, r)
		return
	}
	tenantNS := tenantNamespaceFor(tenantLogin)

	// SSE headers — same as build-log handler.
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
	// Heartbeat: comment lines are ignored by the EventSource API but
	// keep proxies from coalescing the stream into a single buffered
	// response. Started immediately so the first byte arrives within
	// 200ms even when there's nothing else to send yet.
	heartbeat := func() bool {
		if _, err := w.Write([]byte(": hb\n\n")); err != nil {
			return false
		}
		flush()
		return true
	}
	heartbeat()

	ctx := r.Context()

	// Fast paths for terminal states. A 'failed' deployment may still
	// have an old pod around (because we use maxUnavailable: 0 on the
	// rolling update) but it's the *previous* deployment's pod, not
	// this one's — we'd be lying to the user by showing it. So we
	// only attempt to find a pod when the deployment is 'ready' or
	// still in flight.
	switch dep.Status {
	case "queued":
		send("log", map[string]string{"c": "system", "m": "Deployment hasn't been built yet — no runtime pod."})
		send("end", map[string]string{"reason": "not built"})
		return
	case "failed", "cancelled":
		send("log", map[string]string{"c": "system", "m": "Deployment did not reach 'ready' — no runtime pod was created for this commit."})
		send("end", map[string]string{"reason": dep.Status})
		return
	}

	// For building/deploying/ready: try to locate a tenant pod. Wait
	// up to 60s for it to come into existence — the rollout can lag
	// behind the deployment row by a few seconds.
	pod, err := s.waitForTenantPod(ctx, tenantNS, id, 60*time.Second, heartbeat)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Warn("wait for tenant pod failed", "id", id, "err", err)
		}
		send("end", map[string]string{"reason": "no tenant pod"})
		return
	}
	if pod == "" {
		send("log", map[string]string{"c": "system", "m": "No running tenant pod found for this deployment yet. (If the deployment was rolled over by a newer one, its pod is gone.)"})
		send("end", map[string]string{"reason": "no tenant pod"})
		return
	}

	send("status", map[string]string{"status": dep.Status, "pod": pod})

	// Run the heartbeat ticker in parallel with the log stream. When
	// the stream ends (container exits / context cancelled) the
	// ticker goroutine exits too.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go func() {
		t := time.NewTicker(15 * time.Second)
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

	err = s.k8s.streamPodLog(ctx, tenantNS, pod, tenantContainer, func(line string) {
		send("log", map[string]string{"c": tenantContainer, "m": line})
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		s.log.Warn("runtime log stream failed", "id", id, "pod", pod, "err", err)
		send("log", map[string]string{"c": "system", "m": "[stream ended] " + err.Error()})
	}
	send("end", map[string]string{"reason": "stream ended"})
}

// waitForTenantPod polls findTenantPod every 2s up to maxWait, calling
// heartbeat() on each iteration so the SSE connection stays warm even
// when the rollout is slow. Returns (name, nil) once a pod exists,
// ("", nil) on deadline.
func (s *server) waitForTenantPod(ctx context.Context, namespace, deploymentID string, maxWait time.Duration, heartbeat func() bool) (string, error) {
	deadline := time.Now().Add(maxWait)
	for {
		lookupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		pod, err := s.k8s.findTenantPod(lookupCtx, namespace, deploymentID)
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
		if heartbeat != nil {
			heartbeat()
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
