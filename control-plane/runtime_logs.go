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
	"fmt"
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
	// 2KB padding preamble — see comment on sseBufferBust in logs.go.
	// Defeats response-coalescing in intermediate proxies that would
	// otherwise hold small log lines in a buffer until the 2s ticker
	// hits, making the dashboard feel laggy.
	w.Write(sseBufferBust)
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
	//
	// Emit progressive system messages so the user sees activity
	// instead of staring at a silent 'streaming…' while we poll.
	// After a Stop → Start cycle the new pod can take 5–10s to come
	// up; if there's a labelling drift between projects.
	// production_deployment_id and the K8s Deployment that actually
	// has a pod, this is the only signal that something is off.
	sel := fmt.Sprintf("paas.deployment=%s,app.kubernetes.io/component=tenant", id)
	send("log", map[string]string{
		"c": "system",
		"m": fmt.Sprintf("Searching for pod in %s with selector %s", tenantNS, sel),
	})
	progress := func(attempt int) {
		// Every 3rd poll (~6s) past the first so we don't spam the
		// stream when the rollout lands quickly.
		if attempt > 0 && attempt%3 == 0 {
			send("log", map[string]string{
				"c": "system",
				"m": fmt.Sprintf("Still waiting for a tenant pod… (%ds elapsed)", attempt*2),
			})
		}
	}
	pod, err := s.waitForTenantPod(ctx, tenantNS, id, 60*time.Second, heartbeat, progress)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Warn("wait for tenant pod failed", "id", id, "err", err)
		}
		send("end", map[string]string{"reason": "no tenant pod"})
		return
	}
	if pod == "" {
		// 60s timeout with no pod. Most common causes: image pull
		// failure, the K8s Deployment is scaled to 0, or the
		// deployment_id we're searching for isn't the one whose pod
		// is actually behind the production alias (drift between
		// projects.production_deployment_id and reality). Tell the
		// user the exact selector + namespace so they can verify
		// from a shell with one command.
		send("log", map[string]string{
			"c": "system",
			"m": fmt.Sprintf("No running tenant pod found for this deployment after 60s. Verify with: kubectl get pods -n %s -l %q", tenantNS, sel),
		})
		send("log", map[string]string{
			"c": "system",
			"m": "This usually means: (a) the pod is in ImagePullBackOff / CrashLoopBackOff, (b) the K8s Deployment for this commit was scaled to 0, or (c) a different commit's pod is the one actually serving traffic (try clicking another deployment in Overview).",
		})
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
		// 2s tick keeps the stream warm and bounds the worst-case
		// buffer-flush latency at any upstream proxy.
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
// when the rollout is slow, and progress(attempt) so the caller can
// emit user-visible status while we wait. Returns (name, nil) once a
// pod exists, ("", nil) on deadline.
func (s *server) waitForTenantPod(ctx context.Context, namespace, deploymentID string, maxWait time.Duration, heartbeat func() bool, progress func(int)) (string, error) {
	deadline := time.Now().Add(maxWait)
	attempt := 0
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
		if progress != nil {
			progress(attempt)
		}
		attempt++
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
