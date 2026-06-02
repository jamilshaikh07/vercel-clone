package main

// Self-rebuilder: a background goroutine that polls GitHub for new
// commits to this repo's main branch, and on detection (a) recreates
// the in-cluster Kaniko build Job to bake a new control-plane image
// and (b) patches the control-plane Deployment with a restartedAt
// annotation to roll a fresh pod.
//
// Why this lives inside the control-plane (and not as a GitHub Action,
// or a separate operator):
//
//   * The cluster's API server is on an internal IP that isn't reachable
//     from GitHub-hosted runners without a tunnel — that's a bigger
//     security/credential conversation than we want today.
//   * A separate operator adds another pod + RBAC surface for what is
//     effectively a 200-line poll loop.
//   * Running inside the control-plane means the rebuild path uses the
//     same kube client + same ServiceAccount we already trust.
//
// "Kill the bishop" safety: if a bad commit lands…
//
//   1. that breaks the build  →  build Job fails, NO rollout, old pod
//                                keeps serving + keeps polling for the
//                                next (hopefully fixed) commit.
//   2. that builds but crashes on startup  →  rolling update is configured
//                                with maxUnavailable=0 in manifest 04, so
//                                the old pod keeps serving until the new
//                                one passes readiness. Crashlooping pod
//                                never replaces the working one.
//   3. that builds, starts, but breaks THIS code path  →  manual escape
//                                hatch: scripts/rebuild-control-plane.sh
//                                bypasses the control-plane entirely.
//
// State: a ConfigMap "control-plane-rebuilder-state" in paas-system holds
// the SHA we last built. Bootstrap on first run reads the SHA but does
// NOT trigger a build — we assume whatever's already running matches the
// remote on initial deploy.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	selfRepoFullName       = "jamilshaikh07/vercel-clone"
	selfRepoBranch         = "main"
	rebuilderPollInterval  = 90 * time.Second
	rebuilderInitialDelay  = 30 * time.Second
	rebuilderStateCM       = "control-plane-rebuilder-state"
	rebuilderJobName       = "build-control-plane"
	rebuilderDeployName    = "control-plane"
	rebuilderNamespace     = "paas-system"
	rebuilderBuildTimeout  = 8 * time.Minute
	// Kaniko destination — must match the deployment's image: field so the
	// rolled pod pulls the just-built layer.
	rebuilderImageRef = "ttl.sh/spinup-control-plane-05836ffc:24h"
)

// startSelfRebuilder spawns the poller goroutine. Returns immediately; the
// goroutine lives until ctx is cancelled (server shutdown).
func startSelfRebuilder(ctx context.Context, k *kubeClient, log *slog.Logger) {
	go func() {
		log = log.With("subsystem", "self-rebuilder")
		log.Info("starting self-rebuilder",
			"repo", selfRepoFullName, "branch", selfRepoBranch,
			"interval", rebuilderPollInterval)
		// First tick after a small delay so we don't slam GitHub on every
		// restart — multiple pods rolling out in quick succession would
		// otherwise each fire an immediate poll.
		select {
		case <-ctx.Done():
			return
		case <-time.After(rebuilderInitialDelay):
		}
		t := time.NewTicker(rebuilderPollInterval)
		defer t.Stop()
		// Run an immediate tick after the initial delay, then on every interval.
		for {
			if err := rebuilderTick(ctx, k, log); err != nil {
				log.Warn("tick failed", "err", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

func rebuilderTick(ctx context.Context, k *kubeClient, log *slog.Logger) error {
	tickCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	latest, err := githubLatestSHA(tickCtx, selfRepoFullName, selfRepoBranch)
	if err != nil {
		return fmt.Errorf("get latest SHA: %w", err)
	}

	lastBuilt, err := readRebuilderState(tickCtx, k)
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}

	// Bootstrap: first run after the ConfigMap doesn't exist yet. Record
	// the current SHA so we don't immediately rebuild on every new pod.
	if lastBuilt == "" {
		log.Info("bootstrapping rebuilder state", "sha", latest[:8])
		return writeRebuilderState(ctx, k, latest)
	}

	if lastBuilt == latest {
		return nil
	}

	log.Info("new commit detected — rebuilding",
		"old_sha", lastBuilt[:8], "new_sha", latest[:8])

	// We hand the rebuild path its own context so it isn't cut by the
	// short tickCtx — the actual build can take up to 5 minutes.
	buildCtx, buildCancel := context.WithTimeout(ctx, rebuilderBuildTimeout)
	defer buildCancel()

	if err := triggerBuildJob(buildCtx, k, log); err != nil {
		return fmt.Errorf("trigger build: %w", err)
	}
	if err := rolloutRestart(buildCtx, k, log); err != nil {
		return fmt.Errorf("rollout: %w", err)
	}
	if err := writeRebuilderState(buildCtx, k, latest); err != nil {
		return fmt.Errorf("save state: %w", err)
	}
	log.Info("rebuild complete", "sha", latest[:8])
	return nil
}

// githubLatestSHA hits the public REST endpoint. Unauthenticated, so
// rate-limited to 60/hr/IP — at our 90s poll cadence we burn ≈40/hr.
// Returns the SHA on success or a wrapped error on transport/parse failure.
func githubLatestSHA(ctx context.Context, fullName, branch string) (string, error) {
	u := fmt.Sprintf("https://api.github.com/repos/%s/branches/%s",
		fullName, url.PathEscape(branch))
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "vercel-clone-self-rebuilder/1.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		// Surface the rate-limit-reset header on 403 so we can see it in logs.
		extra := ""
		if reset := resp.Header.Get("X-RateLimit-Reset"); reset != "" {
			extra = " ratelimit_reset=" + reset
		}
		return "", fmt.Errorf("HTTP %d%s", resp.StatusCode, extra)
	}
	var body struct {
		Commit struct {
			SHA string `json:"sha"`
		} `json:"commit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}
	if len(body.Commit.SHA) != 40 {
		return "", fmt.Errorf("unexpected sha format: %q", body.Commit.SHA)
	}
	return body.Commit.SHA, nil
}

// readRebuilderState fetches last_sha from the state ConfigMap. Returns
// ("", nil) when the CM doesn't exist yet — that's the bootstrap case.
func readRebuilderState(ctx context.Context, k *kubeClient) (string, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/configmaps/%s",
		rebuilderNamespace, rebuilderStateCM)
	status, body, err := k.do(ctx, "GET", path, nil)
	if err != nil {
		return "", err
	}
	if status == 404 {
		return "", nil
	}
	if status != 200 {
		return "", fmt.Errorf("HTTP %d: %s", status, snippet(body))
	}
	var cm struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &cm); err != nil {
		return "", err
	}
	return cm.Data["last_sha"], nil
}

// writeRebuilderState upserts last_sha into the state ConfigMap. Uses
// kubeClient.apply which creates-or-updates depending on whether the CM
// already exists.
func writeRebuilderState(ctx context.Context, k *kubeClient, sha string) error {
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      rebuilderStateCM,
			"namespace": rebuilderNamespace,
		},
		"data": map[string]string{
			"last_sha":   sha,
			"updated_at": time.Now().UTC().Format(time.RFC3339),
		},
	}
	collection := fmt.Sprintf("/api/v1/namespaces/%s/configmaps", rebuilderNamespace)
	return k.apply(ctx, collection, rebuilderStateCM, obj)
}

// triggerBuildJob recreates the Kaniko Job (Jobs are immutable so we can't
// just resubmit) and blocks until it reaches Complete/Failed. Returns nil
// only on Complete.
func triggerBuildJob(ctx context.Context, k *kubeClient, log *slog.Logger) error {
	// 1. Delete any existing Job + its pods. propagationPolicy=Background
	//    lets the API return quickly while the GC cleans up pods.
	delPath := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs/%s?propagationPolicy=Background",
		rebuilderNamespace, rebuilderJobName)
	if status, _, err := k.do(ctx, "DELETE", delPath, nil); err != nil {
		return fmt.Errorf("delete old job: %w", err)
	} else if status != 200 && status != 202 && status != 404 {
		return fmt.Errorf("delete old job HTTP %d", status)
	}
	// Wait for the old job + its pods to actually disappear, otherwise
	// the create below races against a name-collision.
	if err := waitForJobGone(ctx, k); err != nil {
		return fmt.Errorf("wait for old job gone: %w", err)
	}

	// 2. Create new Job — identical shape to manifests/03-build-control-plane.yaml.
	job := map[string]any{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata": map[string]any{
			"name":      rebuilderJobName,
			"namespace": rebuilderNamespace,
			"labels": map[string]string{
				"app.kubernetes.io/component": "builder",
				"app.kubernetes.io/part-of":   "paas",
				"paas.trigger":                "self-rebuilder",
			},
		},
		"spec": map[string]any{
			"backoffLimit":            0,
			"ttlSecondsAfterFinished": 3600,
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": map[string]string{
						"app.kubernetes.io/component": "builder",
					},
				},
				"spec": map[string]any{
					"restartPolicy": "Never",
					"containers": []any{
						map[string]any{
							"name":  "kaniko",
							"image": "gcr.io/kaniko-project/executor:v1.23.2",
							"args": []string{
								"--context=git://github.com/" + selfRepoFullName + ".git#refs/heads/" + selfRepoBranch,
								"--context-sub-path=control-plane",
								"--dockerfile=Dockerfile",
								"--destination=" + rebuilderImageRef,
								"--snapshot-mode=redo",
								"--use-new-run",
								"--cache=false",
								"--verbosity=info",
							},
							"resources": map[string]any{
								"requests": map[string]string{"cpu": "500m", "memory": "1Gi"},
								"limits":   map[string]string{"cpu": "2", "memory": "4Gi"},
							},
						},
					},
				},
			},
		},
	}
	createPath := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs", rebuilderNamespace)
	if status, body, err := k.do(ctx, "POST", createPath, job); err != nil {
		return fmt.Errorf("create job: %w", err)
	} else if status < 200 || status >= 300 {
		return fmt.Errorf("create job HTTP %d: %s", status, snippet(body))
	}
	log.Info("build job created — waiting for completion")

	// 3. Poll status. Kaniko build is typically 60-120s.
	deadline := time.Now().Add(rebuilderBuildTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(4 * time.Second):
		}
		ok, done, err := jobOutcome(ctx, k)
		if err != nil {
			log.Warn("status check failed (will retry)", "err", err)
			continue
		}
		if !done {
			continue
		}
		if !ok {
			return fmt.Errorf("build job failed")
		}
		log.Info("build job succeeded")
		return nil
	}
	return fmt.Errorf("build timed out after %s", rebuilderBuildTimeout)
}

// waitForJobGone polls until the build Job is fully deleted. The Background
// propagation policy can leave the object around briefly while pods are GC'd.
func waitForJobGone(ctx context.Context, k *kubeClient) error {
	path := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs/%s",
		rebuilderNamespace, rebuilderJobName)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, _, err := k.do(ctx, "GET", path, nil)
		if err != nil {
			return err
		}
		if status == 404 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("old job still present after 30s")
}

// jobOutcome inspects the Job status. Returns (success, done, err).
func jobOutcome(ctx context.Context, k *kubeClient) (bool, bool, error) {
	path := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs/%s",
		rebuilderNamespace, rebuilderJobName)
	status, body, err := k.do(ctx, "GET", path, nil)
	if err != nil {
		return false, false, err
	}
	if status != 200 {
		return false, false, fmt.Errorf("HTTP %d", status)
	}
	var j struct {
		Status struct {
			Succeeded  int `json:"succeeded"`
			Failed     int `json:"failed"`
			Conditions []struct {
				Type   string `json:"type"`
				Status string `json:"status"`
			} `json:"conditions"`
		} `json:"status"`
	}
	if err := json.Unmarshal(body, &j); err != nil {
		return false, false, err
	}
	for _, c := range j.Status.Conditions {
		if c.Status != "True" {
			continue
		}
		switch c.Type {
		case "Complete":
			return true, true, nil
		case "Failed":
			return false, true, nil
		}
	}
	return false, false, nil
}

// rolloutRestart adds a kubectl.kubernetes.io/restartedAt annotation to the
// Deployment's pod template — the canonical idiom kubectl uses to trigger
// a rolling restart without changing the image reference. A strategic-merge
// PATCH is the smallest possible mutation.
func rolloutRestart(ctx context.Context, k *kubeClient, log *slog.Logger) error {
	patch := map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{
					"annotations": map[string]string{
						"kubectl.kubernetes.io/restartedAt": time.Now().UTC().Format(time.RFC3339),
					},
				},
			},
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/apis/apps/v1/namespaces/%s/deployments/%s",
		rebuilderNamespace, rebuilderDeployName)
	u := k.base + path
	req, err := http.NewRequestWithContext(ctx, "PATCH", u, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/strategic-merge-patch+json")
	resp, err := k.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("patch deployment HTTP %d", resp.StatusCode)
	}
	log.Info("deployment rollout triggered")
	return nil
}
