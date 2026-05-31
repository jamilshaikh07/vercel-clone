package main

// /v1/version — surfaces what commit the running control-plane image was
// built from. Sourced from the self-rebuilder state ConfigMap (paas-system/
// control-plane-rebuilder-state, key 'last_sha'), so the value is always in
// lock-step with what the rebuilder actually built last.
//
// Two reasons we don't embed this at compile-time via -ldflags:
//   1. The Kaniko Job is the same for every build, so wiring a per-build SHA
//      into ldflags means rewriting the Job spec on each rebuild.
//   2. The rebuilder already maintains the source-of-truth CM; reading it
//      here is one round-trip and removes the duplication.
//
// Falls back to {"sha":"","short_sha":"dev"} when the CM doesn't exist
// (first-run before the rebuilder has had a chance to write it). That's
// also what local-dev builds will see, which is informative enough.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type versionResponse struct {
	SHA       string `json:"sha"`
	ShortSHA  string `json:"short_sha"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

func (s *server) handleVersion(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	v := versionResponse{ShortSHA: "dev"}

	// readRebuilderState is in self_rebuild.go — returns ("", nil) when
	// the CM doesn't exist yet, so the dev fallback below holds.
	sha, err := readRebuilderState(ctx, s.k8s)
	if err == nil && len(sha) >= 7 {
		v.SHA = sha
		v.ShortSHA = sha[:7]
	}

	// Best-effort fetch of updated_at — same CM, just a different key.
	// If it fails we still return the SHA so the UI gets something useful.
	if v.SHA != "" {
		if ua, err := readRebuilderStateField(ctx, s.k8s, "updated_at"); err == nil {
			v.UpdatedAt = ua
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

// readRebuilderStateField is a tiny helper that fetches one data key from the
// rebuilder state ConfigMap. Lives here (not in self_rebuild.go) because it
// only exists to serve this endpoint — keeping it scoped to its consumer.
func readRebuilderStateField(ctx context.Context, k *kubeClient, key string) (string, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/configmaps/%s",
		rebuilderNamespace, rebuilderStateCM)
	status, body, err := k.do(ctx, "GET", path, nil)
	if err != nil {
		return "", err
	}
	if status != 200 {
		return "", fmt.Errorf("HTTP %d", status)
	}
	var cm struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &cm); err != nil {
		return "", err
	}
	return cm.Data[key], nil
}
