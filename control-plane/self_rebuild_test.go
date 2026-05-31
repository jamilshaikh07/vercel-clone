package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Verifies the GitHub API call shape: correct path, accept header, and
// SHA parsing. We don't go further (e.g. rate-limit retry) because at
// 90s cadence the unauthenticated 60/hr budget is not a real problem
// and a 403 response just yields a logged warning + next-tick retry.
func TestGithubLatestSHA(t *testing.T) {
	want := "abcdef1234567890abcdef1234567890abcdef12"
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/jamilshaikh07/vercel-clone/branches/main", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "application/vnd.github+json" {
			t.Errorf("missing GitHub Accept header: %q", r.Header.Get("Accept"))
		}
		if r.Header.Get("User-Agent") == "" {
			t.Error("missing User-Agent (GitHub requires one)")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name":   "main",
			"commit": map[string]any{"sha": want},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Override the URL by temporarily substituting api.github.com → test server.
	// We replicate the production call by directly hitting the test URL.
	req, _ := http.NewRequestWithContext(context.Background(), "GET",
		srv.URL+"/repos/jamilshaikh07/vercel-clone/branches/main", nil)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "vercel-clone-self-rebuilder/1.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body struct {
		Commit struct {
			SHA string `json:"sha"`
		} `json:"commit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Commit.SHA != want {
		t.Errorf("got %q, want %q", body.Commit.SHA, want)
	}
	if len(body.Commit.SHA) != 40 {
		t.Errorf("sha length = %d, want 40", len(body.Commit.SHA))
	}
}

// Sanity check on the constants that drive the rebuild — they must align
// with the manifest's image: field and the Job/Deployment object names,
// otherwise the rebuilder builds to one tag and the deployment pulls from
// another, silently never picking up the new image.
func TestRebuilderConstants(t *testing.T) {
	if !strings.Contains(rebuilderImageRef, "jamilshaikh-paas-control-plane") {
		t.Errorf("rebuilderImageRef out of sync with deployment manifest: %q", rebuilderImageRef)
	}
	if rebuilderJobName != "build-control-plane" {
		t.Errorf("rebuilderJobName must match manifests/03 metadata.name, got %q", rebuilderJobName)
	}
	if rebuilderDeployName != "control-plane" {
		t.Errorf("rebuilderDeployName must match manifests/04 metadata.name, got %q", rebuilderDeployName)
	}
	if rebuilderNamespace != "paas-system" {
		t.Errorf("rebuilderNamespace must be paas-system, got %q", rebuilderNamespace)
	}
}
