package main

// GitHub App auth — JWT (signed with the App's RSA private key) used to
// mint short-lived per-installation tokens that we then use to clone
// private repos and post status checks.
//
// We use only the standard library: crypto/rsa + crypto/sha256 + base64.
// jwt libraries are overkill for the ~25 lines this requires.
//
// Tokens are cached in-process for their useful lifetime (GitHub returns
// 60 minutes; we refresh ~5 minutes early to absorb clock skew).

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type githubApp struct {
	appID      int64
	privateKey *rsa.PrivateKey
	http       *http.Client

	mu    sync.Mutex
	cache map[int64]cachedToken
}

type cachedToken struct {
	token     string
	expiresAt time.Time
}

func loadGitHubApp(appIDStr, pemPath string) (*githubApp, error) {
	appID, err := parseAppID(appIDStr)
	if err != nil {
		return nil, err
	}
	pemBytes, err := os.ReadFile(pemPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", pemPath, err)
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("private key pem is not valid")
	}

	var key *rsa.PrivateKey
	switch block.Type {
	case "RSA PRIVATE KEY":
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	case "PRIVATE KEY":
		anyKey, err2 := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err2 != nil {
			return nil, fmt.Errorf("parse PKCS8: %w", err2)
		}
		var ok bool
		key, ok = anyKey.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("private key is not RSA")
		}
	default:
		return nil, fmt.Errorf("unsupported pem type %q", block.Type)
	}
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	return &githubApp{
		appID:      appID,
		privateKey: key,
		http:       &http.Client{Timeout: 15 * time.Second},
		cache:      make(map[int64]cachedToken),
	}, nil
}

func parseAppID(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty app id")
	}
	var id int64
	if _, err := fmt.Sscan(s, &id); err != nil {
		return 0, fmt.Errorf("invalid app id %q: %w", s, err)
	}
	return id, nil
}

// signAppJWT returns a JWT valid for ~9 minutes. GitHub's limit is 10.
func (g *githubApp) signAppJWT() (string, error) {
	now := time.Now()
	header := map[string]string{"alg": "RS256", "typ": "JWT"}
	payload := map[string]any{
		// iat backdated 30s to absorb clock skew between us and GitHub
		"iat": now.Add(-30 * time.Second).Unix(),
		"exp": now.Add(9 * time.Minute).Unix(),
		"iss": g.appID,
	}
	hb, _ := json.Marshal(header)
	pb, _ := json.Marshal(payload)
	enc := base64.RawURLEncoding
	signingInput := enc.EncodeToString(hb) + "." + enc.EncodeToString(pb)

	sum := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, g.privateKey, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	return signingInput + "." + enc.EncodeToString(sig), nil
}

// installationToken returns a (possibly cached) installation access token.
// Refresh happens once a token is within 5 minutes of expiry.
func (g *githubApp) installationToken(ctx context.Context, installationID int64) (string, error) {
	g.mu.Lock()
	if c, ok := g.cache[installationID]; ok && time.Until(c.expiresAt) > 5*time.Minute {
		tok := c.token
		g.mu.Unlock()
		return tok, nil
	}
	g.mu.Unlock()

	jwt, err := g.signAppJWT()
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	url := fmt.Sprintf("https://api.github.com/app/installations/%d/access_tokens", installationID)
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := g.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("installation token: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var r struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	if r.Token == "" {
		return "", errors.New("empty token in response")
	}

	g.mu.Lock()
	g.cache[installationID] = cachedToken{token: r.Token, expiresAt: r.ExpiresAt}
	g.mu.Unlock()
	return r.Token, nil
}

// setCommitStatus posts a Status check to the given commit. `state` must be
// one of: pending, success, failure, error. Description is truncated to 140
// chars (GitHub's limit). Returns nil on 2xx, error otherwise. Callers
// generally want to log-and-swallow — a missing status check shouldn't fail
// an otherwise-good build.
func (g *githubApp) setCommitStatus(
	ctx context.Context,
	installationID int64,
	repoFullName, sha, state, description, targetURL, context string,
) error {
	if len(description) > 140 {
		description = description[:137] + "..."
	}
	tok, err := g.installationToken(ctx, installationID)
	if err != nil {
		return fmt.Errorf("mint token: %w", err)
	}
	body, err := json.Marshal(map[string]string{
		"state":       state,
		"target_url":  targetURL,
		"description": description,
		"context":     context,
	})
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://api.github.com/repos/%s/statuses/%s", repoFullName, sha)
	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("set status: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

// fetchBranchSHA returns the HEAD commit SHA of `branch` on `repoFullName`,
// authenticated as the App installation. Used when we need to start a
// build but don't have a `push` event to read the SHA from — i.e. the
// auto-deploy fired after a repo gets added to the App (when the user
// installed *after* their last push), and the dashboard's manual
// "Deploy now" button.
//
// Returns the bare commit SHA (no ref/branch prefix); callers are
// responsible for constructing "refs/heads/<branch>" if they need it.
func (g *githubApp) fetchBranchSHA(ctx context.Context, installationID int64, repoFullName, branch string) (string, error) {
	tok, err := g.installationToken(ctx, installationID)
	if err != nil {
		return "", fmt.Errorf("mint token: %w", err)
	}
	url := fmt.Sprintf("https://api.github.com/repos/%s/branches/%s", repoFullName, branch)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := g.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("fetch branch %q: HTTP %d: %s",
			branch, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	// Response shape (trimmed):
	//   { "name": "main", "commit": { "sha": "abc...", ... }, ... }
	var r struct {
		Commit struct {
			SHA string `json:"sha"`
		} `json:"commit"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return "", fmt.Errorf("decode branch response: %w", err)
	}
	if r.Commit.SHA == "" {
		return "", errors.New("github returned empty sha for branch")
	}
	return r.Commit.SHA, nil
}

// fetchRepoDefaultBranch returns the repo's current default branch name
// (typically "main" or "master"). Used as a fallback when our recorded
// production_branch turns out to be wrong — installation_repositories
// webhook payloads don't include default_branch, so projects created
// from that flow start with a hardcoded "main" guess that fails for
// repos using "master" (or any custom default).
func (g *githubApp) fetchRepoDefaultBranch(ctx context.Context, installationID int64, repoFullName string) (string, error) {
	tok, err := g.installationToken(ctx, installationID)
	if err != nil {
		return "", fmt.Errorf("mint token: %w", err)
	}
	url := fmt.Sprintf("https://api.github.com/repos/%s", repoFullName)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := g.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("fetch repo %q: HTTP %d: %s",
			repoFullName, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var r struct {
		DefaultBranch string `json:"default_branch"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return "", fmt.Errorf("decode repo response: %w", err)
	}
	if r.DefaultBranch == "" {
		return "", errors.New("github returned empty default_branch for repo")
	}
	return r.DefaultBranch, nil
}
