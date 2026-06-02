package main

// Minimal GitHub REST client for opening a "Spinup SEO fixes" PR.
//
// We use the contents-API shortcut (PUT /contents/{path}) rather than
// the lower-level git-data API (blob -> tree -> commit -> ref). Three
// HTTP calls instead of five, and the request bodies are flat —
// suitable for a one-file-per-PR feature. If we ever need to commit
// multiple files atomically (or files larger than a few hundred KB —
// the Contents API has a 1 MiB body cap) we'd swap to git-data.
//
// Authentication uses the installation token already cached by the
// existing githubApp wrapper. The handler picks the installation_id
// up from the project row so the token scope is automatically
// limited to the user's own repos.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// ghRepoMeta is the subset of GET /repos/{owner}/{repo} that we use.
type ghRepoMeta struct {
	Description   string `json:"description"`
	DefaultBranch string `json:"default_branch"`
}

// ghFile is the subset of GET /repos/.../contents/{path} we care about.
// `content` is base64 with newlines per GitHub's API convention; the
// helper below decodes it.
type ghFile struct {
	SHA     string `json:"sha"`
	Content string `json:"content"`
}

// ghPRResp is the subset of POST /repos/.../pulls that we return up.
type ghPRResp struct {
	HTMLURL string `json:"html_url"`
	Number  int    `json:"number"`
}

// ghDo wraps the existing HTTP client with the boilerplate headers
// every GitHub call needs. token is the installation token, never the
// app JWT.
func (g *githubApp) ghDo(
	ctx context.Context, method, url, token string, body any,
) (*http.Response, []byte, error) {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal body: %w", err)
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := g.http.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return resp, respBody, nil
}

// getRepoMeta returns the live repository description + default branch.
// Used by the SEO patcher as a source for the meta description.
func (g *githubApp) getRepoMeta(
	ctx context.Context, installationID int64, repoFullName string,
) (*ghRepoMeta, error) {
	tok, err := g.installationToken(ctx, installationID)
	if err != nil {
		return nil, err
	}
	url := "https://api.github.com/repos/" + repoFullName
	resp, body, err := g.ghDo(ctx, "GET", url, tok, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("get repo: HTTP %d: %s", resp.StatusCode, snippetOfBody(body))
	}
	var out ghRepoMeta
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode repo: %w", err)
	}
	return &out, nil
}

// getFile returns the decoded contents + the file's SHA so we can
// safely PUT an update. Returns (nil, "", nil) on 404 so the caller
// can fall through to the next candidate path without treating
// 'missing' as an error.
func (g *githubApp) getFile(
	ctx context.Context, installationID int64, repoFullName, path, ref string,
) (content []byte, sha string, err error) {
	tok, err := g.installationToken(ctx, installationID)
	if err != nil {
		return nil, "", err
	}
	url := fmt.Sprintf("https://api.github.com/repos/%s/contents/%s",
		repoFullName, path)
	if ref != "" {
		url += "?ref=" + ref
	}
	resp, body, err := g.ghDo(ctx, "GET", url, tok, nil)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode == 404 {
		return nil, "", nil
	}
	if resp.StatusCode != 200 {
		return nil, "", fmt.Errorf("get file %s: HTTP %d: %s",
			path, resp.StatusCode, snippetOfBody(body))
	}
	var f ghFile
	if err := json.Unmarshal(body, &f); err != nil {
		return nil, "", fmt.Errorf("decode file: %w", err)
	}
	// GitHub returns base64 with embedded newlines every 60 chars.
	clean := strings.ReplaceAll(f.Content, "\n", "")
	decoded, err := base64.StdEncoding.DecodeString(clean)
	if err != nil {
		return nil, "", fmt.Errorf("decode b64: %w", err)
	}
	return decoded, f.SHA, nil
}

// getBranchSHA returns the commit SHA at the tip of a branch. Used to
// know what point to fork the new "spinup/seo-fixes-…" branch off of.
func (g *githubApp) getBranchSHA(
	ctx context.Context, installationID int64, repoFullName, branch string,
) (string, error) {
	tok, err := g.installationToken(ctx, installationID)
	if err != nil {
		return "", err
	}
	url := fmt.Sprintf("https://api.github.com/repos/%s/git/ref/heads/%s",
		repoFullName, branch)
	resp, body, err := g.ghDo(ctx, "GET", url, tok, nil)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("get branch ref %s: HTTP %d: %s",
			branch, resp.StatusCode, snippetOfBody(body))
	}
	var r struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return "", fmt.Errorf("decode ref: %w", err)
	}
	if r.Object.SHA == "" {
		return "", errors.New("empty SHA in branch ref")
	}
	return r.Object.SHA, nil
}

// createBranch points a new ref at an existing commit. Idempotent-ish:
// returns nil if the branch already exists pointing at the same SHA
// (a stale leftover from a previous PR attempt). Different-SHA collision
// is bubbled up so the caller can pick a fresh suffix.
func (g *githubApp) createBranch(
	ctx context.Context, installationID int64, repoFullName, newBranch, fromSHA string,
) error {
	tok, err := g.installationToken(ctx, installationID)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://api.github.com/repos/%s/git/refs", repoFullName)
	resp, body, err := g.ghDo(ctx, "POST", url, tok, map[string]string{
		"ref": "refs/heads/" + newBranch,
		"sha": fromSHA,
	})
	if err != nil {
		return err
	}
	// 201 Created; 422 Unprocessable Entity if the ref already exists.
	if resp.StatusCode == 201 {
		return nil
	}
	if resp.StatusCode == 422 && bytes.Contains(body, []byte("Reference already exists")) {
		return errAlreadyExists
	}
	return fmt.Errorf("create branch %s: HTTP %d: %s",
		newBranch, resp.StatusCode, snippetOfBody(body))
}

// putFile creates-or-updates a file on a given branch in one call. If
// fileSHA is empty the API treats it as a create; non-empty means update
// (and the SHA must match the current file SHA, or we get 409).
func (g *githubApp) putFile(
	ctx context.Context, installationID int64,
	repoFullName, path, branch, message string,
	content []byte, fileSHA string,
) error {
	tok, err := g.installationToken(ctx, installationID)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://api.github.com/repos/%s/contents/%s",
		repoFullName, path)
	body := map[string]any{
		"message": message,
		"branch":  branch,
		"content": base64.StdEncoding.EncodeToString(content),
	}
	if fileSHA != "" {
		body["sha"] = fileSHA
	}
	resp, respBody, err := g.ghDo(ctx, "PUT", url, tok, body)
	if err != nil {
		return err
	}
	// 200 (updated) or 201 (created) are both success.
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("put file %s: HTTP %d: %s",
			path, resp.StatusCode, snippetOfBody(respBody))
	}
	return nil
}

// createPR opens a (optionally draft) pull request and returns the
// browser-friendly HTMLURL so the dashboard can deep-link straight to it.
func (g *githubApp) createPR(
	ctx context.Context, installationID int64,
	repoFullName, title, body, headBranch, baseBranch string, draft bool,
) (*ghPRResp, error) {
	tok, err := g.installationToken(ctx, installationID)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("https://api.github.com/repos/%s/pulls", repoFullName)
	reqBody := map[string]any{
		"title": title,
		"body":  body,
		"head":  headBranch,
		"base":  baseBranch,
		"draft": draft,
		// Don't auto-create deeper changes; reviewers expect a clean PR
		// they can merge with one click.
		"maintainer_can_modify": true,
	}
	resp, respBody, err := g.ghDo(ctx, "POST", url, tok, reqBody)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 201 {
		return nil, fmt.Errorf("create PR: HTTP %d: %s",
			resp.StatusCode, snippetOfBody(respBody))
	}
	var out ghPRResp
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, fmt.Errorf("decode PR: %w", err)
	}
	return &out, nil
}

// snippetOfBody returns at most 200 bytes of a GitHub error response so
// our own error messages stay readable in logs without dumping a multi-KB
// HTML page on a stray 5xx.
func snippetOfBody(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}

// errAlreadyExists is returned by createBranch when the target ref already
// exists. Callers should pick a fresh suffix and retry.
var errAlreadyExists = errors.New("branch already exists")
