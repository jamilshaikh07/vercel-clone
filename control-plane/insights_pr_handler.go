package main

// POST /v1/projects/{id}/insights/suggest-pr
//
// One-click "open a PR with heuristic SEO fixes" for a project. The
// flow is intentionally synchronous (no job queue) — every step is
// either a fast GitHub call or a small CPU-bound transformation, and
// returning the resulting PR URL immediately gives the user a
// satisfying single click → "✓ PR #42 opened" experience.
//
// Failure semantics: any partial progress (a branch created but the
// PR creation failed) leaves the branch on the repo. The user can
// either delete it manually or click again — the handler picks a
// fresh timestamped branch name on each invocation, so retries don't
// collide.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type suggestPRResp struct {
	OK         bool       `json:"ok"`
	PRURL      string     `json:"pr_url,omitempty"`
	PRNumber   int        `json:"pr_number,omitempty"`
	BranchName string     `json:"branch,omitempty"`
	FilePath   string     `json:"file_path,omitempty"`
	Applied    []seoPatch `json:"applied,omitempty"`
	Skipped    []seoPatch `json:"skipped,omitempty"`
	Message    string     `json:"message,omitempty"` // user-facing summary line
}

func (s *server) handleSuggestInsightsPR(w http.ResponseWriter, r *http.Request) {
	// Re-use the deeper project lookup so we get installation_id +
	// repo full_name + production_branch in one query. authoriseProject
	// only returns the lite projectInfo (no GH fields) — sufficient for
	// status/env/etc. but not for talking to GitHub.
	id := projectIDFromPath(w, r)
	if id == "" {
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
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	target, err := s.store.GetProjectForDeploy(ctx, id, scope)
	if err != nil {
		s.log.Error("get project (suggest-pr) failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if target == nil {
		http.NotFound(w, r)
		return
	}

	// --- 1. Find the live URL (for canonical/og:url + sanity check) -------
	deps, err := s.store.ListDeploymentsForProjects(ctx, []string{target.ProjectID}, 10)
	if err != nil {
		s.log.Error("list deployments (suggest-pr) failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	var liveURL string
	for _, d := range deps[target.ProjectID] {
		if d.Status == "ready" && d.URL != nil && *d.URL != "" {
			liveURL = *d.URL
			break
		}
	}
	if liveURL == "" {
		writeJSON(w, http.StatusOK, &suggestPRResp{
			Message: "No READY deployment yet — push code and let it deploy before generating a PR.",
		})
		return
	}

	// --- 2. Pull repo metadata (description fuels meta description) -------
	meta, err := s.gh.getRepoMeta(ctx, target.InstallationID, target.RepoFullName)
	if err != nil {
		s.log.Warn("get repo meta failed", "err", err)
		// Soft-fail: continue with empty description; the patcher will
		// just skip the description-dependent patches.
		meta = &ghRepoMeta{DefaultBranch: target.ProductionBranch}
	}
	baseBranch := target.ProductionBranch
	if baseBranch == "" {
		baseBranch = meta.DefaultBranch
	}
	if baseBranch == "" {
		baseBranch = "main"
	}

	// --- 3. Pick the first existing entry HTML file ------------------------
	var (
		entryPath string
		original  []byte
		fileSHA   string
	)
	for _, candidate := range suggestPREntryCandidates() {
		content, sha, ferr := s.gh.getFile(ctx, target.InstallationID,
			target.RepoFullName, candidate, baseBranch)
		if ferr != nil {
			s.log.Warn("get file probe failed",
				"path", candidate, "err", ferr)
			continue
		}
		if content == nil {
			continue
		}
		entryPath, original, fileSHA = candidate, content, sha
		break
	}
	if entryPath == "" {
		writeJSON(w, http.StatusOK, &suggestPRResp{
			Message: "Couldn't find a recognisable HTML entry file (tried " +
				strings.Join(suggestPREntryCandidates(), ", ") +
				"). The heuristic patcher only supports static HTML entries for now — framework templates (Next, Astro components, etc.) need manual edits.",
		})
		return
	}

	// --- 4. Run the patcher; bail early if nothing to do ------------------
	repoShort := target.RepoFullName
	if i := strings.LastIndexByte(repoShort, '/'); i >= 0 {
		repoShort = repoShort[i+1:]
	}
	newHTML, patches := applySEOPatches(string(original), seoInputs{
		RepoName:    repoShort,
		RepoDesc:    meta.Description,
		LiveURL:     liveURL,
		DefaultLang: "en",
	})

	applied, skipped := splitPatches(patches)
	if len(applied) == 0 {
		writeJSON(w, http.StatusOK, &suggestPRResp{
			FilePath: entryPath,
			Skipped:  skipped,
			Message:  "Nothing to do — every SEO signal we know how to add is already present in " + entryPath + ".",
		})
		return
	}
	if newHTML == string(original) {
		// Defensive: applySEOPatches should never claim "applied" without
		// changing the bytes. If we get here, the diff would be empty
		// and GitHub would reject a no-op commit.
		writeJSON(w, http.StatusOK, &suggestPRResp{
			FilePath: entryPath,
			Applied:  applied,
			Skipped:  skipped,
			Message:  "No net changes after patching — file already covered.",
		})
		return
	}

	// --- 5. Branch + commit + PR -----------------------------------------
	baseSHA, err := s.gh.getBranchSHA(ctx, target.InstallationID,
		target.RepoFullName, baseBranch)
	if err != nil {
		respondGitHubErr(w, s.log, "branch lookup", err)
		return
	}

	branchName := "spinup/seo-fixes-" + time.Now().UTC().Format("20060102-150405")
	if err := s.gh.createBranch(ctx, target.InstallationID,
		target.RepoFullName, branchName, baseSHA); err != nil {
		if errors.Is(err, errAlreadyExists) {
			// Astronomically unlikely (per-second resolution) but be polite.
			branchName += "-r"
			if err := s.gh.createBranch(ctx, target.InstallationID,
				target.RepoFullName, branchName, baseSHA); err != nil {
				respondGitHubErr(w, s.log, "branch create (retry)", err)
				return
			}
		} else {
			respondGitHubErr(w, s.log, "branch create", err)
			return
		}
	}

	commitMsg := fmt.Sprintf("seo: add %d missing meta tag%s to %s",
		len(applied), plural(len(applied)), entryPath)
	if err := s.gh.putFile(ctx, target.InstallationID,
		target.RepoFullName, entryPath, branchName,
		commitMsg, []byte(newHTML), fileSHA); err != nil {
		respondGitHubErr(w, s.log, "file write", err)
		return
	}

	prBody := buildPRBody(applied, skipped, entryPath, liveURL)
	pr, err := s.gh.createPR(ctx, target.InstallationID,
		target.RepoFullName,
		"Spinup: SEO improvements for "+entryPath,
		prBody,
		branchName, baseBranch, true /* draft */)
	if err != nil {
		respondGitHubErr(w, s.log, "PR create", err)
		return
	}

	writeJSON(w, http.StatusOK, &suggestPRResp{
		OK:         true,
		PRURL:      pr.HTMLURL,
		PRNumber:   pr.Number,
		BranchName: branchName,
		FilePath:   entryPath,
		Applied:    applied,
		Skipped:    skipped,
		Message: fmt.Sprintf("Opened draft PR #%d with %d fix%s for %s.",
			pr.Number, len(applied), pluralEs(len(applied)), entryPath),
	})
}

// splitPatches partitions the patches list into the ones we wrote into
// the HTML and the ones we deliberately skipped (so the user can see
// "we couldn't add a meta description because the GitHub repo description
// is empty" rather than a silent omission).
func splitPatches(all []seoPatch) (applied, skipped []seoPatch) {
	for _, p := range all {
		if p.Applied {
			applied = append(applied, p)
		} else {
			skipped = append(skipped, p)
		}
	}
	return
}

// buildPRBody renders the markdown body of the PR. We list every change
// with the snippet that was added so a reviewer can see the diff at a
// glance, and we explicitly call out the skips so the user can take
// action (e.g. "add a repo description") and re-run.
func buildPRBody(applied, skipped []seoPatch, entryPath, liveURL string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Automated SEO improvements based on the Insights report for [%s](%s).\n\n",
		liveURL, liveURL)
	fmt.Fprintf(&b, "**File changed:** `%s`\n\n", entryPath)
	if len(applied) > 0 {
		b.WriteString("### Added\n\n")
		for _, p := range applied {
			fmt.Fprintf(&b, "- **%s** — `%s`\n", p.Label, strings.TrimSpace(p.Snippet))
		}
		b.WriteString("\n")
	}
	if len(skipped) > 0 {
		b.WriteString("### Skipped (need your input)\n\n")
		for _, p := range skipped {
			fmt.Fprintf(&b, "- **%s** — %s\n", p.Label, p.Reason)
		}
		b.WriteString("\n")
	}
	b.WriteString("---\n")
	b.WriteString("_Opened by the Spinup dashboard's Insights → \"Open improvement PR\" button. ")
	b.WriteString("This PR is a **draft** — review, edit, and merge when you're happy with it. ")
	b.WriteString("Existing tags were left untouched; only missing ones were added._\n")
	return b.String()
}

// respondGitHubErr surfaces a useful error to the dashboard JSON
// response while logging the full detail server-side. GitHub permission
// failures (403) are translated to an actionable message; everything
// else gets the raw HTTP detail in development and a generic 502 in
// production.
func respondGitHubErr(w http.ResponseWriter, log interface {
	Error(msg string, args ...any)
}, stage string, err error) {
	log.Error("github call failed", "stage", stage, "err", err)
	msg := err.Error()
	status := http.StatusBadGateway
	switch {
	case strings.Contains(msg, "HTTP 403"):
		// Most common cause: the GitHub App install doesn't have
		// Contents:write or Pull-requests:write granted on this repo.
		status = http.StatusForbidden
		msg = "GitHub rejected the request with 403. The Spinup GitHub App needs the 'Contents' (read & write) and 'Pull requests' (read & write) permissions on this repo. Re-install or update the app's permissions on github.com → Settings → Applications → Spinup and try again."
	case strings.Contains(msg, "HTTP 404"):
		status = http.StatusNotFound
		msg = "GitHub returned 404 for " + stage + ". Either the branch was deleted between probes or the install lost access to the repo."
	}
	writeJSON(w, status, &suggestPRResp{Message: msg})
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
func pluralEs(n int) string {
	if n == 1 {
		return ""
	}
	return "es"
}

// (writeJSON lives in telemetry_handlers.go — shared across handlers.)
