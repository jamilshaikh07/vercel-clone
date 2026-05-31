package main

// Per-app SEO / production-readiness checks.
//
// The endpoint fetches the latest READY deployment URL, parses the HTML
// for a fixed set of SEO signals (title, meta description, viewport,
// canonical, OpenGraph), and probes /robots.txt + /sitemap.xml. Each
// check produces a {status, value, hint} triple; the overall score is
// a weighted % of passing checks.
//
// Why not Lighthouse / a real headless browser?
//   - 'real' browser-based scoring needs a Chromium image and several
//     seconds of CPU per check. For an MVP that surfaces obvious wins
//     (no <title>, no description, no viewport), a static-HTML scan
//     hits 80% of the value at 0.5s and zero dependencies.
//   - Performance/accessibility scoring is intentionally out of scope —
//     it's not the unique value our PaaS provides.
//
// Future: optional cron that runs the same checks periodically and
// stores a delta so we can show 'your SEO is improving' charts.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const (
	insightsFetchTimeout = 8 * time.Second
	insightsMaxBytes     = 1 << 20 // cap response read at 1 MiB
)

type insightCheck struct {
	ID     string `json:"id"`
	Label  string `json:"label"`
	Status string `json:"status"` // pass | warn | fail
	Value  string `json:"value,omitempty"`
	Hint   string `json:"hint,omitempty"`
	Weight int    `json:"weight"` // contribution to overall score, 1..5
}

type insightReport struct {
	URL          string         `json:"url"`
	CheckedAt    time.Time      `json:"checked_at"`
	Status       int            `json:"status"`
	FetchedMs    int64          `json:"fetched_ms"`
	PageSizeKB   int            `json:"page_size_kb"`
	Score        int            `json:"score"`           // 0..100
	Checks       []insightCheck `json:"checks"`
	FetchErr     string         `json:"fetch_err,omitempty"`
}

// handleProjectInsights generates a report on demand. We always re-run
// the fetch — caching could mask freshly-shipped fixes, and 8s is well
// inside the user's patience for a manual click.
func (s *server) handleProjectInsights(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	// Find the most recent READY deployment for this project. We pull
	// it via the existing per-project deployments list rather than a
	// dedicated query — list size is capped at 10 already so the cost
	// is trivial.
	deps, err := s.store.ListDeploymentsForProjects(r.Context(), []string{proj.ID}, 10)
	if err != nil {
		s.log.Error("list deployments failed", "project_id", proj.ID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	var liveURL string
	for _, d := range deps[proj.ID] {
		if d.Status == "ready" && d.URL != nil && *d.URL != "" {
			liveURL = *d.URL
			break
		}
	}
	if liveURL == "" {
		writeJSON(w, http.StatusOK, &insightReport{
			CheckedAt: time.Now().UTC(),
			FetchErr:  "no READY deployment yet — push code to your repo, then check back",
		})
		return
	}
	rpt := runInsightChecks(r.Context(), liveURL)
	writeJSON(w, http.StatusOK, rpt)
}

// runInsightChecks does the actual fetch + analysis. Split out so we
// could also call it from a future scheduled job without touching the
// HTTP layer.
func runInsightChecks(parent context.Context, target string) *insightReport {
	rpt := &insightReport{URL: target, CheckedAt: time.Now().UTC()}

	ctx, cancel := context.WithTimeout(parent, insightsFetchTimeout)
	defer cancel()

	client := &http.Client{Timeout: insightsFetchTimeout}
	req, err := http.NewRequestWithContext(ctx, "GET", target, nil)
	if err != nil {
		rpt.FetchErr = "bad URL: " + err.Error()
		return rpt
	}
	// A user-agent makes us look like a polite bot rather than a generic
	// Go http client — some apps gate on this for analytics correctness.
	req.Header.Set("User-Agent", "paas-insights/1.0 (+https://paas.jamilshaikh.in)")
	req.Header.Set("Accept", "text/html,application/xhtml+xml")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		rpt.FetchErr = "fetch failed: " + err.Error()
		return rpt
	}
	defer resp.Body.Close()
	rpt.Status = resp.StatusCode
	body, err := io.ReadAll(io.LimitReader(resp.Body, insightsMaxBytes))
	rpt.FetchedMs = time.Since(start).Milliseconds()
	if err != nil {
		rpt.FetchErr = "read body: " + err.Error()
		return rpt
	}
	rpt.PageSizeKB = (len(body) + 1023) / 1024
	html := string(body)

	// --- Run all checks. Each appendCheck() argument set is a single
	//     focussed assertion, so adding a new SEO signal stays a 3-line
	//     change rather than a sprawl. ---
	rpt.Checks = append(rpt.Checks, checkStatus(resp.StatusCode))
	rpt.Checks = append(rpt.Checks, checkContentType(resp.Header.Get("Content-Type")))
	rpt.Checks = append(rpt.Checks, checkHTTPS(target))
	rpt.Checks = append(rpt.Checks, checkTitle(html))
	rpt.Checks = append(rpt.Checks, checkMeta(html, "description", "Meta description",
		"50-160 chars summarising the page — shown in Google results."))
	rpt.Checks = append(rpt.Checks, checkViewport(html))
	rpt.Checks = append(rpt.Checks, checkCanonical(html, target))
	rpt.Checks = append(rpt.Checks, checkOG(html, "og:title", "OpenGraph title"))
	rpt.Checks = append(rpt.Checks, checkOG(html, "og:description", "OpenGraph description"))
	rpt.Checks = append(rpt.Checks, checkOG(html, "og:image", "OpenGraph image"))
	rpt.Checks = append(rpt.Checks, checkH1(html))
	rpt.Checks = append(rpt.Checks, checkLang(html))
	rpt.Checks = append(rpt.Checks, checkFavicon(html))
	rpt.Checks = append(rpt.Checks, checkPageSize(rpt.PageSizeKB))
	rpt.Checks = append(rpt.Checks, probePath(ctx, client, target, "/robots.txt", "robots.txt",
		"Tells search engines which paths to crawl."))
	rpt.Checks = append(rpt.Checks, probePath(ctx, client, target, "/sitemap.xml", "sitemap.xml",
		"Lists pages for search engines to index — recommended for multi-page sites."))

	rpt.Score = computeScore(rpt.Checks)
	return rpt
}

// computeScore sums weight × (1.0 for pass, 0.5 for warn, 0.0 for fail)
// and returns the % of total possible. Weighting lets us flag the
// big-deal items (title, viewport) without making cosmetic checks
// drag the headline score down disproportionately.
func computeScore(checks []insightCheck) int {
	var got, total float64
	for _, c := range checks {
		total += float64(c.Weight)
		switch c.Status {
		case "pass":
			got += float64(c.Weight)
		case "warn":
			got += float64(c.Weight) * 0.5
		}
	}
	if total == 0 {
		return 0
	}
	return int((got / total) * 100)
}

// --- Individual checks ----------------------------------------------------
// Each returns one insightCheck. Pure functions: no I/O except probePath
// which does its own bounded HTTP.

func checkStatus(code int) insightCheck {
	c := insightCheck{ID: "status", Label: "HTTP status", Weight: 5,
		Value: fmt.Sprintf("%d", code)}
	switch {
	case code == 200:
		c.Status = "pass"
	case code >= 300 && code < 400:
		c.Status = "warn"
		c.Hint = "redirect chains hurt SEO and add latency"
	default:
		c.Status = "fail"
		c.Hint = "page must return 200 to be crawled"
	}
	return c
}

func checkContentType(ct string) insightCheck {
	c := insightCheck{ID: "content_type", Label: "Content-Type", Weight: 2, Value: ct}
	if strings.HasPrefix(ct, "text/html") {
		c.Status = "pass"
	} else {
		c.Status = "warn"
		c.Hint = "expected text/html for a web page"
	}
	return c
}

func checkHTTPS(target string) insightCheck {
	c := insightCheck{ID: "https", Label: "HTTPS", Weight: 4, Value: target}
	if strings.HasPrefix(target, "https://") {
		c.Status = "pass"
	} else {
		c.Status = "fail"
		c.Hint = "search engines de-rank plain HTTP"
	}
	return c
}

var titleRE = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)

func checkTitle(html string) insightCheck {
	c := insightCheck{ID: "title", Label: "Page title", Weight: 5}
	m := titleRE.FindStringSubmatch(html)
	if m == nil {
		c.Status = "fail"
		c.Hint = "missing <title> — the most important SEO tag"
		return c
	}
	t := strings.TrimSpace(m[1])
	c.Value = t
	switch {
	case t == "":
		c.Status = "fail"
		c.Hint = "<title> is empty"
	case len(t) < 10:
		c.Status = "warn"
		c.Hint = "title is very short (<10 chars)"
	case len(t) > 60:
		c.Status = "warn"
		c.Hint = "title is over 60 chars — Google may truncate"
	default:
		c.Status = "pass"
	}
	return c
}

// metaContent looks for <meta name="X" content="Y"> OR <meta property="X" content="Y">.
// The lookup is permissive about quote style + attribute order.
func metaContent(html, key string) string {
	pat := regexp.MustCompile(
		`(?is)<meta[^>]+(?:name|property)\s*=\s*["']` + regexp.QuoteMeta(key) +
			`["'][^>]*content\s*=\s*["']([^"']*)["']`)
	if m := pat.FindStringSubmatch(html); m != nil {
		return strings.TrimSpace(m[1])
	}
	// Try the swapped attribute order: content first, then name/property.
	pat = regexp.MustCompile(
		`(?is)<meta[^>]+content\s*=\s*["']([^"']*)["'][^>]*(?:name|property)\s*=\s*["']` +
			regexp.QuoteMeta(key) + `["']`)
	if m := pat.FindStringSubmatch(html); m != nil {
		return strings.TrimSpace(m[1])
	}
	return ""
}

func checkMeta(html, key, label, hint string) insightCheck {
	c := insightCheck{ID: "meta_" + key, Label: label, Weight: 4}
	v := metaContent(html, key)
	c.Value = v
	switch {
	case v == "":
		c.Status = "fail"
		c.Hint = hint
	case len(v) < 50:
		c.Status = "warn"
		c.Hint = fmt.Sprintf("%d chars — aim for 50-160", len(v))
	case len(v) > 160:
		c.Status = "warn"
		c.Hint = fmt.Sprintf("%d chars — over 160, may be truncated", len(v))
	default:
		c.Status = "pass"
	}
	return c
}

func checkViewport(html string) insightCheck {
	c := insightCheck{ID: "viewport", Label: "Mobile viewport", Weight: 5}
	v := metaContent(html, "viewport")
	c.Value = v
	if v == "" {
		c.Status = "fail"
		c.Hint = "without <meta name=viewport> Google flags the page as non-mobile-friendly"
	} else {
		c.Status = "pass"
	}
	return c
}

var canonicalRE = regexp.MustCompile(
	`(?is)<link[^>]+rel\s*=\s*["']canonical["'][^>]*href\s*=\s*["']([^"']+)["']`)

func checkCanonical(html, target string) insightCheck {
	c := insightCheck{ID: "canonical", Label: "Canonical URL", Weight: 2}
	m := canonicalRE.FindStringSubmatch(html)
	if m == nil {
		c.Status = "warn"
		c.Hint = "no <link rel=canonical> — helps avoid duplicate-content penalties"
		return c
	}
	c.Value = m[1]
	c.Status = "pass"
	return c
}

func checkOG(html, key, label string) insightCheck {
	c := insightCheck{ID: "og_" + key, Label: label, Weight: 2}
	v := metaContent(html, key)
	c.Value = v
	if v == "" {
		c.Status = "warn"
		c.Hint = "improves how the page renders when shared on social platforms"
	} else {
		c.Status = "pass"
	}
	return c
}

var h1RE = regexp.MustCompile(`(?is)<h1[^>]*>(.*?)</h1>`)

func checkH1(html string) insightCheck {
	c := insightCheck{ID: "h1", Label: "H1 heading", Weight: 3}
	matches := h1RE.FindAllStringSubmatch(html, -1)
	switch {
	case len(matches) == 0:
		c.Status = "warn"
		c.Hint = "no <h1> — semantic headings help screen readers and crawlers"
	case len(matches) > 1:
		c.Status = "warn"
		c.Value = fmt.Sprintf("%d found", len(matches))
		c.Hint = "multiple <h1> tags — convention is one per page"
	default:
		c.Status = "pass"
		c.Value = stripTags(matches[0][1])
	}
	return c
}

var langRE = regexp.MustCompile(`(?is)<html[^>]+lang\s*=\s*["']([^"']+)["']`)

func checkLang(html string) insightCheck {
	c := insightCheck{ID: "lang", Label: "HTML lang attribute", Weight: 1}
	m := langRE.FindStringSubmatch(html)
	if m == nil {
		c.Status = "warn"
		c.Hint = "<html lang=…> improves accessibility + translation hints"
	} else {
		c.Status = "pass"
		c.Value = m[1]
	}
	return c
}

var faviconRE = regexp.MustCompile(
	`(?is)<link[^>]+rel\s*=\s*["'](?:shortcut )?icon["']`)

func checkFavicon(html string) insightCheck {
	c := insightCheck{ID: "favicon", Label: "Favicon", Weight: 1}
	if faviconRE.MatchString(html) {
		c.Status = "pass"
	} else {
		c.Status = "warn"
		c.Hint = "browsers + Google SERP show a favicon — set <link rel=icon>"
	}
	return c
}

func checkPageSize(kb int) insightCheck {
	c := insightCheck{ID: "page_size", Label: "Page size", Weight: 2,
		Value: fmt.Sprintf("%d KB", kb)}
	switch {
	case kb == 0:
		c.Status = "fail"
		c.Hint = "empty response body"
	case kb > 500:
		c.Status = "warn"
		c.Hint = "page is large — consider code-splitting or compression"
	default:
		c.Status = "pass"
	}
	return c
}

// probePath issues a HEAD-equivalent (GET with no-body-read) against an
// auxiliary path under the same origin and reports presence.
func probePath(ctx context.Context, client *http.Client, target, suffix, label, hint string) insightCheck {
	c := insightCheck{ID: strings.TrimPrefix(suffix, "/"), Label: label, Weight: 1}
	probeURL := strings.TrimRight(target, "/") + suffix
	c.Value = suffix
	req, err := http.NewRequestWithContext(ctx, "GET", probeURL, nil)
	if err != nil {
		c.Status = "warn"
		c.Hint = "probe error: " + err.Error()
		return c
	}
	req.Header.Set("User-Agent", "paas-insights/1.0")
	resp, err := client.Do(req)
	if err != nil {
		c.Status = "warn"
		c.Hint = "probe failed: " + err.Error()
		return c
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode == 200 {
		c.Status = "pass"
	} else {
		c.Status = "warn"
		c.Hint = fmt.Sprintf("%d — %s", resp.StatusCode, hint)
	}
	return c
}

// stripTags is a quick-and-dirty inner-text extractor for our h1 value.
// We only display this string, so cleanliness > rigor — a full HTML
// parser would be overkill.
var tagRE = regexp.MustCompile(`<[^>]+>`)

func stripTags(s string) string {
	out := tagRE.ReplaceAllString(s, "")
	return strings.TrimSpace(out)
}
