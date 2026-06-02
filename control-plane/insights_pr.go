package main

// SEO/meta heuristic patcher.
//
// Pure functions that take a snapshot of one HTML file plus a small bag of
// project metadata (repo description, live URL) and return a patched copy
// with the obvious-but-missing tags inserted. The function is deliberately
// conservative:
//
//   - We never overwrite an existing tag. If the page already has a
//     <title> or a meta viewport we leave it alone, even if it's
//     suboptimal. The Insights report flags those for the user; an
//     auto-generated rewrite would lose the human's intent.
//   - We only operate on a single source file at a time (the most likely
//     entry HTML — see suggestPR_pickEntryFile). Multi-file refactors
//     (framework-specific patches in Next/Astro components) are out of
//     scope for v1.
//   - We never insert content that we don't have a clean source for.
//     If the GitHub repo description is blank we don't fabricate a meta
//     description — better to leave it for the user than to ship "TODO".
//
// All input strings are assumed UTF-8 and small (<1 MiB — bounded by
// the caller). The output is a strict superset of the input, with extra
// <head> children injected before </head>.

import (
	"fmt"
	"regexp"
	"strings"
)

// seoPatch represents one applied or skipped change. The handler bundles
// these into the PR body so the reviewer can see what the bot did and
// why, and into the JSON response so the dashboard can show a summary.
type seoPatch struct {
	ID       string `json:"id"`       // stable key matching insightCheck.ID where possible
	Label    string `json:"label"`    // human-readable description
	Applied  bool   `json:"applied"`  // true if we actually inserted into HTML
	Reason   string `json:"reason"`   // for !Applied: why we skipped
	Snippet  string `json:"snippet"`  // the line we inserted (if applied)
}

// seoInputs is everything the patcher needs to know about the project
// to render concrete values into the inserted tags.
type seoInputs struct {
	RepoName    string // e.g. "spinup-app" — used as a fallback <title>
	RepoDesc    string // GitHub repo description — used as meta description
	LiveURL     string // e.g. "https://my-app.spinup.in" — used for canonical & og:url
	DefaultLang string // usually "en" — could be parameterised in future
}

// applySEOPatches walks the input HTML once, detects which signals are
// missing, and returns (newHTML, list-of-changes). If the HTML has no
// recognisable <head> closing tag we return the original unchanged with
// a single skipped patch explaining why — the handler turns that into a
// user-facing message rather than a stack trace.
func applySEOPatches(html string, in seoInputs) (string, []seoPatch) {
	patches := []seoPatch{}

	headCloseRE := regexp.MustCompile(`(?i)</head\s*>`)
	headCloseMatch := headCloseRE.FindStringIndex(html)
	if headCloseMatch == nil {
		patches = append(patches, seoPatch{
			ID: "head_missing", Label: "Inject SEO tags",
			Reason: "no </head> tag found in file — likely a framework template the bot can't safely edit",
		})
		return html, patches
	}

	// We'll accumulate snippets to inject just before </head>, in a stable
	// order that reads naturally in the resulting diff.
	var inject []string

	addTag := func(p seoPatch, snippet string) {
		p.Applied = true
		p.Snippet = snippet
		inject = append(inject, snippet)
		patches = append(patches, p)
	}
	skipTag := func(p seoPatch, reason string) {
		p.Applied = false
		p.Reason = reason
		patches = append(patches, p)
	}

	// ---- <html lang=…> -----------------------------------------------------
	// Different from the others: lives on <html>, not <head>. Handled
	// inline below since it changes the existing tag rather than inserting.
	htmlOpenRE := regexp.MustCompile(`(?is)<html([^>]*)>`)
	if m := htmlOpenRE.FindStringSubmatchIndex(html); m != nil {
		attrs := html[m[2]:m[3]]
		if !regexp.MustCompile(`(?i)\blang\s*=`).MatchString(attrs) {
			lang := in.DefaultLang
			if lang == "" {
				lang = "en"
			}
			newOpen := fmt.Sprintf(`<html%s lang="%s">`, attrs, lang)
			html = html[:m[0]] + newOpen + html[m[1]:]
			// re-locate </head> since we just shifted indexes
			headCloseMatch = headCloseRE.FindStringIndex(html)
			patches = append(patches, seoPatch{
				ID: "lang", Label: "Set <html lang>",
				Applied: true, Snippet: fmt.Sprintf(`lang="%s"`, lang),
			})
		}
	}

	// ---- <title> -----------------------------------------------------------
	// Inserted into <head>, not anywhere else. If the page already has a
	// <title> (even empty) we don't touch it.
	if !regexp.MustCompile(`(?is)<title[^>]*>.*?</title>`).MatchString(html) {
		title := strings.TrimSpace(in.RepoName)
		if title == "" {
			skipTag(seoPatch{ID: "title", Label: "Add <title>"},
				"no repository name available to use as a fallback title")
		} else {
			addTag(seoPatch{ID: "title", Label: "Add <title>"},
				fmt.Sprintf("    <title>%s</title>", htmlEscape(title)))
		}
	}

	// ---- <meta name="description"> ----------------------------------------
	if metaContent(html, "description") == "" {
		if d := strings.TrimSpace(in.RepoDesc); d != "" {
			addTag(seoPatch{ID: "meta_description", Label: "Add meta description"},
				fmt.Sprintf(`    <meta name="description" content="%s">`, htmlEscape(d)))
		} else {
			skipTag(seoPatch{ID: "meta_description", Label: "Add meta description"},
				"no GitHub repository description set — add one in repo settings and re-run")
		}
	}

	// ---- <meta name="viewport"> -------------------------------------------
	if metaContent(html, "viewport") == "" {
		addTag(seoPatch{ID: "viewport", Label: "Add mobile viewport meta"},
			`    <meta name="viewport" content="width=device-width, initial-scale=1">`)
	}

	// ---- <link rel="canonical"> -------------------------------------------
	if !regexp.MustCompile(`(?is)<link[^>]+rel\s*=\s*["']canonical["']`).MatchString(html) {
		if in.LiveURL != "" {
			addTag(seoPatch{ID: "canonical", Label: "Add canonical URL"},
				fmt.Sprintf(`    <link rel="canonical" href="%s">`, htmlEscape(in.LiveURL)))
		}
	}

	// ---- OpenGraph tags ---------------------------------------------------
	if metaContent(html, "og:title") == "" && strings.TrimSpace(in.RepoName) != "" {
		addTag(seoPatch{ID: "og_title", Label: "Add OpenGraph title"},
			fmt.Sprintf(`    <meta property="og:title" content="%s">`, htmlEscape(in.RepoName)))
	}
	if metaContent(html, "og:description") == "" && strings.TrimSpace(in.RepoDesc) != "" {
		addTag(seoPatch{ID: "og_description", Label: "Add OpenGraph description"},
			fmt.Sprintf(`    <meta property="og:description" content="%s">`, htmlEscape(in.RepoDesc)))
	}
	if metaContent(html, "og:url") == "" && in.LiveURL != "" {
		addTag(seoPatch{ID: "og_url", Label: "Add OpenGraph URL"},
			fmt.Sprintf(`    <meta property="og:url" content="%s">`, htmlEscape(in.LiveURL)))
	}
	if metaContent(html, "og:type") == "" {
		addTag(seoPatch{ID: "og_type", Label: "Add OpenGraph type"},
			`    <meta property="og:type" content="website">`)
	}

	if len(inject) == 0 {
		return html, patches
	}

	// Locate </head> again (may have shifted from the lang edit) and inject
	// just before it, preserving the original indentation of the closing
	// tag for a clean diff.
	headCloseMatch = headCloseRE.FindStringIndex(html)
	// Figure out the indentation of the line containing </head> so injected
	// children line up. Walk back to the previous newline.
	headStart := headCloseMatch[0]
	lineStart := headStart
	for lineStart > 0 && html[lineStart-1] != '\n' {
		lineStart--
	}
	indent := html[lineStart:headStart]
	// Strip any non-whitespace from indent (e.g. text before </head> on the
	// same line) so we don't smear content into the indent string.
	for i := 0; i < len(indent); i++ {
		if indent[i] != ' ' && indent[i] != '\t' {
			indent = ""
			break
		}
	}

	// Re-indent each snippet to match the existing </head> column.
	out := strings.Builder{}
	out.WriteString(html[:headStart])
	for _, snip := range inject {
		// Snippets already carry 4 spaces of indent from the addTag callers
		// — replace that with the file's actual indent so the diff is clean.
		trimmed := strings.TrimLeft(snip, " \t")
		out.WriteString(indent)
		out.WriteString(trimmed)
		out.WriteString("\n")
	}
	out.WriteString(indent)
	out.WriteString(html[headStart:])
	return out.String(), patches
}

// htmlEscape covers the four characters that matter inside an attribute
// value or text node. We don't pull in html/template for this one-liner
// because the inputs are tiny and the rules are stable.
func htmlEscape(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
	)
	return r.Replace(s)
}

// pickEntryFile picks the first file path from the project repo that's
// a plausible HTML entry point — i.e. one whose <head> the patcher knows
// how to edit. We probe by attempting to fetch each candidate against the
// GitHub Contents API; whichever returns 200 first wins.
//
// Order matters: more-specific paths come first (so a Vite app with both
// index.html at root AND a public/index.html prefers the root one which
// is the actual served template). Returning the first hit means we touch
// at most one file per PR — much easier for the user to review.
func suggestPREntryCandidates() []string {
	return []string{
		"index.html",
		"public/index.html",
		"src/index.html",
		"app/index.html",
	}
}
