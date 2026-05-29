// setup-gh-app drives the GitHub App "manifest flow" end-to-end so you
// don't have to fill the App-creation form by hand.
//
// Flow:
//  1. We start a local HTTP server on a random port.
//  2. GET / renders an HTML page that auto-submits a hidden manifest
//     to https://github.com/settings/apps/new — GitHub shows a single
//     "Create GitHub App" review page with everything pre-filled.
//  3. After you click Create, GitHub redirects back to /callback?code=...
//  4. We exchange the code at /app-manifests/{code}/conversions
//     (no auth required — the code is single-use, short-lived).
//  5. We write the resulting app_id / client_id / client_secret /
//     webhook_secret / pem to ./out/ and print kubectl commands.
//
// The webhook_secret we pass in the manifest is preserved by GitHub,
// so it matches the one already living in the cluster Secret.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

type manifest struct {
	Name           string            `json:"name"`
	URL            string            `json:"url"`
	HookAttributes map[string]any    `json:"hook_attributes"`
	RedirectURL    string            `json:"redirect_url"`
	Description    string            `json:"description,omitempty"`
	Public         bool              `json:"public"`
	DefaultEvents  []string          `json:"default_events"`
	DefaultPerms   map[string]string `json:"default_permissions"`
}

type conversion struct {
	ID            int64  `json:"id"`
	Slug          string `json:"slug"`
	NodeID        string `json:"node_id"`
	Name          string `json:"name"`
	ClientID      string `json:"client_id"`
	ClientSecret  string `json:"client_secret"`
	WebhookSecret string `json:"webhook_secret"`
	PEM           string `json:"pem"`
	HTMLURL       string `json:"html_url"`
}

func main() {
	var (
		appName       = flag.String("name", "jamilshaikh-paas", "GitHub App name (must be globally unique)")
		homepageURL   = flag.String("homepage", "https://paas.jamilshaikh.in", "App homepage URL")
		webhookURL    = flag.String("webhook-url", "https://paas.jamilshaikh.in/webhooks/github", "Webhook receiver URL")
		webhookSecret = flag.String("webhook-secret", "", "Webhook HMAC secret (will be passed to GitHub so it matches the cluster Secret). If empty, attempts to read from kubectl.")
		outDir        = flag.String("out", "./out", "Directory to write credentials to")
		listenAddr    = flag.String("listen", "127.0.0.1:0", "Local listen address (random port if :0)")
		openBrowser   = flag.Bool("open", true, "Auto-open the browser to start the flow")
	)
	flag.Parse()

	if *webhookSecret == "" {
		s, err := readWebhookSecretFromCluster()
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not auto-read webhook secret: %v\nPass -webhook-secret=...\n", err)
			os.Exit(2)
		}
		*webhookSecret = s
		fmt.Fprintln(os.Stderr, "→ read webhook secret from kube secret paas-system/github-webhook")
	}

	if err := os.MkdirAll(*outDir, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir out: %v\n", err)
		os.Exit(1)
	}

	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(1)
	}
	localBase := "http://" + ln.Addr().String()
	redirect := localBase + "/callback"

	m := manifest{
		Name:           *appName,
		URL:            *homepageURL,
		HookAttributes: map[string]any{"url": *webhookURL, "active": true},
		RedirectURL:    redirect,
		Description:    "Self-hosted PaaS — builds and deploys your apps to a Talos K8s cluster on every push.",
		Public:         false,
		// installation + installation_repositories are auto-delivered to all
		// Apps; they cannot appear in default_events.
		DefaultEvents: []string{"push", "pull_request"},
		DefaultPerms: map[string]string{
			"contents":         "read",
			"metadata":         "read",
			"pull_requests":    "read",
			"statuses":         "write",
			"checks":           "write",
			"deployments":      "write",
		},
	}
	manifestJSON, err := json.Marshal(m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal manifest: %v\n", err)
		os.Exit(1)
	}
	// The webhook secret is set out-of-band on the App after creation
	// (the manifest endpoint does not accept hook_secret directly; GitHub
	// generates one and returns it via the conversion API). We then
	// reconcile by setting the cluster Secret to match what GitHub returned.

	done := make(chan *conversion, 1)
	srv := &http.Server{Handler: newMux(string(manifestJSON), done)}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		_ = srv.Serve(ln)
	}()

	startURL := localBase + "/"
	fmt.Fprintf(os.Stderr, "→ visit %s in your browser if it doesn't open automatically\n", startURL)
	if *openBrowser {
		_ = openInBrowser(startURL)
	}

	select {
	case <-ctx.Done():
		fmt.Fprintln(os.Stderr, "cancelled before App creation completed")
		os.Exit(130)
	case conv := <-done:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)

		writeAll(*outDir, conv)
		printNextSteps(*outDir, conv, *webhookSecret)
	}
}

func newMux(manifestJSON string, done chan<- *conversion) http.Handler {
	mux := http.NewServeMux()
	tmpl := template.Must(template.New("submit").Parse(submitHTML))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = tmpl.Execute(w, map[string]string{"Manifest": manifestJSON})
	})
	mux.HandleFunc("GET /callback", func(w http.ResponseWriter, r *http.Request) {
		code := r.URL.Query().Get("code")
		if code == "" {
			http.Error(w, "missing code", http.StatusBadRequest)
			return
		}
		fmt.Fprintln(os.Stderr, "→ received code, exchanging…")
		conv, err := exchangeCode(r.Context(), code)
		if err != nil {
			http.Error(w, "exchange failed: "+err.Error(), http.StatusBadGateway)
			fmt.Fprintf(os.Stderr, "exchange failed: %v\n", err)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, `<!doctype html><meta charset="utf-8"><title>done</title>
<body style="font-family:ui-monospace,monospace;background:#0b1020;color:#e5e7eb;padding:3rem">
<h1 style="color:#a5f3fc">App created ✓</h1>
<p>Name: <code>`+template.HTMLEscapeString(conv.Slug)+`</code></p>
<p>You can close this tab and return to the terminal.</p>
</body>`)
		done <- conv
	})
	return mux
}

// exchangeCode hits POST /app-manifests/{code}/conversions. No auth required.
func exchangeCode(ctx context.Context, code string) (*conversion, error) {
	endpoint := "https://api.github.com/app-manifests/" + url.PathEscape(code) + "/conversions"
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var c conversion
	if err := json.Unmarshal(body, &c); err != nil {
		return nil, err
	}
	if c.ID == 0 || c.PEM == "" {
		return nil, errors.New("response missing id or pem")
	}
	return &c, nil
}

func writeAll(dir string, c *conversion) {
	must := func(name, data string, mode os.FileMode) {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(data), mode); err != nil {
			fmt.Fprintf(os.Stderr, "write %s: %v\n", p, err)
			os.Exit(1)
		}
	}
	must("app-id", fmt.Sprintf("%d\n", c.ID), 0o600)
	must("slug", c.Slug+"\n", 0o600)
	must("client-id", c.ClientID+"\n", 0o600)
	must("client-secret", c.ClientSecret+"\n", 0o600)
	must("webhook-secret", c.WebhookSecret+"\n", 0o600)
	must("private-key.pem", c.PEM, 0o600)
	must("install-url", c.HTMLURL+"/installations/new\n", 0o600)
}

func printNextSteps(dir string, c *conversion, originalSecret string) {
	abs, _ := filepath.Abs(dir)
	fmt.Println()
	fmt.Println("=========================================================")
	fmt.Println("  GitHub App created")
	fmt.Println("=========================================================")
	fmt.Printf("  Slug:        %s\n", c.Slug)
	fmt.Printf("  App ID:      %d\n", c.ID)
	fmt.Printf("  Client ID:   %s\n", c.ClientID)
	fmt.Printf("  Manage:      %s\n", c.HTMLURL)
	fmt.Printf("  Install at:  %s/installations/new\n", c.HTMLURL)
	fmt.Printf("  Files in:    %s/\n", abs)
	fmt.Println()
	if originalSecret != "" && originalSecret != c.WebhookSecret {
		fmt.Println("  ⚠ GitHub generated its OWN webhook secret (manifest flow does")
		fmt.Println("    not honor the one we pre-set in the cluster). Reconciling:")
		fmt.Println()
	}
	fmt.Println("  Run these to load creds into the cluster:")
	fmt.Println()
	fmt.Printf(`    kubectl -n paas-system create secret generic github-app \
      --from-literal=app-id=%d \
      --from-literal=client-id=%s \
      --from-literal=client-secret=%s \
      --from-file=private-key.pem=%s/private-key.pem \
      --dry-run=client -o yaml | kubectl apply -f -
`, c.ID, c.ClientID, c.ClientSecret, abs)
	fmt.Println()
	fmt.Printf(`    kubectl -n paas-system create secret generic github-webhook \
      --from-literal=secret=%s \
      --dry-run=client -o yaml | kubectl apply -f -
`, c.WebhookSecret)
	fmt.Println()
	fmt.Println("  Then install the App on the paas-sample-hello repo:")
	fmt.Printf("    %s/installations/new\n", c.HTMLURL)
	fmt.Println("=========================================================")
}

func readWebhookSecretFromCluster() (string, error) {
	out, err := exec.Command("kubectl", "-n", "paas-system", "get", "secret", "github-webhook",
		"-o", "jsonpath={.data.secret}").Output()
	if err != nil {
		return "", err
	}
	dec, err := base64Decode(strings.TrimSpace(string(out)))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(dec), nil
}

func base64Decode(s string) (string, error) {
	// avoid pulling in encoding/base64 import section conflicts — local impl
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	var idx [256]int
	for i := range idx {
		idx[i] = -1
	}
	for i, c := range alphabet {
		idx[c] = i
	}
	s = strings.TrimRight(s, "=")
	var out []byte
	var buf, bits uint32
	for i := 0; i < len(s); i++ {
		v := idx[s[i]]
		if v < 0 {
			return "", fmt.Errorf("invalid base64 char %q", s[i])
		}
		buf = (buf << 6) | uint32(v)
		bits += 6
		if bits >= 8 {
			bits -= 8
			out = append(out, byte(buf>>bits))
			buf &= (1 << bits) - 1
		}
	}
	return string(out), nil
}

func openInBrowser(target string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "linux":
		cmd = exec.Command("xdg-open", target)
	case "darwin":
		cmd = exec.Command("open", target)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", target)
	default:
		return fmt.Errorf("unsupported OS")
	}
	return cmd.Start()
}

const submitHTML = `<!doctype html>
<meta charset="utf-8">
<title>Creating GitHub App…</title>
<style>
  body { font-family: ui-monospace, SFMono-Regular, monospace;
         background: #0b1020; color: #e5e7eb; padding: 3rem; }
  h1 { color: #a5f3fc; }
  code { color: #f9a8d4; }
  button { padding: .5rem 1rem; font-size: 1rem; cursor: pointer; }
</style>
<h1>Creating GitHub App via manifest flow</h1>
<p>This page will submit a pre-filled App manifest to GitHub.</p>
<p>You'll see GitHub's review page next — click <strong>Create GitHub App</strong> there.</p>
<form id="f" method="post" action="https://github.com/settings/apps/new">
  <input type="hidden" name="manifest" value='{{ .Manifest }}'>
  <button type="submit">Continue to GitHub →</button>
</form>
<script>
  // Auto-submit after a short delay so users see what's happening.
  setTimeout(() => document.getElementById('f').submit(), 600);
</script>
`
