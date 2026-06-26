package main

import (
	"net/http"
	"os"
	"strconv"
	"strings"
)

// hostConfig splits the public surface across marketing hosts (landing +
// waitlist) and app hosts (dashboard + OAuth). Defaults match phase-2 DNS:
//   spinup.in / www.spinup.in  → marketing
//   app.spinup.in              → dashboard
type hostConfig struct {
	marketing map[string]bool
	app       map[string]bool
	// appBase is the canonical dashboard origin (OAuth redirect_uri base).
	appBase string
}

func loadHostConfig(dashboardBaseURL string) *hostConfig {
	appBase := strings.TrimRight(strings.TrimSpace(dashboardBaseURL), "/")
	if appBase == "" {
		appBase = "https://app.spinup.in"
	}

	marketing := parseHostSet(os.Getenv("MARKETING_HOSTS"), "spinup.in", "www.spinup.in")
	app := parseHostSet(os.Getenv("APP_HOSTS"), "app.spinup.in")

	// Dev / single-host fallback: if APP_HOSTS is unset and DASHBOARD_BASE_URL
	// points at spinup.in, treat the apex as the app so existing installs
	// keep working until DNS for app.spinup.in is live.
	if os.Getenv("APP_HOSTS") == "" {
		host := hostFromURL(appBase)
		if host != "" && !marketing[host] {
			app[host] = true
		}
		// When marketing and app share spinup.in, marketing wins for / and
		// /login; dashboard moves to app.spinup.in once APP_HOSTS is set.
	}

	return &hostConfig{marketing: marketing, app: app, appBase: appBase}
}

func parseHostSet(raw string, defaults ...string) map[string]bool {
	m := map[string]bool{}
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\n' || r == '\r' || r == '\t' || r == ';'
	})
	if len(fields) == 0 {
		fields = defaults
	}
	for _, f := range fields {
		if h := strings.ToLower(strings.TrimSpace(f)); h != "" {
			m[h] = true
		}
	}
	return m
}

func hostFromURL(u string) string {
	u = strings.TrimSpace(u)
	if u == "" {
		return ""
	}
	if !strings.Contains(u, "://") {
		u = "https://" + u
	}
	parsed, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil || parsed.URL == nil {
		return ""
	}
	return strings.ToLower(parsed.URL.Hostname())
}

func requestHost(r *http.Request) string {
	h := r.Host
	if i := strings.IndexByte(h, ':'); i >= 0 {
		h = h[:i]
	}
	return strings.ToLower(h)
}

func (hc *hostConfig) isMarketing(host string) bool { return hc.marketing[host] }
func (hc *hostConfig) isApp(host string) bool       { return hc.app[host] }

// surface reports which host bucket handled the request. Unknown hosts are
// treated as app so health probes and misconfigured DNS still reach the API.
func (hc *hostConfig) surface(host string) string {
	if hc.isMarketing(host) {
		return "marketing"
	}
	return "app"
}

// loadMaxProjects reads MAX_PROJECTS_PER_USER (default 3). Zero disables the cap.
func loadMaxProjects() int {
	raw := strings.TrimSpace(os.Getenv("MAX_PROJECTS_PER_USER"))
	if raw == "" {
		return 3
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 3
	}
	return n
}
