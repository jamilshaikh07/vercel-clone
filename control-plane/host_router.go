package main

import (
	"net/http"
	"strings"
)

// withHostRouting sends marketing traffic (landing + waitlist) on
// spinup.in/www and the dashboard + OAuth on app.spinup.in. Health probes
// and GitHub webhooks work on every hostname.
func (s *server) withHostRouting(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/healthz" || path == "/readyz" || path == "/webhooks/github" {
			next.ServeHTTP(w, r)
			return
		}

		if s.hosts.surface(requestHost(r)) != "marketing" {
			next.ServeHTTP(w, r)
			return
		}

		switch {
		case r.Method == http.MethodGet && (path == "/" || path == "/login"):
			s.handleLandingPage(w, r)
		case r.Method == http.MethodPost && path == "/v1/waitlist":
			s.handleJoinWaitlist(w, r)
		case strings.HasPrefix(path, "/auth/"):
			http.Redirect(w, r, s.hosts.appBase+path, http.StatusFound)
		default:
			http.Redirect(w, r, s.hosts.appBase+r.URL.RequestURI(), http.StatusFound)
		}
	})
}
