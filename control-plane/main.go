// Control plane for the self-hosted PaaS.
//
// First slice: receive verified GitHub webhooks at /webhooks/github
// and structured-log them. No DB, no build orchestration yet —
// the goal is to prove the end-to-end identity + delivery path before
// wiring in Kaniko jobs and K8s deploys.
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	maxBodyBytes      = 5 << 20 // 5 MiB — well above GitHub's webhook payload cap
	sigHeader         = "X-Hub-Signature-256"
	eventHeader       = "X-GitHub-Event"
	deliveryHeader    = "X-GitHub-Delivery"
	installationHdr   = "X-GitHub-Hook-Installation-Target-Id"
	installationType  = "X-GitHub-Hook-Installation-Target-Type"
	shutdownGraceTime = 10 * time.Second
)

type server struct {
	webhookSecret []byte
	log           *slog.Logger
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	secret := strings.TrimSpace(os.Getenv("GITHUB_WEBHOOK_SECRET"))
	if secret == "" {
		log.Error("GITHUB_WEBHOOK_SECRET is required")
		os.Exit(1)
	}

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	s := &server{webhookSecret: []byte(secret), log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.healthz)
	mux.HandleFunc("POST /webhooks/github", s.handleGitHubWebhook)
	mux.HandleFunc("GET /", s.root)

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           withRequestLogging(log, mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("control plane listening", "addr", addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server died", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGraceTime)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
}

func (s *server) root(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "vercel-clone control plane — POST /webhooks/github\n")
}

func (s *server) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
}

func (s *server) handleGitHubWebhook(w http.ResponseWriter, r *http.Request) {
	event := r.Header.Get(eventHeader)
	delivery := r.Header.Get(deliveryHeader)
	sig := r.Header.Get(sigHeader)

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		s.log.Warn("read body failed", "err", err, "delivery", delivery)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if !verifySignature(s.webhookSecret, sig, body) {
		s.log.Warn("invalid signature", "delivery", delivery, "event", event)
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	// Minimal envelope extraction — log enough to confirm wiring works,
	// without pretending to model every event type yet.
	var env struct {
		Action       string `json:"action,omitempty"`
		Repository   struct {
			FullName string `json:"full_name"`
		} `json:"repository,omitempty"`
		Ref         string `json:"ref,omitempty"`
		After       string `json:"after,omitempty"`
		Installation struct {
			ID int64 `json:"id"`
		} `json:"installation,omitempty"`
		Sender struct {
			Login string `json:"login"`
		} `json:"sender,omitempty"`
	}
	_ = json.Unmarshal(body, &env) // best-effort; not all events have these

	s.log.Info("github webhook verified",
		"delivery", delivery,
		"event", event,
		"action", env.Action,
		"repo", env.Repository.FullName,
		"ref", env.Ref,
		"sha", env.After,
		"installation_id", env.Installation.ID,
		"sender", env.Sender.Login,
		"target_type", r.Header.Get(installationType),
		"target_id", r.Header.Get(installationHdr),
		"bytes", len(body),
	)

	w.WriteHeader(http.StatusAccepted)
	_, _ = io.WriteString(w, "accepted\n")
}

// verifySignature implements GitHub's X-Hub-Signature-256 HMAC check.
// Constant-time compare to avoid timing side channels.
func verifySignature(secret []byte, header string, body []byte) bool {
	const prefix = "sha256="
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	want, err := hex.DecodeString(header[len(prefix):])
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	got := mac.Sum(nil)
	return hmac.Equal(got, want)
}

// withRequestLogging is a tiny middleware that records request lines
// without the body. Useful for sanity-checking ingress + TLS.
func withRequestLogging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sr := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sr, r)
		log.Info("http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sr.status,
			"dur_ms", time.Since(start).Milliseconds(),
			"remote", r.Header.Get("CF-Connecting-IP"),
			"ua", r.UserAgent(),
		)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(c int) {
	s.status = c
	s.ResponseWriter.WriteHeader(c)
}
