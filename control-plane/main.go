// Control plane for the self-hosted PaaS.
//
// Current capabilities:
//   - HMAC-verified GitHub webhook ingestion at POST /webhooks/github
//   - Persistence to Postgres (CNPG-managed): installations, repos,
//     projects, deployments, full webhook audit log
//   - Idempotent dispatch keyed on X-GitHub-Delivery so retries are safe
//   - GET /admin/deployments to inspect recent activity
//
// Still TODO (next slice): mint installation tokens, spawn Kaniko Jobs,
// apply Deployment+Service+IngressRoute, drive the status machine.
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

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maxBodyBytes      = 5 << 20
	sigHeader         = "X-Hub-Signature-256"
	eventHeader       = "X-GitHub-Event"
	deliveryHeader    = "X-GitHub-Delivery"
	installTargetType = "X-GitHub-Hook-Installation-Target-Type"
	installTargetID   = "X-GitHub-Hook-Installation-Target-Id"
	shutdownGrace     = 10 * time.Second
)

type server struct {
	webhookSecret []byte
	store         *store
	k8s           *kubeClient
	// gh is the GitHub App client. It's used by the build worker for
	// installation tokens AND by the auth subsystem for OAuth API calls,
	// so we hold it on the server (not just on the worker).
	gh   *githubApp
	auth *authConfig
	log  *slog.Logger
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	secret := strings.TrimSpace(os.Getenv("GITHUB_WEBHOOK_SECRET"))
	if secret == "" {
		log.Error("GITHUB_WEBHOOK_SECRET is required")
		os.Exit(1)
	}
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" {
		log.Error("DATABASE_URL is required")
		os.Exit(1)
	}
	appIDStr := strings.TrimSpace(os.Getenv("GITHUB_APP_ID"))
	keyPath := strings.TrimSpace(os.Getenv("GITHUB_APP_PRIVATE_KEY_PATH"))
	if appIDStr == "" || keyPath == "" {
		log.Error("GITHUB_APP_ID and GITHUB_APP_PRIVATE_KEY_PATH are required")
		os.Exit(1)
	}

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Database — connect with a short retry loop because the control plane
	// pod can start before CNPG's primary endpoint is ready on cold boot.
	pool, err := connectDB(rootCtx, dsn, log)
	if err != nil {
		log.Error("db connect failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	migCtx, cancel := context.WithTimeout(rootCtx, 30*time.Second)
	if err := runMigrations(migCtx, pool); err != nil {
		cancel()
		log.Error("migrations failed", "err", err)
		os.Exit(1)
	}
	cancel()
	log.Info("migrations applied")

	gh, err := loadGitHubApp(appIDStr, keyPath)
	if err != nil {
		log.Error("load github app failed", "err", err)
		os.Exit(1)
	}
	k8s, err := newInClusterClient()
	if err != nil {
		log.Error("init k8s client failed", "err", err)
		os.Exit(1)
	}
	log.Info("github app + k8s client ready", "app_id", appIDStr, "namespace", k8s.namespace)

	authCfg, err := loadAuthConfig()
	if err != nil {
		log.Error("load auth config failed", "err", err)
		os.Exit(1)
	}
	log.Info("auth ready", "base_url", authCfg.baseURL)

	s := &server{
		webhookSecret: []byte(secret),
		store:         newStore(pool),
		k8s:           k8s,
		gh:            gh,
		auth:          authCfg,
		log:           log,
	}

	requeued, err := s.store.RequeueStuck(rootCtx)
	if err != nil {
		log.Error("requeue stuck deployments failed", "err", err)
	} else if requeued > 0 {
		log.Info("requeued stuck deployments after restart", "count", requeued)
	}

	bw := &worker{store: s.store, gh: gh, k8s: k8s, log: log}
	go bw.Run(rootCtx)

	mux := http.NewServeMux()
	// Public — health probes + webhook (HMAC auth) + login pages.
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz(pool))
	mux.HandleFunc("POST /webhooks/github", s.handleGitHubWebhook)
	mux.HandleFunc("GET /login", s.handleLoginPage)
	mux.HandleFunc("GET /auth/login", s.handleLogin)
	mux.HandleFunc("GET /auth/callback", s.handleCallback)
	mux.HandleFunc("POST /auth/logout", s.handleLogout)

	// Authenticated dashboard root — HTML mode redirects to /login.
	mux.Handle("GET /{$}", s.requireUser(http.HandlerFunc(s.handleDashboard), "html"))

	// Authenticated API surface — JSON mode returns 401 so the SPA can
	// detect logout and bounce to /login itself.
	apiHandlers := map[string]http.HandlerFunc{
		"GET /v1/me":                          s.handleMe,
		"GET /v1/projects":                    s.handleListProjects,
		"GET /v1/deployments":                 s.handleListDeployments,
		"GET /v1/deployments/{id}/logs":       s.handleDeploymentLogs,
	}
	for pat, h := range apiHandlers {
		mux.Handle(pat, s.requireUser(h, "json"))
	}

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           withRequestLogging(log, mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Info("control plane listening", "addr", addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server died", "err", err)
			os.Exit(1)
		}
	}()

	<-rootCtx.Done()
	log.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
}

func connectDB(ctx context.Context, dsn string, log *slog.Logger) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 8
	cfg.MinConns = 1
	cfg.MaxConnLifetime = 30 * time.Minute

	deadline := time.Now().Add(60 * time.Second)
	for attempt := 1; ; attempt++ {
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			pingErr := pool.Ping(pingCtx)
			cancel()
			if pingErr == nil {
				return pool, nil
			}
			pool.Close()
			err = pingErr
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		log.Warn("db not ready, retrying", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *server) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
}

func (s *server) readyz(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			http.Error(w, "db not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ready\n")
	}
}

func (s *server) handleListDeployments(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	u := userFromCtx(r.Context())
	scope := ""
	if u != nil && !u.IsAdmin {
		scope = u.ID
	}
	rows, err := s.store.ListRecentDeploymentsForUser(ctx, scope, 50)
	if err != nil {
		s.log.Error("list deployments failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"deployments": rows})
}

// --- webhook receiver ------------------------------------------------------

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

	env := parseEnvelope(body)

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	inserted, err := s.store.RecordDelivery(
		ctx, delivery, event, env.Action, env.Installation.ID, env.Repository.FullName, body,
	)
	if err != nil {
		s.log.Error("record delivery failed", "delivery", delivery, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !inserted {
		// Duplicate retry from GitHub — already processed.
		s.log.Info("duplicate delivery ignored", "delivery", delivery, "event", event)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	if err := s.dispatch(ctx, event, body, env, delivery); err != nil {
		s.log.Error("dispatch failed", "delivery", delivery, "event", event, "err", err)
		// We've already persisted the delivery; tell GitHub OK so it doesn't
		// retry. Real processing happens in background reconciliation later.
	}

	s.log.Info("github webhook verified",
		"delivery", delivery,
		"event", event,
		"action", env.Action,
		"repo", env.Repository.FullName,
		"ref", env.Ref,
		"sha", env.After,
		"installation_id", env.Installation.ID,
		"sender", env.Sender.Login,
		"target_type", r.Header.Get(installTargetType),
		"target_id", r.Header.Get(installTargetID),
		"bytes", len(body),
	)
	w.WriteHeader(http.StatusAccepted)
	_, _ = io.WriteString(w, "accepted\n")
}

// dispatch handles only events we currently care about. Anything else is
// already persisted to webhook_deliveries — no-op here keeps the receiver
// forward-compatible with new GitHub events.
func (s *server) dispatch(ctx context.Context, event string, body []byte, env envelope, deliveryID string) error {
	switch event {
	case "installation":
		return s.handleInstallation(ctx, env, body)

	case "installation_repositories":
		return s.handleInstallationRepos(ctx, env, body)

	case "push":
		// Ignore branch deletes and tag pushes — only build commits to branches.
		if env.Deleted || !strings.HasPrefix(env.Ref, "refs/heads/") || env.After == "" || env.After == "0000000000000000000000000000000000000000" {
			return nil
		}
		res, err := s.store.EnqueueDeployment(ctx,
			env.Installation.ID, env.Repository.ID, env.After, env.Ref, deliveryID)
		if err != nil {
			return err
		}
		if res == nil {
			s.log.Warn("push for unknown project — skipping",
				"repo", env.Repository.FullName, "installation_id", env.Installation.ID)
			return nil
		}
		s.log.Info("deployment queued",
			"deployment_id", res.DeploymentID,
			"project_id", res.ProjectID,
			"slug", res.Slug,
			"repo", env.Repository.FullName,
			"ref", env.Ref,
			"sha", env.After,
		)
		return nil
	}
	return nil
}

func (s *server) handleInstallation(ctx context.Context, env envelope, body []byte) error {
	switch env.Action {
	case "created", "new_permissions_accepted", "unsuspend":
		if err := s.store.UpsertInstallation(ctx, installationUpsert{
			ID:           env.Installation.ID,
			AccountLogin: env.Installation.Account.Login,
			AccountID:    env.Installation.Account.ID,
			TargetType:   env.Installation.TargetType,
		}); err != nil {
			return err
		}
		// If the installer's GitHub account already has a dashboard user,
		// claim this installation immediately so AddRepos below stamps
		// projects with the right owner from the start.
		if err := s.store.LinkInstallationByGitHubAccount(ctx,
			env.Installation.ID, env.Installation.Account.ID); err != nil {
			s.log.Warn("link installation owner failed",
				"installation_id", env.Installation.ID, "err", err)
		}
		// On created, payload includes the initial list of repositories.
		var p struct {
			Repositories []githubRepo `json:"repositories"`
		}
		if err := json.Unmarshal(body, &p); err == nil && len(p.Repositories) > 0 {
			if err := s.store.AddRepos(ctx, env.Installation.ID, toRepoRows(p.Repositories)); err != nil {
				return err
			}
			// Belt-and-braces: backfill in case the installation was linked
			// AFTER the AddRepos statement happened in a prior call.
			_ = s.store.SetProjectOwnerByInstallation(ctx, env.Installation.ID)
		}
		return nil

	case "suspend":
		return s.store.SuspendInstallation(ctx, env.Installation.ID, true)

	case "deleted":
		return s.store.DeleteInstallation(ctx, env.Installation.ID)
	}
	return nil
}

func (s *server) handleInstallationRepos(ctx context.Context, env envelope, body []byte) error {
	var p struct {
		RepositoriesAdded   []githubRepo `json:"repositories_added"`
		RepositoriesRemoved []githubRepo `json:"repositories_removed"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		return err
	}
	if len(p.RepositoriesAdded) > 0 {
		if err := s.store.AddRepos(ctx, env.Installation.ID, toRepoRows(p.RepositoriesAdded)); err != nil {
			return err
		}
		_ = s.store.SetProjectOwnerByInstallation(ctx, env.Installation.ID)
	}
	if len(p.RepositoriesRemoved) > 0 {
		ids := make([]int64, 0, len(p.RepositoriesRemoved))
		for _, r := range p.RepositoriesRemoved {
			ids = append(ids, r.ID)
		}
		if err := s.store.RemoveRepos(ctx, env.Installation.ID, ids); err != nil {
			return err
		}
	}
	return nil
}

// --- envelope --------------------------------------------------------------

type envelope struct {
	Action     string `json:"action,omitempty"`
	Ref        string `json:"ref,omitempty"`
	Before     string `json:"before,omitempty"`
	After      string `json:"after,omitempty"`
	Deleted    bool   `json:"deleted,omitempty"`
	Repository struct {
		ID            int64  `json:"id"`
		FullName      string `json:"full_name"`
		Private       bool   `json:"private"`
		DefaultBranch string `json:"default_branch"`
	} `json:"repository,omitempty"`
	Installation struct {
		ID         int64  `json:"id"`
		TargetType string `json:"target_type"`
		Account    struct {
			Login string `json:"login"`
			ID    int64  `json:"id"`
		} `json:"account"`
	} `json:"installation,omitempty"`
	Sender struct {
		Login string `json:"login"`
	} `json:"sender,omitempty"`
}

type githubRepo struct {
	ID       int64  `json:"id"`
	FullName string `json:"full_name"`
	Private  bool   `json:"private"`
	// default_branch is not in the slim repo object inside
	// installation_repositories payloads; we fill it from API later.
	DefaultBranch string `json:"default_branch,omitempty"`
}

func parseEnvelope(body []byte) envelope {
	var e envelope
	_ = json.Unmarshal(body, &e)
	return e
}

func toRepoRows(in []githubRepo) []repoRow {
	out := make([]repoRow, 0, len(in))
	for _, r := range in {
		out = append(out, repoRow{
			ID:            r.ID,
			FullName:      r.FullName,
			Private:       r.Private,
			DefaultBranch: r.DefaultBranch,
		})
	}
	return out
}

// --- signature + logging ---------------------------------------------------

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
	return hmac.Equal(mac.Sum(nil), want)
}

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
