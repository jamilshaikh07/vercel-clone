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
	hosts       *hostConfig
	waitlistRL  *ipRateLimiter
	maxProjects int
	// tcache absorbs multi-tab dashboard polling so the metrics-server
	// + Traefik /metrics endpoints are only hit once per 5s window. Safe
	// to share across requests — collector serialises on its own mutex.
	tcache telemetryCache
	// series holds the rolling 60-minute history of per-service HTTP
	// status-code counts. Populated by the sampler goroutine; read by
	// the per-app traffic chart.
	series *seriesStore
	// pseries is the richer per-project rolling history: CPU, memory,
	// RPS, and real latency percentiles. Powers the BI-style charts on
	// the per-app Telemetry/Traffic pages.
	pseries *projectSeries
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

	// Optional: enables /v1/projects/{id}/database. Absent in dev where
	// CNPG superuser access is off; the endpoint returns 503 in that case.
	globalSuperuserURI = strings.TrimSpace(os.Getenv("PG_SUPERUSER_URI"))
	if globalSuperuserURI == "" {
		log.Warn("PG_SUPERUSER_URI not set — tenant DB provisioning disabled")
	}

	s := &server{
		webhookSecret: []byte(secret),
		store:         newStore(pool),
		k8s:           k8s,
		gh:            gh,
		auth:          authCfg,
		log:           log,
		hosts:         loadHostConfig(authCfg.baseURL),
		waitlistRL:    newIPRateLimiter(10, time.Minute),
		maxProjects:   loadMaxProjects(),
		series:        newSeriesStore(),
		pseries:       newProjectSeries(),
	}
	log.Info("host routing ready",
		"app_base", s.hosts.appBase,
		"max_projects_per_user", s.maxProjects,
	)

	requeued, err := s.store.RequeueStuck(rootCtx)
	if err != nil {
		log.Error("requeue stuck deployments failed", "err", err)
	} else if requeued > 0 {
		log.Info("requeued stuck deployments after restart", "count", requeued)
	}

	if orphans, err := s.store.ReconcileOrphanRepos(rootCtx); err != nil {
		log.Error("reconcile orphan repos failed", "err", err)
	} else if len(orphans) > 0 {
		log.Info("reconciled orphan repos into projects", "count", len(orphans))
		for _, o := range orphans {
			s.tryAutoDeployRepo(rootCtx, o.InstallationID, githubRepo{
				ID: o.RepoID, FullName: o.FullName, DefaultBranch: o.DefaultBranch,
			})
		}
	}

	bw := &worker{store: s.store, gh: gh, k8s: k8s, log: log}
	go bw.Run(rootCtx)

	// Self-rebuilder: polls our own git repo for new commits and triggers
	// a fresh image build + rollout. Lets `git push` actually deploy
	// control-plane changes without any manual kubectl steps.
	startSelfRebuilder(rootCtx, k8s, log)

	// Telemetry sampler: every 30s snapshots Traefik counters into the
	// in-memory ring buffer so the per-app traffic page can draw a real
	// time-series chart from delta-per-window.
	startTelemetrySampler(rootCtx, s, log)

	// Synthetic-traffic monitor: probes each live app every 30s so the
	// telemetry/traffic charts populate with real data instead of looking
	// dead between organic visits. Doubles as a blackbox uptime check.
	// Disable with PAAS_DISABLE_SYNTHETIC=1.
	startSyntheticMonitor(rootCtx, s, log)

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
		"GET /v1/me":                            s.handleMe,
		"GET /v1/version":                       s.handleVersion,
		"GET /v1/projects":                      s.handleListProjects,
		"PATCH /v1/projects/{id}":                 s.handlePatchProject,
		"GET /v1/deployments":                   s.handleListDeployments,
		"GET /v1/deployments/{id}/logs":         s.handleDeploymentLogs,
		"GET /v1/deployments/{id}/runtime-logs": s.handleDeploymentRuntimeLogs,
		"POST /v1/deployments/{id}/redeploy":    s.handleRedeploy,
		"POST /v1/deployments/{id}/promote":     s.handlePromoteDeployment,
		"POST /v1/projects/{id}/deploy":         s.handleDeployNow,
		"GET /v1/projects/{id}/database":        s.handleGetProjectDatabase,
		"POST /v1/projects/{id}/database":       s.handleCreateProjectDatabase,
		"POST /v1/projects/{id}/database/query": s.handleRunQuery,
		"GET /v1/projects/{id}/env":             s.handleListProjectEnv,
		"PUT /v1/projects/{id}/env/{name}":      s.handleUpsertProjectEnv,
		"DELETE /v1/projects/{id}/env/{name}":   s.handleDeleteProjectEnv,
		"GET /v1/telemetry":                       s.handleGlobalTelemetry,
		"GET /v1/projects/{id}/telemetry":         s.handleProjectTelemetry,
		"GET /v1/projects/{id}/telemetry/series":  s.handleProjectSeries,
		"GET /v1/projects/{id}/metrics":           s.handleProjectMetrics,
		"GET /v1/projects/{id}/status":            s.handleProjectStatus,
		"POST /v1/projects/{id}/scale":            s.handleProjectScale,
		"GET /v1/projects/{id}/insights":          s.handleProjectInsights,
		"POST /v1/projects/{id}/insights/suggest-pr": s.handleSuggestInsightsPR,
		"GET /v1/projects/{id}/domains":                       s.handleListProjectDomains,
		"POST /v1/projects/{id}/domains":                      s.handleAddProjectDomain,
		"POST /v1/projects/{id}/domains/{hostname}/verify":    s.handleVerifyProjectDomain,
		"DELETE /v1/projects/{id}/domains/{hostname}":         s.handleDeleteProjectDomain,
	}
	for pat, h := range apiHandlers {
		mux.Handle(pat, s.requireUser(h, "json"))
	}

	mux.Handle("GET /admin/waitlist", s.requireUser(http.HandlerFunc(s.handleAdminWaitlist), "json"))

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           withRequestLogging(log, s.withHostRouting(mux)),
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
			env.Installation.ID, env.Repository.ID, env.After, env.Ref, deliveryID, "webhook")
		if err != nil {
			return err
		}
		if res == nil {
			s.log.Warn("push for unknown project — skipping",
				"repo", env.Repository.FullName, "installation_id", env.Installation.ID)
			return nil
		}
		if res.Deduped {
			// Non-production push of a commit SHA we already built (e.g. the
			// "Open improvement PR" bot forking a branch off main's HEAD).
			// Same image + preview URL, so we skip the redundant build.
			s.log.Info("push for already-built commit — skipping duplicate build",
				"project_id", res.ProjectID,
				"slug", res.Slug,
				"repo", env.Repository.FullName,
				"ref", env.Ref,
				"sha", env.After,
			)
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
			if err := s.store.AddRepos(ctx, env.Installation.ID, toRepoRows(p.Repositories), s.maxProjects); err != nil {
				return err
			}
			// Belt-and-braces: backfill in case the installation was linked
			// AFTER the AddRepos statement happened in a prior call.
			_ = s.store.SetProjectOwnerByInstallation(ctx, env.Installation.ID)
			// Kick off an initial deploy per repo — user installed the App
			// expecting their app to come up, not to have to make a junk
			// commit to fire the first webhook. Best-effort; logged on fail.
			for _, r := range p.Repositories {
				s.tryAutoDeployRepo(ctx, env.Installation.ID, r)
			}
		}
		return nil

	case "suspend":
		return s.store.SuspendInstallation(ctx, env.Installation.ID, true)

	case "deleted":
		return s.store.DeleteInstallation(ctx, env.Installation.ID)
	}
	return nil
}

// tryAutoDeployRepo enqueues an initial build for a freshly-connected repo
// by asking GitHub for the HEAD SHA of its default branch. Used by both
// installation-event paths (initial install with repos, and adds after
// install). Best-effort: a transient GitHub failure or missing project
// row is logged at WARN and swallowed, because the webhook ack must not
// be blocked by a deploy that we can always retry later via the "Deploy
// now" button. Runs synchronously inside the webhook request context so
// it inherits the receiver's 10s timeout — plenty for a single GitHub
// API call per repo.
func (s *server) tryAutoDeployRepo(ctx context.Context, installationID int64, r githubRepo) {
	branch := strings.TrimSpace(r.DefaultBranch)
	if branch == "" {
		branch = "main"
	}
	sha, err := s.gh.fetchBranchSHA(ctx, installationID, r.FullName, branch)
	if err != nil {
		s.log.Warn("auto-deploy: fetch HEAD sha failed",
			"repo", r.FullName, "branch", branch,
			"installation_id", installationID, "err", err)
		return
	}
	res, err := s.store.EnqueueDeployment(ctx,
		installationID, r.ID, sha, "refs/heads/"+branch, "", "install")
	if err != nil {
		s.log.Warn("auto-deploy: enqueue failed",
			"repo", r.FullName, "sha", sha, "err", err)
		return
	}
	if res == nil {
		// Shouldn't happen — AddRepos just inserted the project row in
		// the same transaction-less batch — but if it does, no build
		// gets stranded; the user can still click "Deploy now".
		s.log.Warn("auto-deploy: project row not found post-AddRepos",
			"repo", r.FullName, "installation_id", installationID)
		return
	}
	s.log.Info("auto-deploy queued from install",
		"deployment_id", res.DeploymentID,
		"project_id", res.ProjectID,
		"slug", res.Slug,
		"repo", r.FullName,
		"branch", branch,
		"sha", sha,
	)
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
		if err := s.store.AddRepos(ctx, env.Installation.ID, toRepoRows(p.RepositoriesAdded), s.maxProjects); err != nil {
			return err
		}
		_ = s.store.SetProjectOwnerByInstallation(ctx, env.Installation.ID)
		// Mirror the "created" path: bring each newly-added repo up to
		// production immediately so the dashboard doesn't sit at "0
		// deployments — push to start" forever.
		for _, r := range p.RepositoriesAdded {
			s.tryAutoDeployRepo(ctx, env.Installation.ID, r)
		}
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
