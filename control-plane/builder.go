package main

// Worker that turns queued deployments into running pods.
//
// Lifecycle for one deployment row:
//
//   queued ──claim──▶ building
//                       │
//                       ├── mint installation token
//                       ├── create per-build Secret (git creds)
//                       ├── create Kaniko Job
//                       └── poll until Succeeded or Failed
//                              │
//             ┌────────────────┴────────────────┐
//             ▼                                 ▼
//          failed                          deploying
//        (error msg)                            │
//                                ├── apply Deployment
//                                ├── apply Service
//                                ├── apply IngressRoute
//                                └── ready (with URL)
//
// We use a single in-process goroutine that polls every few seconds. The
// SQL claim uses FOR UPDATE SKIP LOCKED so adding replicas later is safe.

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

const (
	buildNamespace = "paas-system"
	// Tenant Deployment/Service/IngressRoute now live in
	// paas-tenant-<login> — see tenant.go. Build Jobs stay in paas-system
	// so build-time Secrets are never visible from tenant pods.
	tenantHostZone = "spinup.in"
	kanikoImage     = "gcr.io/kaniko-project/executor:v1.23.2"
	// TODO: detect from the built image's Config.ExposedPorts. For MVP we
	// assume the modern non-root convention (most rootless images use 8080+).
	tenantPort = 8080
	pollInterval = 3 * time.Second
	buildTimeout = 15 * time.Minute
	// Registry has two endpoints for the same backing storage:
	//   * registryPushHost — in-cluster Service, plain HTTP. Kaniko uses this
	//     so blob uploads don't hit Cloudflare's 100s edge timeout (524).
	//   * registryPullHost — public HTTPS via the Cloudflare Tunnel. Tenant
	//     pods pull through here so we don't have to teach containerd to
	//     trust internal HTTP registries.
	// The registry stores by repo+tag — hostnames are just transport endpoints,
	// so a push via one hostname is pullable via the other.
	registryPushHost = "registry.paas-system.svc.cluster.local:5000"
	registryPullHost = "registry.spinup.in"
	dockerCfgSecret  = "registry-dockercfg"
	tunnelTarget     = "3a067db9-77b1-49c9-a3d4-30f86d16c80d.cfargotunnel.com"
	// GitHub commit status check
	statusContext = "paas/deploy"
	// dashboardBaseURL is used as target_url for pending/failed status checks.
	// Successful deployments point at the live preview URL instead.
	dashboardBaseURL = "https://spinup.in"
)

type worker struct {
	store *store
	gh    *githubApp
	k8s   *kubeClient
	log   *slog.Logger
}

// Run blocks until ctx is cancelled. Errors per-deployment are logged and
// recorded on the row; the loop never terminates on a build failure.
func (w *worker) Run(ctx context.Context) {
	w.log.Info("build worker started")
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			w.log.Info("build worker stopping")
			return
		case <-t.C:
			if err := w.tick(ctx); err != nil {
				w.log.Error("worker tick failed", "err", err)
			}
		}
	}
}

func (w *worker) tick(ctx context.Context) error {
	// Reconcile: rows that have been non-terminal longer than buildTimeout
	// belong to a worker that died (crashed pod, rollout race, OOM, etc.).
	// Reset them so we can pick them up. The build path is idempotent and
	// reuses the in-cluster Job if it's still progressing.
	if n, err := w.store.RequeueStale(ctx, buildTimeout); err != nil {
		w.log.Error("reconcile stale failed", "err", err)
	} else if n > 0 {
		w.log.Info("reconciled stale deployments", "count", n)
	}

	claim, err := w.store.ClaimNextQueued(ctx)
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	if claim == nil {
		return nil
	}
	w.log.Info("claimed deployment",
		"deployment_id", claim.DeploymentID, "slug", claim.Slug,
		"repo", claim.RepoFullName, "sha", short(claim.CommitSHA))

	// Use a per-deployment context so a stuck build can't wedge the whole worker.
	buildCtx, cancel := context.WithTimeout(ctx, buildTimeout)
	defer cancel()

	if err := w.runOne(buildCtx, claim); err != nil {
		w.log.Error("deployment failed",
			"deployment_id", claim.DeploymentID, "err", err)
		if markErr := w.store.MarkFailedIfInFlight(ctx, claim.DeploymentID, err.Error()); markErr != nil {
			w.log.Error("mark failed failed", "err", markErr)
		}
		w.postStatus(ctx, claim, "failure", err.Error(), deploymentLogURL(claim.DeploymentID))
		return nil
	}
	return nil
}

// postStatus posts a GitHub commit status, swallowing errors. The build is
// the source of truth; a missing or stale check must never block deploys.
func (w *worker) postStatus(ctx context.Context, c *claimedDeployment, state, desc, target string) {
	postCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := w.gh.setCommitStatus(postCtx, c.InstallationID,
		c.RepoFullName, c.CommitSHA, state, desc, target, statusContext); err != nil {
		w.log.Warn("set commit status failed",
			"deployment_id", c.DeploymentID, "state", state, "err", err)
		return
	}
	w.log.Info("commit status posted",
		"deployment_id", c.DeploymentID, "state", state, "desc", desc)
}

// deploymentLogURL returns a dashboard URL that auto-opens the logs panel
// for one deployment. The fragment is read by static/dashboard.html on load.
func deploymentLogURL(id string) string {
	return dashboardBaseURL + "/#d=" + id
}

func (w *worker) runOne(ctx context.Context, c *claimedDeployment) error {
	shortSHA := short(c.CommitSHA)
	repoPath := fmt.Sprintf("%s:%s", c.Slug, shortSHA)
	pushImage := registryPushHost + "/" + repoPath
	pullImage := registryPullHost + "/" + repoPath
	host := fmt.Sprintf("%s-%s.%s", c.Slug, shortSHA, tenantHostZone)
	buildName := buildJobName(c.DeploymentID)
	gitSecretName := buildName + "-git"
	deployName := fmt.Sprintf("%s-%s", c.Slug, shortSHA)

	// First commit status: pending → "building". target_url points at the
	// dashboard with the deployment pre-selected so clicking the yellow
	// dot in GitHub takes you straight to the live logs.
	w.postStatus(ctx, c, "pending", "building", deploymentLogURL(c.DeploymentID))

	// 1. Mint a short-lived installation token for cloning the repo.
	token, err := w.gh.installationToken(ctx, c.InstallationID)
	if err != nil {
		return fmt.Errorf("mint installation token: %w", err)
	}

	// 2. Per-build Secret holds the auth'd clone URL + SHA. Kept out of
	//    the Job spec args so `kubectl describe job` doesn't leak the token.
	cloneURL := fmt.Sprintf("https://x-access-token:%s@github.com/%s.git",
		token, c.RepoFullName)
	if err := w.k8s.applySecret(ctx, buildNamespace, gitSecretName,
		map[string]string{
			"GIT_CLONE_URL": cloneURL,
			"COMMIT_SHA":    c.CommitSHA,
		},
		map[string]string{
			"app.kubernetes.io/managed-by": "control-plane",
			"app.kubernetes.io/component":  "build",
			"paas.deployment":              c.DeploymentID,
		},
	); err != nil {
		return fmt.Errorf("create git secret: %w", err)
	}

	// 3. Job: initContainer clones the repo into an emptyDir, Kaniko
	//    builds from that directory (dir:// context). We do this because
	//    Kaniko's own git fetcher (1.23.x) silently mishandles URL-embedded
	//    HTTPS credentials — independently verified with alpine/git that
	//    the same URL clones fine outside Kaniko.
	//
	//    ensureBuildJob is idempotent so a worker restart in the middle of
	//    a build re-attaches to the running Job instead of redoing work.
	jobSpec := buildJobManifest(buildJobInput{
		Name:          buildName,
		Namespace:     buildNamespace,
		Destination:   pushImage,
		GitSecretName: gitSecretName,
		DeploymentID:  c.DeploymentID,
		ProjectSlug:   c.Slug,
	})
	if err := w.ensureBuildJob(ctx, buildNamespace, buildName, jobSpec); err != nil {
		return fmt.Errorf("ensure job: %w", err)
	}

	// 4. Poll until terminal.
	if err := w.waitForJob(ctx, buildNamespace, buildName); err != nil {
		return err
	}

	// 5. Transition to deploying, write image, apply tenant resources.
	if err := w.store.MarkDeploying(ctx, c.DeploymentID, pullImage); err != nil {
		return fmt.Errorf("mark deploying: %w", err)
	}

	// Tenant isolation: every deploy lives inside paas-tenant-<login>.
	// ensureTenant is idempotent — on the warm path it's four GETs that
	// confirm the namespace, quota, limits, and network policies already
	// exist. The build Job + per-build Secret intentionally stay in
	// paas-system; tenant pods never see them.
	if c.TenantLogin == "" {
		return fmt.Errorf("deployment %s has no tenant login — refusing to deploy unscoped", c.DeploymentID)
	}
	tenantNS, err := w.ensureTenant(ctx, c.TenantLogin)
	if err != nil {
		return fmt.Errorf("ensure tenant namespace: %w", err)
	}

	// Optional Postgres binding: if the user clicked "Add Database" on
	// this project, inject DATABASE_URL from the tenant-ns Secret and
	// label the pod so the allow-paas-db-egress NetworkPolicy applies.
	// Absence of a binding is the common case and not an error.
	pdb, err := w.store.GetProjectDatabase(ctx, c.ProjectID)
	if err != nil {
		// Don't fail the deploy — log and proceed without DB binding.
		w.log.Error("lookup project db failed", "deployment_id", c.DeploymentID, "err", err)
	}
	dbSecretName := ""
	if pdb != nil {
		dbSecretName = pdb.SecretName
	}

	// Project env vars: materialise into a per-project Secret named
	// paas-env-<slug> in the tenant namespace. We re-apply on every
	// deploy even when the map is empty, because (a) the user may have
	// just deleted the last var and we need the pod to no longer see
	// it, and (b) applySecret is idempotent so the warm path is just
	// a PUT that resolves to "no change". The Deployment manifest
	// references the Secret with optional=true so a missing Secret
	// (e.g. user manually deleted it via kubectl) is non-fatal.
	envMap, err := w.store.MapProjectEnv(ctx, c.ProjectID)
	if err != nil {
		// Don't fail the deploy on a transient DB hiccup — log and
		// proceed with whatever env Secret is already in the cluster.
		w.log.Error("lookup project env failed", "deployment_id", c.DeploymentID, "err", err)
		envMap = nil
	}
	envSecretName := "paas-env-" + c.Slug
	if envMap != nil {
		if err := w.k8s.applySecret(ctx, tenantNS, envSecretName, envMap,
			map[string]string{
				"app.kubernetes.io/managed-by": "control-plane",
				"app.kubernetes.io/component":  "tenant-env",
				"paas.project":                 c.Slug,
			},
		); err != nil {
			return fmt.Errorf("apply env secret: %w", err)
		}
	}

	if err := w.k8s.applyDeployment(ctx, tenantNS, deployName,
		deploymentManifest(deployInput{
			Name:          deployName,
			Namespace:     tenantNS,
			Slug:          c.Slug,
			ShortSHA:      shortSHA,
			Image:         pullImage,
			DeploymentID:  c.DeploymentID,
			CommitSHA:     c.CommitSHA,
			Port:          tenantPort,
			DBSecretName:  dbSecretName,
			EnvSecretName: envSecretName,
		}),
	); err != nil {
		return fmt.Errorf("apply deployment: %w", err)
	}
	if err := w.k8s.applyService(ctx, tenantNS, deployName,
		serviceManifest(deployName, tenantNS, c.Slug, shortSHA, tenantPort),
	); err != nil {
		return fmt.Errorf("apply service: %w", err)
	}
	if err := w.k8s.applyIngressRoute(ctx, tenantNS, deployName,
		ingressRouteManifest(deployName, tenantNS, host, c.Slug, shortSHA),
	); err != nil {
		return fmt.Errorf("apply ingressroute: %w", err)
	}

	liveURL := "https://" + host
	if err := w.store.MarkReady(ctx, c.DeploymentID, liveURL); err != nil {
		return fmt.Errorf("mark ready: %w", err)
	}

	// Production alias: if this commit is on the project's production
	// branch, point <slug>.<zone> at the per-deployment Service. The route
	// name is shared across all production deploys for this project, so
	// applying it for a newer SHA atomically cuts traffic over to the new
	// Service without touching the per-SHA preview URLs.
	prodURL := liveURL
	if c.Ref == "refs/heads/"+c.ProductionBranch {
		prodHost := fmt.Sprintf("%s.%s", c.Slug, tenantHostZone)
		prodRouteName := "prod-" + c.Slug
		if err := w.k8s.applyIngressRoute(ctx, tenantNS, prodRouteName,
			productionAliasManifest(prodRouteName, tenantNS, prodHost, deployName, c.Slug),
		); err != nil {
			// Don't fail the deploy — the per-SHA URL is already serving.
			w.log.Error("apply production alias failed",
				"deployment_id", c.DeploymentID, "host", prodHost, "err", err)
		} else {
			prodURL = "https://" + prodHost
			w.log.Info("production alias applied",
				"deployment_id", c.DeploymentID, "host", prodHost, "target_service", deployName)
			// Source-of-truth: record which deployment the alias now
			// points at. The dashboard reads this from /v1/projects
			// to pick the 'current' row in the deployments list.
			// Non-fatal: a stale pointer just falls back to the old
			// 'latest READY' heuristic in the UI on next refresh.
			if err := w.store.SetProductionDeployment(ctx, c.ProjectID, c.DeploymentID); err != nil {
				w.log.Warn("set production_deployment_id failed",
					"deployment_id", c.DeploymentID, "project_id", c.ProjectID, "err", err)
			}
		}

		// Custom domains: re-apply IngressRoutes for every verified
		// custom hostname so they cut over to the new Service at the
		// same instant as the production alias. Errors are logged but
		// non-fatal — the per-SHA URL is already serving and the user
		// can re-publish individual domains from the Domains page.
		if domains, err := w.store.ListVerifiedDomainsForProject(ctx, c.ProjectID); err != nil {
			w.log.Warn("list verified domains failed",
				"deployment_id", c.DeploymentID, "err", err)
		} else {
			for _, host := range domains {
				routeName := customDomainRouteName(host)
				if err := w.k8s.applyIngressRoute(ctx, tenantNS, routeName,
					productionAliasManifest(routeName, tenantNS, host, deployName, c.Slug),
				); err != nil {
					w.log.Error("apply custom domain route failed",
						"deployment_id", c.DeploymentID, "host", host, "err", err)
					continue
				}
				w.log.Info("custom domain route applied",
					"deployment_id", c.DeploymentID, "host", host, "target_service", deployName)
			}
		}
	}

	// Garbage-collect old Deployments and Services for this project. Only
	// runs on production-branch deploys; the new pod is already serving so
	// it's safe to remove the previous ones.
	if c.Ref == "refs/heads/"+c.ProductionBranch {
		oldSel := fmt.Sprintf("paas.project=%s,paas.commit!=%s", c.Slug, shortSHA)
		if err := w.k8s.deleteAllByLabel(ctx, tenantNS, "apis/apps/v1", "deployments", oldSel); err != nil {
			w.log.Warn("cleanup old deployments failed", "deployment_id", c.DeploymentID, "err", err)
		}
		if err := w.k8s.deleteAllByLabel(ctx, tenantNS, "api/v1", "services", oldSel); err != nil {
			w.log.Warn("cleanup old services failed", "deployment_id", c.DeploymentID, "err", err)
		}
	}

	// Success status: target_url is the production alias when applicable,
	// otherwise the per-SHA preview URL.
	w.postStatus(ctx, c, "success", "deployed", prodURL)
	w.log.Info("deployment ready",
		"deployment_id", c.DeploymentID, "url", liveURL, "prod_url", prodURL,
		"push_image", pushImage, "pull_image", pullImage)
	return nil
}

// productionAliasManifest is a Traefik IngressRoute that points
// <slug>.<zone> at one specific per-deployment Service. The route name is
// stable per project (prod-<slug>) so successive ready deployments on the
// production branch update it in place via applyIngressRoute's create-or-PUT.
func productionAliasManifest(routeName, namespace, host, serviceName, slug string) map[string]any {
	return map[string]any{
		"apiVersion": "traefik.io/v1alpha1",
		"kind":       "IngressRoute",
		"metadata": map[string]any{
			"name":      routeName,
			"namespace": namespace,
			"labels": map[string]any{
				"app.kubernetes.io/managed-by": "control-plane",
				"app.kubernetes.io/component":  "production-alias",
				"paas.project":                 slug,
			},
			"annotations": map[string]any{
				"external-dns.alpha.kubernetes.io/target": tunnelTarget,
			},
		},
		"spec": map[string]any{
			"entryPoints": []any{"web"},
			"routes": []any{
				map[string]any{
					"match": fmt.Sprintf("Host(`%s`)", host),
					"kind":  "Rule",
					"services": []any{
						map[string]any{
							"name": serviceName,
							"port": 80,
						},
					},
				},
			},
		},
	}
}

// ensureBuildJob makes a Kaniko Job exist for this deployment, recreating
// it only if a previous attempt failed. This is what lets the worker
// safely resume a deployment after a pod restart.
func (w *worker) ensureBuildJob(ctx context.Context, namespace, name string, spec map[string]any) error {
	phase, err := w.k8s.getJobPhase(ctx, namespace, name)
	if err != nil {
		return err
	}
	if phase != nil {
		switch {
		case phase.Failed > 0 || phase.FailureMsg != "":
			w.log.Info("recreating previously failed build job", "job", name, "reason", phase.FailureMsg)
			if err := w.k8s.deleteJob(ctx, namespace, name); err != nil {
				return err
			}
			time.Sleep(2 * time.Second)
		default:
			w.log.Info("attaching to existing build job", "job", name,
				"active", phase.Active, "succeeded", phase.Succeeded)
			return nil
		}
	}
	if err := w.k8s.createJob(ctx, namespace, spec); err != nil {
		return fmt.Errorf("create job: %w", err)
	}
	w.log.Info("build job created", "job", name)
	return nil
}

func (w *worker) waitForJob(ctx context.Context, namespace, name string) error {
	deadline := time.Now().Add(buildTimeout)
	var seenJob bool
	var nilSince time.Time
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		phase, err := w.k8s.getJobPhase(ctx, namespace, name)
		if err != nil {
			return fmt.Errorf("get job phase: %w", err)
		}
		if phase == nil {
			if seenJob {
				return fmt.Errorf("build job deleted unexpectedly")
			}
			if nilSince.IsZero() {
				nilSince = time.Now()
			} else if time.Since(nilSince) > 2*time.Minute {
				return fmt.Errorf("build job not found after create")
			}
			time.Sleep(time.Second)
			continue
		}
		seenJob = true
		nilSince = time.Time{}
		switch {
		case phase.Succeeded >= 1:
			return nil
		case phase.Failed >= 1 || phase.FailureMsg != "":
			msg := phase.FailureMsg
			if msg == "" {
				msg = fmt.Sprintf("kaniko job failed (failed=%d)", phase.Failed)
			}
			return fmt.Errorf("build failed: %s", msg)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("build timed out after %s", buildTimeout)
		}
		time.Sleep(pollInterval)
	}
}

// --- manifest builders -----------------------------------------------------

// autoDockerfileScript runs after `git checkout` inside the clone
// initContainer. It writes /workspace/Dockerfile when the user's repo
// doesn't already have one, so students with a plain Vite / CRA /
// Astro / Gatsby / static site can deploy with zero infra config.
//
// Detection order:
//
//  1. Existing Dockerfile  → leave it alone (escape hatch for power users).
//  2. package.json present → grep its dependencies for a known build tool
//     and generate a multi-stage build: Node builds to <outdir>, then
//     nginx-unprivileged serves it on port 8080 (matches tenantPort).
//  3. index.html at root   → pure static site, single-stage nginx copy.
//  4. None of the above    → exit 1 with a friendly explanation; the
//     deployment is marked failed and the dashboard shows the message.
//
// The generated nginx config does SPA fallback (try_files … /index.html)
// which is correct for client-routed apps and harmless for plain static.
// All output ends up under /usr/share/nginx/html which the unprivileged
// nginx image (uid 101) can read but not modify at runtime — same as
// any other tenant pod under our PSA `restricted` namespace.
const autoDockerfileScript = `
            # ---- Auto-Dockerfile (Slice D) ----
            #
            # Two design notes worth carrying around:
            #
            # 1. The nginx config (default.conf) is written to a file in
            #    the workspace (.paas-nginx.conf) and then 'COPY'd into
            #    the image. An earlier version generated it inline via
            #    'RUN { echo ...; } > /etc/nginx/conf.d/default.conf'
            #    with $$-escaped $uri references; the interaction between
            #    shell-heredoc expansion + Dockerfile parser + kaniko
            #    rendered $uri as literal $$uri in the final file,
            #    which nginx rejected with "invalid variable name". The
            #    COPY approach has zero escape levels and just works.
            #
            # 2. The .dockerignore we emit deliberately does NOT exclude
            #    .paas-nginx.conf — kaniko needs it in the build context
            #    to satisfy the COPY. For the pure-static branch the
            #    file will also land at /usr/share/nginx/html/, but
            #    nginx's default config ignores dotfiles so it's not
            #    served. Tidy enough for the MVP.
            cat > .dockerignore <<DI
.git
.gitignore
.dockerignore
Dockerfile
node_modules
.next
.nuxt
.cache
DI

            if [ -f Dockerfile.user ] || [ -f Dockerfile.original ]; then
                : # placeholder — power-users can override later
            fi
            if [ -f Dockerfile ]; then
                # User-provided Dockerfile takes precedence. Drop the
                # auto .dockerignore so we don't shadow their build
                # context expectations.
                echo "auto-detect: using user-provided Dockerfile"
                rm -f .dockerignore
            elif [ -f package.json ]; then
                # Pick the first framework whose dep we find. Order matters:
                # Next/Nuxt come last because they need their own SSR server
                # (handled as static export for MVP).
                if grep -q '"vite"' package.json; then
                    FW="vite";    OUTDIR="dist"
                elif grep -q '"react-scripts"' package.json; then
                    FW="cra";     OUTDIR="build"
                elif grep -q '"astro"' package.json; then
                    FW="astro";   OUTDIR="dist"
                elif grep -q '"gatsby"' package.json; then
                    FW="gatsby";  OUTDIR="public"
                elif grep -q '"@sveltejs/kit"' package.json; then
                    FW="sveltekit"; OUTDIR="build"
                elif grep -q '"parcel"' package.json; then
                    FW="parcel";  OUTDIR="dist"
                else
                    echo "ERROR: auto-detect found package.json but no recognised"
                    echo "       build tool. Supported: vite, react-scripts (CRA),"
                    echo "       astro, gatsby, @sveltejs/kit, parcel."
                    echo "       Either add one of those, or commit a Dockerfile."
                    exit 1
                fi

                # Choose the lockfile-aware install command so npm ci works
                # when there's a package-lock.json, falling back to install
                # for repos that only ship package.json. yarn/pnpm get the
                # same treatment.
                if   [ -f package-lock.json ]; then INSTALL="npm ci"
                elif [ -f yarn.lock ];         then INSTALL="corepack enable && yarn install --frozen-lockfile"
                elif [ -f pnpm-lock.yaml ];    then INSTALL="corepack enable && pnpm install --frozen-lockfile"
                else                                INSTALL="npm install"
                fi
                BUILD="npm run build"
                if [ -f yarn.lock ];      then BUILD="yarn build"; fi
                if [ -f pnpm-lock.yaml ]; then BUILD="pnpm build"; fi

                echo "auto-detect: framework=$FW outdir=$OUTDIR install='$INSTALL' build='$BUILD'"

                # Quoted heredoc (<<'NGINX'): NO shell expansion, so a
                # literal '$uri' survives the trip to the file unchanged.
                # This is the file the Dockerfile COPYs in further down.
                cat > .paas-nginx.conf <<'NGINX'
server {
  listen 8080;
  server_name _;
  root /usr/share/nginx/html;
  index index.html;
  location / { try_files $uri $uri/ /index.html; }
  location ~* \.(?:js|css|woff2?|png|jpg|jpeg|svg|gif|ico|webp)$ {
    add_header Cache-Control "public, max-age=31536000, immutable";
  }
}
NGINX

                # nginx-unprivileged already defaults to USER 101, so we
                # don't flip USER at all. --chown=101:101 makes the
                # copied files owned by the runtime user so nginx can
                # open them without extra permission dances.
                cat > Dockerfile <<EOF
# --- AUTO-GENERATED by paas (Slice D) — framework: $FW ---
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json yarn.lock* pnpm-lock.yaml* ./
RUN $INSTALL
COPY . .
RUN $BUILD

FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY --chown=101:101 --from=build /app/$OUTDIR /usr/share/nginx/html
COPY --chown=101:101 .paas-nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 8080
EOF
                echo "auto-detect: wrote Dockerfile ($(wc -l < Dockerfile) lines) + .paas-nginx.conf"

            elif [ -f index.html ]; then
                echo "auto-detect: pure-static site (no package.json, found index.html)"
                cat > .paas-nginx.conf <<'NGINX'
server {
  listen 8080;
  server_name _;
  root /usr/share/nginx/html;
  index index.html;
  location / { try_files $uri $uri/ /index.html; }
}
NGINX

                # Same COPY-the-file approach as the package.json branch;
                # see the long comment at the top of this script.
                cat > Dockerfile <<'EOF'
# --- AUTO-GENERATED by paas (Slice D) — pure static ---
FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY --chown=101:101 . /usr/share/nginx/html
COPY --chown=101:101 .paas-nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 8080
EOF

            else
                echo "ERROR: no Dockerfile, package.json, or index.html at repo root."
                echo "       paas couldn't figure out how to build this repo."
                echo "       Add one of those three files and push again."
                exit 1
            fi
            # ---- /Auto-Dockerfile ----
`

type buildJobInput struct {
	Name          string
	Namespace     string
	Destination   string
	GitSecretName string
	DeploymentID  string
	ProjectSlug   string
}

func buildJobManifest(in buildJobInput) map[string]any {
	workspaceMount := map[string]any{"name": "workspace", "mountPath": "/workspace"}
	// Kaniko reads docker config from /kaniko/.docker/config.json — mount the
	// project-wide registry credentials there so it can push.
	dockerCfgMount := map[string]any{
		"name":      "docker-config",
		"mountPath": "/kaniko/.docker",
		"readOnly":  true,
	}
	secretEnvFrom := []any{map[string]any{
		"secretRef": map[string]any{"name": in.GitSecretName},
	}}

	cloneCmd := `set -e
            git clone --quiet "$GIT_CLONE_URL" /workspace
            cd /workspace
            git -c advice.detachedHead=false checkout --quiet "$COMMIT_SHA"
            echo "cloned $(git rev-parse --short HEAD) at $(date -u +%FT%TZ)"
` + autoDockerfileScript

	return map[string]any{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata": map[string]any{
			"name":      in.Name,
			"namespace": in.Namespace,
			"labels": map[string]any{
				"app.kubernetes.io/component":  "build",
				"app.kubernetes.io/managed-by": "control-plane",
				"paas.deployment":              in.DeploymentID,
				"paas.project":                 in.ProjectSlug,
			},
		},
		"spec": map[string]any{
			"backoffLimit":            0,
			"activeDeadlineSeconds":   1200,
			"ttlSecondsAfterFinished": 1800,
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": map[string]any{
						"app.kubernetes.io/component": "build",
						"paas.deployment":             in.DeploymentID,
					},
				},
				"spec": map[string]any{
					"restartPolicy": "Never",
					"volumes": []any{
						map[string]any{
							"name":     "workspace",
							"emptyDir": map[string]any{},
						},
						map[string]any{
							"name": "docker-config",
							"secret": map[string]any{
								"secretName": dockerCfgSecret,
								"items": []any{
									map[string]any{
										"key":  ".dockerconfigjson",
										"path": "config.json",
									},
								},
							},
						},
					},
					"initContainers": []any{
						map[string]any{
							"name":         "clone",
							"image":        "alpine/git:latest",
							"command":      []any{"sh", "-c", cloneCmd},
							"envFrom":      secretEnvFrom,
							"volumeMounts": []any{workspaceMount},
							"resources": map[string]any{
								"requests": map[string]any{"cpu": "50m", "memory": "64Mi"},
								"limits":   map[string]any{"cpu": "500m", "memory": "256Mi"},
							},
						},
					},
					"containers": []any{
						map[string]any{
							"name":  "kaniko",
							"image": kanikoImage,
							"args": []any{
								"--context=dir:///workspace",
								"--dockerfile=Dockerfile",
								"--destination=" + in.Destination,
								"--insecure",
								"--snapshot-mode=redo",
								"--use-new-run",
								"--cache=false",
								"--verbosity=info",
							},
							"volumeMounts": []any{workspaceMount, dockerCfgMount},
							"resources": map[string]any{
								"requests": map[string]any{"cpu": "1", "memory": "1536Mi"},
								"limits":   map[string]any{"cpu": "2", "memory": "4Gi"},
							},
						},
					},
				},
			},
		},
	}
}

type deployInput struct {
	Name         string
	Namespace    string
	Slug         string
	ShortSHA     string
	Image        string
	DeploymentID string
	CommitSHA    string
	Port         int
	// DBSecretName, when non-empty, names the Secret in the same
	// namespace whose `DATABASE_URL` key is wired as an env var. The
	// pod also picks up the `paas.db=enabled` label so the
	// allow-paas-db-egress NetworkPolicy permits CNPG traffic.
	DBSecretName string
	// EnvSecretName, when non-empty, names the per-project Secret in
	// the tenant namespace whose keys are projected wholesale as env
	// vars via envFrom. Marked optional in the manifest so a missing
	// Secret (no env vars set yet, or first-time deploy before the
	// worker materialised it) is non-fatal.
	EnvSecretName string
}

func deploymentManifest(in deployInput) map[string]any {
	labels := map[string]any{
		"app":                          in.Name,
		"app.kubernetes.io/managed-by": "control-plane",
		"app.kubernetes.io/component":  "tenant",
		"paas.project":                 in.Slug,
		"paas.deployment":              in.DeploymentID,
		"paas.commit":                  in.ShortSHA,
	}
	if in.DBSecretName != "" {
		labels["paas.db"] = "enabled"
	}
	// envVars populated below; declared up-front so we can append.
	var envVars []any
	if in.DBSecretName != "" {
		envVars = append(envVars, map[string]any{
			"name": "DATABASE_URL",
			"valueFrom": map[string]any{
				"secretKeyRef": map[string]any{
					"name": in.DBSecretName,
					"key":  "DATABASE_URL",
				},
			},
		})
	}
	return map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      in.Name,
			"namespace": in.Namespace,
			"labels":    labels,
			"annotations": map[string]any{
				"paas.commit-sha": in.CommitSHA,
			},
		},
		"spec": map[string]any{
			"replicas": 1,
			"selector": map[string]any{
				"matchLabels": map[string]any{"app": in.Name},
			},
			"strategy": map[string]any{
				"type": "RollingUpdate",
				"rollingUpdate": map[string]any{
					"maxSurge":       1,
					"maxUnavailable": 0,
				},
			},
			"template": map[string]any{
				"metadata": map[string]any{"labels": labels},
				"spec": map[string]any{
					"imagePullSecrets": []any{
						map[string]any{"name": dockerCfgSecret},
					},
					// Pod-level securityContext: required by PSA `restricted`
					// in the tenant namespace. The numeric uid/gid match
					// nginxinc/nginx-unprivileged (101) and most common
					// non-root distro images. Tenants overriding this in
					// their own Dockerfile won't be blocked by us — only
					// the must-have flags (runAsNonRoot, seccomp, no
					// privilege escalation) are enforced.
					"securityContext": map[string]any{
						"runAsNonRoot": true,
						"runAsUser":    101,
						"runAsGroup":   101,
						"fsGroup":      101,
						"seccompProfile": map[string]any{
							"type": "RuntimeDefault",
						},
					},
					"containers": []any{
						func() map[string]any {
							c := map[string]any{
								"name":            "app",
								"image":           in.Image,
								"imagePullPolicy": "IfNotPresent",
								"ports": []any{
									map[string]any{
										"containerPort": in.Port,
										"name":          "http",
									},
								},
								// Runtime pod budget. Static nginx idles at <5m CPU
								// and bursts to ~30m on a real cold load; Node
								// apps behave similarly for most of our workloads,
								// but CPU-bound work in a request path (bcrypt
								// hashing, JSON-heavy responses) throttled hard
								// at 200m — reported as multi-second signup
								// latency on chem-desk. Bumped to 500m/50m; at
								// 500m limit an 8-core tenant quota still gives
								// ~16 replicas of headroom.
								"resources": map[string]any{
									"requests": map[string]any{"cpu": "50m", "memory": "32Mi"},
									"limits":   map[string]any{"cpu": "500m", "memory": "256Mi"},
								},
								"securityContext": map[string]any{
									"allowPrivilegeEscalation": false,
									"capabilities": map[string]any{
										"drop": []any{"ALL"},
									},
									// readOnlyRootFilesystem isn't enforced by
									// `restricted` — leaving it writable so
									// nginx can rewrite its temp paths.
								},
							}
							if len(envVars) > 0 {
								c["env"] = envVars
							}
							// envFrom projects every key of the per-project
							// Secret as an env var named after the key. Going
							// through envFrom (vs listing each key explicitly)
							// means CRUD on env vars doesn't require us to
							// re-template the Deployment YAML — we just update
							// the Secret in place and the pod sees new values
							// on its next restart (which the redeploy triggers).
							if in.EnvSecretName != "" {
								c["envFrom"] = []any{
									map[string]any{
										"secretRef": map[string]any{
											"name":     in.EnvSecretName,
											"optional": true,
										},
									},
								}
							}
							return c
						}(),
					},
				},
			},
		},
	}
}

func serviceManifest(name, namespace, slug, shortSHA string, port int) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"labels": map[string]any{
				"app.kubernetes.io/managed-by": "control-plane",
				"paas.project":                 slug,
				"paas.commit":                  shortSHA,
			},
		},
		"spec": map[string]any{
			"selector": map[string]any{"app": name},
			"ports": []any{
				map[string]any{
					"name":       "http",
					"port":       80,
					"targetPort": port,
				},
			},
		},
	}
}

func ingressRouteManifest(name, namespace, host, slug, shortSHA string) map[string]any {
	return map[string]any{
		"apiVersion": "traefik.io/v1alpha1",
		"kind":       "IngressRoute",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"labels": map[string]any{
				"app.kubernetes.io/managed-by": "control-plane",
				"paas.project":                 slug,
				"paas.commit":                  shortSHA,
			},
			"annotations": map[string]any{
				"external-dns.alpha.kubernetes.io/target": tunnelTarget,
			},
		},
		"spec": map[string]any{
			"entryPoints": []any{"web"},
			"routes": []any{
				map[string]any{
					"match": fmt.Sprintf("Host(`%s`)", host),
					"kind":  "Rule",
					"services": []any{
						map[string]any{
							"name": name,
							"port": 80,
						},
					},
				},
			},
		},
	}
}

func short(sha string) string {
	if len(sha) < 8 {
		return sha
	}
	return sha[:8]
}
