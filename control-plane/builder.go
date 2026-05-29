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
	"strings"
	"time"
)

const (
	buildNamespace  = "paas-system"
	deployNamespace = "paas-system"
	tenantHostZone  = "jamilshaikh.in"
	kanikoImage     = "gcr.io/kaniko-project/executor:v1.23.2"
	// TODO: detect from the built image's Config.ExposedPorts. For MVP we
	// assume the modern non-root convention (most rootless images use 8080+).
	tenantPort = 8080
	pollInterval    = 3 * time.Second
	buildTimeout    = 15 * time.Minute
	registryBase    = "ttl.sh"
	tunnelTarget    = "3a067db9-77b1-49c9-a3d4-30f86d16c80d.cfargotunnel.com"
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
		if markErr := w.store.MarkFailed(ctx, claim.DeploymentID, err.Error()); markErr != nil {
			w.log.Error("mark failed failed", "err", markErr)
		}
		return nil
	}
	return nil
}

func (w *worker) runOne(ctx context.Context, c *claimedDeployment) error {
	shortSHA := short(c.CommitSHA)
	image := fmt.Sprintf("%s/%s-%s:24h", registryBase, c.Slug, shortSHA)
	host := fmt.Sprintf("%s-%s.%s", c.Slug, shortSHA, tenantHostZone)
	buildName := fmt.Sprintf("build-%s", strings.ReplaceAll(c.DeploymentID[:8], "-", ""))
	gitSecretName := buildName + "-git"
	deployName := fmt.Sprintf("%s-%s", c.Slug, shortSHA)

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
		Destination:   image,
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
	if err := w.store.MarkDeploying(ctx, c.DeploymentID, image); err != nil {
		return fmt.Errorf("mark deploying: %w", err)
	}

	if err := w.k8s.applyDeployment(ctx, deployNamespace, deployName,
		deploymentManifest(deployInput{
			Name:         deployName,
			Namespace:    deployNamespace,
			Slug:         c.Slug,
			ShortSHA:     shortSHA,
			Image:        image,
			DeploymentID: c.DeploymentID,
			CommitSHA:    c.CommitSHA,
			Port:         tenantPort,
		}),
	); err != nil {
		return fmt.Errorf("apply deployment: %w", err)
	}
	if err := w.k8s.applyService(ctx, deployNamespace, deployName,
		serviceManifest(deployName, deployNamespace, c.Slug, shortSHA, tenantPort),
	); err != nil {
		return fmt.Errorf("apply service: %w", err)
	}
	if err := w.k8s.applyIngressRoute(ctx, deployNamespace, deployName,
		ingressRouteManifest(deployName, deployNamespace, host, c.Slug, shortSHA),
	); err != nil {
		return fmt.Errorf("apply ingressroute: %w", err)
	}

	if err := w.store.MarkReady(ctx, c.DeploymentID, "https://"+host); err != nil {
		return fmt.Errorf("mark ready: %w", err)
	}
	w.log.Info("deployment ready",
		"deployment_id", c.DeploymentID, "url", "https://"+host, "image", image)
	return nil
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
			// race after create
			time.Sleep(time.Second)
			continue
		}
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
	secretEnvFrom := []any{map[string]any{
		"secretRef": map[string]any{"name": in.GitSecretName},
	}}

	cloneCmd := `set -e
            git clone --quiet "$GIT_CLONE_URL" /workspace
            cd /workspace
            git -c advice.detachedHead=false checkout --quiet "$COMMIT_SHA"
            echo "cloned $(git rev-parse --short HEAD) at $(date -u +%FT%TZ)"`

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
								"--snapshot-mode=redo",
								"--use-new-run",
								"--cache=false",
								"--verbosity=info",
							},
							"volumeMounts": []any{workspaceMount},
							"resources": map[string]any{
								"requests": map[string]any{"cpu": "500m", "memory": "1Gi"},
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
					"containers": []any{
						map[string]any{
							"name":            "app",
							"image":           in.Image,
							"imagePullPolicy": "IfNotPresent",
							"ports": []any{
								map[string]any{
									"containerPort": in.Port,
									"name":          "http",
								},
							},
							"resources": map[string]any{
								"requests": map[string]any{"cpu": "20m", "memory": "32Mi"},
								"limits":   map[string]any{"cpu": "500m", "memory": "256Mi"},
							},
						},
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
