package main

// promote_handlers.go: instant rollback by swapping the production
// alias to an already-built deployment — no rebuild.
//
// POST /v1/deployments/{id}/promote
//
// Every commit's build artefacts live forever in the cluster:
//
//   * Image: pushed to the in-cluster registry as
//     registry.paas-system.svc:5000/<slug>-<sha7> (and stays there).
//   * K8s Deployment, Service and per-SHA IngressRoute named
//     <slug>-<sha7> live in paas-tenant-<login> and are never deleted
//     by the builder. They may be at replicas=0 if the project is
//     stopped, but the objects themselves persist.
//
// That means promotion ≡ re-pointing the production alias IngressRoute
// (prod-<slug>) at an older Service. Equivalent to
//   kubectl rollout undo deployment/<name> --to-revision=N
// but at the Service/Route layer instead of the ReplicaSet layer,
// because we model each commit as its own Deployment.
//
// Custom domains follow the same alias, so they cut over atomically
// with the system zone. Per-SHA preview URLs are unaffected.
//
// Semantics: a true rollback. The promoted pod runs with whatever
// env vars / image it was built with — if a user added env vars after
// that build, they won't be applied unless they redeploy (rebuild).

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

type promoteResponse struct {
	DeploymentID string `json:"deployment_id"`
	ProjectID    string `json:"project_id"`
	Slug         string `json:"slug"`
	CommitSHA    string `json:"commit_sha"`
	Host         string `json:"host"`
	URL          string `json:"url"`
	// CustomDomains lists the verified hostnames whose IngressRoutes
	// were also flipped to point at this deployment, so the dashboard
	// can show the user exactly what cut over.
	CustomDomains []string `json:"custom_domains,omitempty"`
}

func (s *server) handlePromoteDeployment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidRE.MatchString(id) {
		http.Error(w, "invalid deployment id", http.StatusBadRequest)
		return
	}
	u := userFromCtx(r.Context())
	if u == nil {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	scope := u.ID
	if u.IsAdmin {
		scope = ""
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	dep, err := s.store.GetDeploymentForUser(ctx, id, scope)
	if err != nil {
		s.log.Error("promote: get deployment failed", "id", id, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if dep == nil {
		// 404 covers both "no such row" and "not yours" so we don't
		// leak which deployment IDs exist across tenants.
		http.NotFound(w, r)
		return
	}
	if dep.Status != "ready" {
		http.Error(w, "deployment is not ready (cannot promote a "+dep.Status+" build)", http.StatusBadRequest)
		return
	}

	tenantLogin, err := s.store.GetDeploymentTenantLogin(ctx, id, scope)
	if err != nil {
		s.log.Error("promote: get tenant login failed", "id", id, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if tenantLogin == "" {
		http.Error(w, "deployment has no tenant namespace yet", http.StatusBadRequest)
		return
	}
	ns := tenantNamespaceFor(tenantLogin)

	// Same naming convention as builder.go runOne: per-commit
	// Deployment + Service share this name. The Service is what the
	// production alias must point at.
	deployName := fmt.Sprintf("%s-%s", dep.Slug, short(dep.CommitSHA))

	// Swap the system-zone production alias to this deployment.
	prodHost := fmt.Sprintf("%s.%s", dep.Slug, tenantHostZone)
	prodRouteName := "prod-" + dep.Slug
	if err := s.k8s.applyIngressRoute(ctx, ns, prodRouteName,
		productionAliasManifest(prodRouteName, ns, prodHost, deployName, dep.Slug),
	); err != nil {
		s.log.Error("promote: apply production alias failed",
			"id", id, "host", prodHost, "err", err)
		http.Error(w, "promote failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Source-of-truth: record the new alias target so the dashboard's
	// 'current' row tag moves to this commit on the next refresh. Done
	// AFTER the alias is applied so a write that fails leaves the
	// pointer pointing at the previous target — never at a commit
	// whose Service isn't actually behind the alias.
	if err := s.store.SetProductionDeployment(ctx, dep.ProjectID, dep.ID); err != nil {
		// Non-fatal: the alias swap succeeded, the user sees correct
		// traffic. The pointer will self-correct on the next push.
		s.log.Warn("promote: set production_deployment_id failed",
			"id", id, "project_id", dep.ProjectID, "err", err)
	}

	// Cut every verified custom domain over to the same Service so
	// the user's www / apex hostnames don't drift behind the system
	// zone. Errors here are non-fatal — the system-zone promote has
	// already succeeded; we log and continue so a single misbehaving
	// route can't block the rollback.
	customDomains, derr := s.store.ListVerifiedDomainsForProject(ctx, dep.ProjectID)
	if derr != nil {
		s.log.Warn("promote: list custom domains failed", "id", id, "err", derr)
	}
	promotedDomains := make([]string, 0, len(customDomains))
	for _, host := range customDomains {
		routeName := customDomainRouteName(host)
		if err := s.k8s.applyIngressRoute(ctx, ns, routeName,
			productionAliasManifest(routeName, ns, host, deployName, dep.Slug),
		); err != nil {
			s.log.Error("promote: apply custom domain route failed",
				"id", id, "host", host, "err", err)
			continue
		}
		promotedDomains = append(promotedDomains, host)
	}

	s.log.Info("deployment promoted",
		"deployment_id", id,
		"project_id", dep.ProjectID,
		"slug", dep.Slug,
		"commit_sha", dep.CommitSHA,
		"target_service", deployName,
		"prod_host", prodHost,
		"custom_domains", promotedDomains,
		"user", u.GitHubLogin,
	)

	writeJSON(w, http.StatusOK, promoteResponse{
		DeploymentID:  dep.ID,
		ProjectID:     dep.ProjectID,
		Slug:          dep.Slug,
		CommitSHA:     dep.CommitSHA,
		Host:          prodHost,
		URL:           "https://" + prodHost,
		CustomDomains: promotedDomains,
	})
}

