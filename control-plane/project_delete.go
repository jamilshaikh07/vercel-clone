package main

// Delete-project flow: tear down tenant + build K8s resources, then
// remove the projects row (CASCADE clears deployments, domains, env, DB
// metadata). Does NOT revoke GitHub App access — the user does that in
// GitHub if they want pushes to stop arriving.

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

type projectDeleteMeta struct {
	ID             string
	Slug           string
	FullName       string
	TenantLogin    string
	InstallationID int64
	RepoID         int64
}

func (s *server) handleDeleteProject(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()

	meta, err := s.store.GetProjectDeleteMeta(ctx, proj.ID, deleteScope(r))
	if err != nil {
		s.log.Error("get project delete meta failed", "project_id", proj.ID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if meta == nil {
		http.NotFound(w, r)
		return
	}

	warnings := s.teardownProjectResources(ctx, meta)
	deleted, err := s.store.DeleteProject(ctx, meta.ID)
	if err != nil {
		s.log.Error("delete project failed", "project_id", meta.ID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !deleted {
		http.NotFound(w, r)
		return
	}

	s.log.Info("project deleted",
		"project_id", meta.ID,
		"slug", meta.Slug,
		"repo", meta.FullName,
		"k8s_warnings", len(warnings),
	)
	writeJSON(w, http.StatusOK, map[string]any{
		"deleted":  true,
		"slug":     meta.Slug,
		"warnings": warnings,
	})
}

func (s *server) teardownProjectResources(ctx context.Context, proj *projectDeleteMeta) []string {
	var warnings []string
	sel := "paas.project=" + proj.Slug

	if proj.TenantLogin != "" {
		ns := tenantNamespaceFor(proj.TenantLogin)
		for _, err := range []error{
			s.k8s.deleteAllByLabel(ctx, ns, "apis/apps/v1", "deployments", sel),
			s.k8s.deleteAllByLabel(ctx, ns, "api/v1", "services", sel),
			s.k8s.deleteAllByLabel(ctx, ns, "apis/traefik.io/v1alpha1", "ingressroutes", sel),
			s.k8s.deleteAllByLabel(ctx, ns, "api/v1", "secrets", sel),
		} {
			if err != nil {
				warnings = append(warnings, err.Error())
			}
		}
		for _, name := range []string{
			"paas-env-" + proj.Slug,
			"paas-db-" + truncate(proj.Slug, 50),
		} {
			if err := s.k8s.deleteSecret(ctx, ns, name); err != nil {
				warnings = append(warnings, err.Error())
			}
		}
	}

	if err := s.k8s.deleteAllByLabel(ctx, buildNamespace, "apis/batch/v1", "jobs", sel); err != nil {
		warnings = append(warnings, err.Error())
	}

	depIDs, err := s.store.ListDeploymentIDsForProject(ctx, proj.ID)
	if err != nil {
		warnings = append(warnings, "list deployments: "+err.Error())
	} else {
		for _, id := range depIDs {
			if err := s.k8s.deleteSecret(ctx, buildNamespace, buildJobName(id)+"-git"); err != nil {
				warnings = append(warnings, err.Error())
			}
		}
	}

	if domains, err := s.store.ListVerifiedDomainsForProject(ctx, proj.ID); err == nil {
		for _, host := range domains {
			warnings = append(warnings,
				fmt.Sprintf("Custom domain %q may still route via cloudflared — remove from tunnel config and DNS if no longer needed", host))
		}
	}

	return warnings
}

func deleteScope(r *http.Request) string {
	u := userFromCtx(r.Context())
	if u == nil {
		return ""
	}
	if u.IsAdmin {
		return ""
	}
	return u.ID
}
