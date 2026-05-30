package main

// database_handlers.go: HTTP endpoints behind /v1/projects/{id}/database.
// All routes are gated by requireUser; project lookups also enforce
// ownership so a logged-in user can't probe another user's project IDs.

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// projectIDFromPath extracts {id} and validates it as a UUID. Returns
// "" + writes the response on invalid input.
func projectIDFromPath(w http.ResponseWriter, r *http.Request) string {
	id := r.PathValue("id")
	if !uuidRE.MatchString(id) {
		http.Error(w, "invalid project id", http.StatusBadRequest)
		return ""
	}
	return id
}

// dedashRE collapses UUID dashes when building DB identifiers.
var dedashRE = regexp.MustCompile(`-`)

// shortID returns the first 8 hex characters of a UUID for use in PG
// identifiers (limited to 63 chars and case-folded).
func shortID(uuid string) string {
	clean := dedashRE.ReplaceAllString(uuid, "")
	if len(clean) > 8 {
		clean = clean[:8]
	}
	return strings.ToLower(clean)
}

// handleCreateProjectDatabase provisions a per-project Postgres database
// (if one doesn't already exist) and returns the connection string. It
// also writes the DATABASE_URL into a Secret in the tenant namespace so
// the next deploy picks it up automatically.
func (s *server) handleCreateProjectDatabase(w http.ResponseWriter, r *http.Request) {
	id := projectIDFromPath(w, r)
	if id == "" {
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

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	proj, err := s.store.GetProjectForOwner(ctx, id, scope)
	if err != nil {
		s.log.Error("get project failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if proj == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if proj.TenantLogin == "" {
		http.Error(w, "project not yet claimed by an owner (install the GitHub App first)", http.StatusBadRequest)
		return
	}

	// Idempotent: if a DB already exists, return it. The Secret may have
	// been deleted out-of-band so we (re)apply it on every GET-or-create.
	existing, err := s.store.GetProjectDatabaseForOwner(ctx, id, scope)
	if err != nil {
		s.log.Error("get project db failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if existing != nil {
		if err := s.applyDBSecret(ctx, proj.TenantLogin, *existing); err != nil {
			s.log.Error("re-apply db secret failed", "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		writeDBResponse(w, *existing, false)
		return
	}

	// Provision: synthesise names + password, ensure the CNPG superuser
	// URI is configured, then run DDL and persist.
	superURI := strings.TrimSpace(globalSuperuserURI)
	if superURI == "" {
		http.Error(w, "database provisioning disabled (PG_SUPERUSER_URI unset)", http.StatusServiceUnavailable)
		return
	}
	short := shortID(id)
	pd := projectDatabase{
		ProjectID:  id,
		DBName:     "app_" + short,
		RoleName:   "u_" + short,
		Host:       "paas-db-rw.paas-system.svc.cluster.local",
		Port:       5432,
		SecretName: "paas-db-" + truncate(proj.Slug, 50),
	}
	pw, err := genPassword()
	if err != nil {
		s.log.Error("gen password failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	pd.Password = pw

	prov := newDBProvisioner(superURI)
	if err := prov.Provision(ctx, pd.RoleName, pd.DBName, pd.Password); err != nil {
		s.log.Error("provision tenant db failed", "project", id, "err", err)
		http.Error(w, "provisioning failed", http.StatusInternalServerError)
		return
	}

	// Apply the Secret BEFORE the metadata row — if the Secret apply
	// fails, the user can retry and we'll re-run DDL (which is idempotent
	// and rotates the password).
	if err := s.applyDBSecret(ctx, proj.TenantLogin, pd); err != nil {
		s.log.Error("apply db secret failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	if err := s.store.CreateProjectDatabase(ctx, pd); err != nil {
		s.log.Error("persist project db failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	s.log.Info("tenant db provisioned",
		"project_id", id,
		"slug", proj.Slug,
		"db_name", pd.DBName,
		"role_name", pd.RoleName,
		"tenant_login", proj.TenantLogin,
	)
	writeDBResponse(w, pd, true)
}

func (s *server) handleGetProjectDatabase(w http.ResponseWriter, r *http.Request) {
	id := projectIDFromPath(w, r)
	if id == "" {
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

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	proj, err := s.store.GetProjectForOwner(ctx, id, scope)
	if err != nil {
		s.log.Error("get project failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if proj == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	pd, err := s.store.GetProjectDatabaseForOwner(ctx, id, scope)
	if err != nil {
		s.log.Error("get project db failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if pd == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"database": nil})
		return
	}
	writeDBResponse(w, *pd, false)
}

func writeDBResponse(w http.ResponseWriter, pd projectDatabase, created bool) {
	w.Header().Set("Content-Type", "application/json")
	if created {
		w.WriteHeader(http.StatusCreated)
	}
	uri := buildDSN(pd.Host, pd.Port, pd.RoleName, pd.Password, pd.DBName)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"database": map[string]any{
			"host":         pd.Host,
			"port":         pd.Port,
			"db_name":      pd.DBName,
			"role_name":    pd.RoleName,
			"password":     pd.Password,
			"uri":          uri,
			"secret_name":  pd.SecretName,
			"env_var_name": "DATABASE_URL",
		},
	})
}

// applyDBSecret ensures the tenant namespace exists and writes the
// DATABASE_URL Secret. ensureTenant is idempotent and safe to call here
// even when the tenant has never deployed anything (the namespace will
// be created with the right PSA/RQ/LR/NP up-front).
func (s *server) applyDBSecret(ctx context.Context, tenantLogin string, pd projectDatabase) error {
	nsName, err := ensureTenant(ctx, s.k8s, tenantLogin)
	if err != nil {
		return err
	}
	dsn := buildDSN(pd.Host, pd.Port, pd.RoleName, pd.Password, pd.DBName)
	return s.k8s.applySecret(ctx, nsName, pd.SecretName,
		map[string]string{"DATABASE_URL": dsn},
		map[string]string{
			"app.kubernetes.io/managed-by": "control-plane",
			"paas.kind":                    "tenant-db",
		},
	)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// globalSuperuserURI is populated in main() from PG_SUPERUSER_URI. We keep
// it as a package-level var (rather than threading it onto server{}) so
// the existing constructor signature stays untouched.
var globalSuperuserURI string
