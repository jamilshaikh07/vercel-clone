package main

// env_handlers.go: HTTP endpoints behind /v1/projects/{id}/env. All routes
// are gated by requireUser; project lookups also enforce ownership so a
// logged-in user can't enumerate or write another user's env vars.
//
// Effect timing: writes are persisted immediately but DO NOT trigger a
// rebuild. The new value is picked up by the next deploy (next git push
// or manual redeploy). This matches Vercel's behaviour and keeps the
// write path cheap — we don't need to rerun kaniko just because the
// user fat-fingered a typo in STRIPE_KEY.

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"time"
)

// envNameRE mirrors the CHECK constraint in 0005_project_env_vars.sql so
// we reject bad input at the API edge with a useful message rather than
// a generic Postgres error.
var envNameRE = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

// envValueMaxBytes guards against a tenant trying to stuff a 10 MB blob
// into etcd via an env var. Matches the DB CHECK length cap.
const envValueMaxBytes = 64 * 1024

// authoriseProject resolves the {id} path param, enforces ownership, and
// returns the project record. Writes the HTTP response and returns nil
// on any failure path so the caller can just `if proj == nil { return }`.
func (s *server) authoriseProject(w http.ResponseWriter, r *http.Request) *projectInfo {
	id := projectIDFromPath(w, r)
	if id == "" {
		return nil
	}
	u := userFromCtx(r.Context())
	if u == nil {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return nil
	}
	scope := u.ID
	if u.IsAdmin {
		scope = ""
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	proj, err := s.store.GetProjectForOwner(ctx, id, scope)
	if err != nil {
		s.log.Error("get project failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return nil
	}
	if proj == nil {
		http.NotFound(w, r)
		return nil
	}
	return proj
}

func (s *server) handleListProjectEnv(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	vars, err := s.store.ListProjectEnv(ctx, proj.ID)
	if err != nil {
		s.log.Error("list project env failed", "project_id", proj.ID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"vars": vars,
	})
}

type envUpsertReq struct {
	Value string `json:"value"`
}

func (s *server) handleUpsertProjectEnv(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	name := r.PathValue("name")
	if !envNameRE.MatchString(name) || len(name) > 128 {
		http.Error(w, "invalid name (must match ^[A-Z_][A-Z0-9_]*$, ≤128 chars)", http.StatusBadRequest)
		return
	}
	if _, reserved := ReservedEnvVarNames[name]; reserved {
		http.Error(w, "name is reserved by the platform (DATABASE_URL is managed by the project database binding)", http.StatusBadRequest)
		return
	}

	var req envUpsertReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, envValueMaxBytes+1024)).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if len(req.Value) > envValueMaxBytes {
		http.Error(w, "value too large (max 64 KiB)", http.StatusRequestEntityTooLarge)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	updatedAt, err := s.store.UpsertProjectEnv(ctx, proj.ID, name, req.Value)
	if err != nil {
		s.log.Error("upsert project env failed", "project_id", proj.ID, "name", name, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(projectEnvVar{
		Name:      name,
		Value:     req.Value,
		UpdatedAt: updatedAt,
	})
}

func (s *server) handleDeleteProjectEnv(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	name := r.PathValue("name")
	if !envNameRE.MatchString(name) || len(name) > 128 {
		http.Error(w, "invalid name", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	ok, err := s.store.DeleteProjectEnv(ctx, proj.ID, name)
	if err != nil {
		s.log.Error("delete project env failed", "project_id", proj.ID, "name", name, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
