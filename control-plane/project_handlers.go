package main

import (
	"encoding/json"
	"net/http"
	"strings"
)

func (s *server) handlePatchProject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidRE.MatchString(id) {
		http.Error(w, "invalid project id", http.StatusBadRequest)
		return
	}
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}

	var body struct {
		DisplayName      *string `json:"display_name"`
		ProductionBranch *string `json:"production_branch"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.DisplayName == nil && body.ProductionBranch == nil {
		http.Error(w, "display_name or production_branch required", http.StatusBadRequest)
		return
	}

	resp := map[string]any{"id": id}

	if body.DisplayName != nil {
		name := strings.TrimSpace(*body.DisplayName)
		if len(name) > 80 {
			http.Error(w, "display_name too long (max 80)", http.StatusBadRequest)
			return
		}
		if err := s.store.UpdateProjectDisplayName(r.Context(), id, name); err != nil {
			s.log.Error("update display_name failed", "project_id", id, "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		resp["display_name"] = name
	}

	if body.ProductionBranch != nil {
		branch := strings.TrimSpace(*body.ProductionBranch)
		if branch == "" {
			http.Error(w, "production_branch cannot be empty", http.StatusBadRequest)
			return
		}
		if len(branch) > 255 {
			http.Error(w, "production_branch too long (max 255)", http.StatusBadRequest)
			return
		}
		if err := s.store.UpdateProjectProductionBranch(r.Context(), id, branch); err != nil {
			s.log.Error("update production_branch failed", "project_id", id, "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		resp["production_branch"] = branch
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
