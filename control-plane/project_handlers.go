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
		DisplayName *string `json:"display_name"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if body.DisplayName == nil {
		http.Error(w, "display_name required", http.StatusBadRequest)
		return
	}

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

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":           id,
		"display_name": name,
	})
}
