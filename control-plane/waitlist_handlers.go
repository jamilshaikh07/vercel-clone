package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/mail"
	"strings"
	"time"
)

var validUseCases = map[string]bool{
	"final-year-project": true,
	"portfolio":          true,
	"small-business":     true,
	"hackathon":          true,
	"side-project":       true,
	"other":              true,
}

type waitlistRequest struct {
	Email    string `json:"email"`
	UseCase  string `json:"use_case"`
	Name     string `json:"name"`
	College  string `json:"college"`
}

type waitlistSignupRow struct {
	ID        string `json:"id"`
	Email     string `json:"email"`
	UseCase   string `json:"use_case"`
	Name      string `json:"name"`
	College   string `json:"college"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func (s *server) handleJoinWaitlist(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ip := clientIP(r)
	if !s.waitlistRL.allow(ip) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}

	var body waitlistRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	email := strings.ToLower(strings.TrimSpace(body.Email))
	if email == "" {
		http.Error(w, "email required", http.StatusBadRequest)
		return
	}
	if _, err := mail.ParseAddress(email); err != nil {
		http.Error(w, "invalid email", http.StatusBadRequest)
		return
	}

	useCase := strings.TrimSpace(body.UseCase)
	if !validUseCases[useCase] {
		http.Error(w, "invalid use_case", http.StatusBadRequest)
		return
	}

	name := strings.TrimSpace(body.Name)
	college := strings.TrimSpace(body.College)
	if len(name) > 120 {
		name = name[:120]
	}
	if len(college) > 120 {
		college = college[:120]
	}

	ctx, cancel := contextWithTimeout(r, 5*time.Second)
	defer cancel()

	if err := s.store.UpsertWaitlistSignup(ctx, email, useCase, name, college); err != nil {
		s.log.Error("waitlist signup failed", "err", err, "email", email)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	s.log.Info("waitlist signup", "email", email, "use_case", useCase, "ip", ip)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func (s *server) handleAdminWaitlist(w http.ResponseWriter, r *http.Request) {
	u := userFromCtx(r.Context())
	if u == nil || !s.isOperator(u) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	ctx, cancel := contextWithTimeout(r, 5*time.Second)
	defer cancel()

	rows, err := s.store.ListWaitlistSignups(ctx)
	if err != nil {
		s.log.Error("list waitlist failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"count":   len(rows),
		"signups": rows,
	})
}

// isOperator gates admin-style endpoints. Allowlisted GitHub logins count as
// operators so a solo maintainer doesn't need a separate is_admin SQL flip.
func (s *server) isOperator(u *sessionUser) bool {
	if u == nil {
		return false
	}
	if u.IsAdmin {
		return true
	}
	return s.auth.loginAllowed(u.GitHubLogin)
}

func contextWithTimeout(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}
