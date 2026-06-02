package main

// GitHub-OAuth login + opaque-token sessions for the dashboard.
//
// Why GitHub OAuth specifically: we already require a GitHub App for the
// build/deploy plumbing. Turning on "Request user authorization (OAuth)
// during installation" on the same App gives us one identity provider that
// also lets us call the GitHub API on the user's behalf (e.g. listing their
// installations) without ever asking for another credential.
//
// Threat model worth being explicit about:
//   * The plaintext session token lives only in the user's cookie. We
//     persist HMAC(SESSION_SECRET, token), so a database leak alone does
//     not yield working sessions.
//   * State for the OAuth dance is signed-with-expiry; we don't trust a
//     callback that arrives without a fresh state cookie we issued.
//   * The cookie is HttpOnly + Secure + SameSite=Lax. Lax (not Strict) is
//     required because GitHub's redirect back to /auth/callback is a
//     top-level GET from a different origin.

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type authConfig struct {
	clientID     string
	clientSecret string
	// sessionSecret is the HMAC key used to (a) hash the bearer token
	// before storage and (b) sign OAuth state tokens. 32 bytes minimum.
	sessionSecret []byte
	// baseURL is the public origin the dashboard is reached at — used to
	// construct redirect_uri values for GitHub. Must match the callback
	// configured on the GitHub App exactly.
	baseURL string
	// allowedLogins gates sign-in. When non-empty, only these GitHub logins
	// (lower-cased) may complete login — everyone else hits the waitlist
	// screen and never gets a session or a tenant namespace. Empty disables
	// the gate (fully open signup). This is the pre-launch abuse guardrail
	// (e.g. keeping crypto miners out). Set via ALLOWED_GH_LOGINS,
	// comma/space/newline separated.
	allowedLogins map[string]bool
}

const (
	sessionCookie    = "paas_session"
	stateCookie      = "paas_oauth_state"
	sessionLifetime  = 30 * 24 * time.Hour
	stateLifetime    = 10 * time.Minute
	githubAuthURL    = "https://github.com/login/oauth/authorize"
	githubTokenURL   = "https://github.com/login/oauth/access_token"
	githubAPIBase    = "https://api.github.com"
)

func loadAuthConfig() (*authConfig, error) {
	cid := strings.TrimSpace(os.Getenv("GITHUB_APP_CLIENT_ID"))
	cs := strings.TrimSpace(os.Getenv("GITHUB_APP_CLIENT_SECRET"))
	ss := strings.TrimSpace(os.Getenv("SESSION_SECRET"))
	base := strings.TrimSpace(os.Getenv("DASHBOARD_BASE_URL"))
	if base == "" {
		base = "https://spinup.in"
	}
	if cid == "" || cs == "" || ss == "" {
		return nil, errors.New(
			"auth requires GITHUB_APP_CLIENT_ID, GITHUB_APP_CLIENT_SECRET, SESSION_SECRET")
	}
	// Allow either raw bytes or base64. We accept either to avoid making
	// the operator hand-roll base64 in their Secret.
	raw, err := base64.StdEncoding.DecodeString(ss)
	if err != nil || len(raw) < 32 {
		raw = []byte(ss)
	}
	if len(raw) < 32 {
		return nil, fmt.Errorf("SESSION_SECRET must be at least 32 bytes (got %d)", len(raw))
	}
	return &authConfig{
		clientID:      cid,
		clientSecret:  cs,
		sessionSecret: raw,
		baseURL:       strings.TrimRight(base, "/"),
		allowedLogins: parseAllowlist(os.Getenv("ALLOWED_GH_LOGINS")),
	}, nil
}

// parseAllowlist splits a comma/space/newline/semicolon-separated list of
// GitHub logins into a lower-cased set. Blank input yields an empty set,
// which disables the gate.
func parseAllowlist(raw string) map[string]bool {
	m := map[string]bool{}
	for _, f := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\n' || r == '\r' || r == '\t' || r == ';'
	}) {
		if f = strings.ToLower(strings.TrimSpace(f)); f != "" {
			m[f] = true
		}
	}
	return m
}

// loginAllowed reports whether the given GitHub login may sign in. An empty
// allowlist disables the gate (everyone allowed) — so a misconfigured/empty
// env never locks the owner out, it just reverts to open signup.
func (a *authConfig) loginAllowed(login string) bool {
	if len(a.allowedLogins) == 0 {
		return true
	}
	return a.allowedLogins[strings.ToLower(strings.TrimSpace(login))]
}

// --- Tokens, hashing, state -----------------------------------------------

// hashSessionToken returns HMAC-SHA256(secret, token). Constant-time-safe;
// the resulting digest is what we look up in the sessions table.
func hashSessionToken(secret []byte, token string) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(token))
	return m.Sum(nil)
}

// signState packs {nonce, expiresAt} and HMACs them. Returned string is
// safe to put in a cookie. verifyState undoes this. We use signed-cookie
// rather than DB-backed state so the OAuth flow doesn't need a write to
// start.
func signState(secret []byte, ttl time.Duration) (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	payload := fmt.Sprintf("%s.%d", hex.EncodeToString(nonce[:]),
		time.Now().Add(ttl).Unix())
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(payload))
	sig := hex.EncodeToString(m.Sum(nil))
	return payload + "." + sig, nil
}

func verifyState(secret []byte, raw string) error {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return errors.New("malformed state")
	}
	payload := parts[0] + "." + parts[1]
	want, err := hex.DecodeString(parts[2])
	if err != nil {
		return err
	}
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(payload))
	if !hmac.Equal(want, m.Sum(nil)) {
		return errors.New("state signature mismatch")
	}
	var exp int64
	if _, err := fmt.Sscan(parts[1], &exp); err != nil {
		return errors.New("malformed state expiry")
	}
	if time.Now().Unix() > exp {
		return errors.New("state expired")
	}
	return nil
}

// randomToken returns a fresh 32-byte URL-safe opaque session token.
func randomToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// --- Handlers -------------------------------------------------------------

func (s *server) handleLogin(w http.ResponseWriter, r *http.Request) {
	state, err := signState(s.auth.sessionSecret, stateLifetime)
	if err != nil {
		s.log.Error("sign oauth state failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     stateCookie,
		Value:    state,
		Path:     "/",
		MaxAge:   int(stateLifetime / time.Second),
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	q := url.Values{}
	q.Set("client_id", s.auth.clientID)
	q.Set("redirect_uri", s.auth.baseURL+"/auth/callback")
	q.Set("state", state)
	// GitHub Apps don't take a `scope` parameter — permissions are baked
	// into the App's configuration. Including scope causes "redirect_uri
	// MUST match" style errors with some App configurations, so we omit.
	http.Redirect(w, r, githubAuthURL+"?"+q.Encode(), http.StatusFound)
}

func (s *server) handleCallback(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	// GitHub hits this endpoint in two distinct scenarios:
	//
	//  1. OAuth sign-in flow we initiated via /login. GitHub round-trips
	//     ?code=...&state=...  — `state` MUST be validated against the
	//     short-lived cookie we set, or anyone could phish a login.
	//
	//  2. Post-install callback that GitHub initiates after the user
	//     installs the App with "Request user authorization (OAuth)
	//     during installation" enabled. It carries
	//     ?code=...&installation_id=...&setup_action=install  — but no
	//     `state`, because GitHub generated the OAuth code on its own
	//     and never saw our cookie.
	//
	// In case (2) we skip state validation but still exchange the code
	// for a token so we can refresh the user's session (or sign them in
	// if they weren't yet) and immediately claim the new installation.
	isInstallCallback := r.URL.Query().Get("setup_action") == "install"
	if code == "" {
		http.Error(w, "missing code", http.StatusBadRequest)
		return
	}
	if !isInstallCallback {
		if state == "" {
			http.Error(w, "missing state", http.StatusBadRequest)
			return
		}
		cookie, err := r.Cookie(stateCookie)
		if err != nil || subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(state)) != 1 {
			http.Error(w, "state mismatch", http.StatusBadRequest)
			return
		}
		if err := verifyState(s.auth.sessionSecret, state); err != nil {
			http.Error(w, "state invalid: "+err.Error(), http.StatusBadRequest)
			return
		}
		// State cookie is single-use — clear it.
		http.SetCookie(w, &http.Cookie{
			Name: stateCookie, Value: "", Path: "/", MaxAge: -1,
			HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode,
		})
	}

	tok, err := s.exchangeOAuthCode(r.Context(), code)
	if err != nil {
		s.log.Error("oauth token exchange failed", "err", err)
		http.Error(w, "github token exchange failed", http.StatusBadGateway)
		return
	}
	gh, err := s.fetchGitHubUser(r.Context(), tok.AccessToken)
	if err != nil {
		s.log.Error("fetch github user failed", "err", err)
		http.Error(w, "fetch user failed", http.StatusBadGateway)
		return
	}

	// Pre-launch abuse guardrail: if an allowlist is configured, only listed
	// GitHub logins may sign in. We block here — before UpsertUserFromGitHub,
	// session minting, and installation claiming — so a non-allowed account
	// never lands a row, a session, or a tenant namespace.
	if !s.auth.loginAllowed(gh.Login) {
		s.log.Warn("login blocked by allowlist", "user", gh.Login, "id", gh.ID, "ip", clientIP(r))
		s.renderWaitlist(w, gh.Login)
		return
	}

	expires := time.Time{}
	if tok.ExpiresIn > 0 {
		expires = time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second)
	}
	userID, err := s.store.UpsertUserFromGitHub(r.Context(), userUpsert{
		GitHubUserID: gh.ID,
		Login:        gh.Login,
		Email:        gh.Email,
		AvatarURL:    gh.AvatarURL,
		OAuthToken:   tok.AccessToken,
		OAuthExpires: expires,
	})
	if err != nil {
		s.log.Error("upsert user failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Claim any prior installations that arrived before this user existed,
	// and inherit ownership of every project under them. Best-effort: a
	// missing claim doesn't block login.
	if n, err := s.store.ClaimInstallationsForUser(r.Context(), userID, gh.ID); err != nil {
		s.log.Warn("claim installations failed", "user", gh.Login, "err", err)
	} else if n > 0 {
		s.log.Info("claimed installations on login", "user", gh.Login, "count", n)
	}

	// Mint session.
	token, err := randomToken()
	if err != nil {
		s.log.Error("random token failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	hash := hashSessionToken(s.auth.sessionSecret, token)
	if err := s.store.CreateSession(r.Context(), sessionRow{
		TokenHash: hash,
		UserID:    userID,
		UserAgent: r.UserAgent(),
		IP:        clientIP(r),
		ExpiresAt: time.Now().Add(sessionLifetime),
	}); err != nil {
		s.log.Error("create session failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    token,
		Path:     "/",
		MaxAge:   int(sessionLifetime / time.Second),
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	s.log.Info("user signed in", "user", gh.Login, "id", userID)
	http.Redirect(w, r, "/", http.StatusFound)
}

// renderWaitlist serves the invite-only screen to a GitHub user who passed
// OAuth but isn't on the allowlist. 403 so it's clearly "authenticated but
// not authorized", and no session cookie is set.
func (s *server) renderWaitlist(w http.ResponseWriter, login string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusForbidden)
	_, _ = fmt.Fprintf(w, waitlistHTML, html.EscapeString(login))
}

const waitlistHTML = `<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Spinup — Access pending</title>
<style>
  :root{color-scheme:dark}
  *{box-sizing:border-box}
  body{margin:0;min-height:100vh;display:grid;place-items:center;
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
    background:#0b0b0f;color:#e8e8ea}
  .card{max-width:460px;padding:40px;text-align:center}
  .logo{font-weight:800;font-size:26px;letter-spacing:-.02em;margin-bottom:24px}
  .logo span{color:#e5484d}
  h1{font-size:22px;margin:0 0 12px}
  p{color:#a1a1aa;line-height:1.6;margin:0 0 12px}
  .who{display:inline-block;margin-top:8px;padding:6px 12px;border:1px solid #2a2a31;
    border-radius:999px;color:#e8e8ea;font-size:13px}
  a{color:#e5484d;text-decoration:none}
  .foot{margin-top:28px;font-size:13px}
</style></head>
<body><div class="card">
  <div class="logo">spin<span>up</span></div>
  <h1>You're on the waitlist</h1>
  <p>Spinup is invite-only while we scale up safely. Your GitHub account
     isn't on the access list yet.</p>
  <div class="who">signed in as @%s</div>
  <p class="foot"><a href="mailto:hello@spinup.in">Request access</a>
     &nbsp;·&nbsp; <a href="/login">Try another account</a></p>
</div></body></html>`

func (s *server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil && c.Value != "" {
		hash := hashSessionToken(s.auth.sessionSecret, c.Value)
		if err := s.store.DeleteSession(r.Context(), hash); err != nil {
			s.log.Warn("delete session failed", "err", err)
		}
	}
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode,
	})
	// Browser POSTs from the dashboard expect JSON; let them follow up
	// with a client-side redirect rather than us doing 302 here.
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func (s *server) handleMe(w http.ResponseWriter, r *http.Request) {
	u := userFromCtx(r.Context())
	if u == nil {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":         u.ID,
		"login":      u.GitHubLogin,
		"email":      u.Email,
		"avatar_url": u.AvatarURL,
		"is_admin":   u.IsAdmin,
	})
}

// --- Middleware -----------------------------------------------------------

type ctxKey int

const userCtxKey ctxKey = 1

func userFromCtx(ctx context.Context) *sessionUser {
	v, _ := ctx.Value(userCtxKey).(*sessionUser)
	return v
}

// requireUser is middleware: if the request carries a valid session cookie
// the user is attached to the request context; otherwise the handler
// responds with the supplied unauthorised behaviour.
//
// Two behaviours we need:
//   * For HTML routes (/, /login) we redirect to /login.
//   * For JSON APIs we 401 with a small JSON body so the dashboard JS
//     can detect the redirect-to-login condition.
func (s *server) requireUser(next http.Handler, mode string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u := s.userFromRequest(r)
		if u == nil {
			if mode == "html" {
				http.Redirect(w, r, "/login", http.StatusFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthenticated"}`))
			return
		}
		ctx := context.WithValue(r.Context(), userCtxKey, u)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// userFromRequest reads paas_session, hashes it, and looks up the
// matching session row. Returns nil silently on any failure — the caller
// decides whether that means "401" or "redirect to login".
func (s *server) userFromRequest(r *http.Request) *sessionUser {
	c, err := r.Cookie(sessionCookie)
	if err != nil || c.Value == "" {
		return nil
	}
	hash := hashSessionToken(s.auth.sessionSecret, c.Value)
	u, err := s.store.LookupSession(r.Context(), hash)
	if err != nil || u == nil {
		return nil
	}
	return u
}

// --- GitHub OAuth API calls ----------------------------------------------

type oauthTokenResponse struct {
	AccessToken      string `json:"access_token"`
	ExpiresIn        int    `json:"expires_in"`
	RefreshToken     string `json:"refresh_token"`
	RefreshExpiresIn int    `json:"refresh_token_expires_in"`
	Scope            string `json:"scope"`
	TokenType        string `json:"token_type"`
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

func (s *server) exchangeOAuthCode(ctx context.Context, code string) (*oauthTokenResponse, error) {
	body, err := json.Marshal(map[string]string{
		"client_id":     s.auth.clientID,
		"client_secret": s.auth.clientSecret,
		"code":          code,
		"redirect_uri":  s.auth.baseURL + "/auth/callback",
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", githubTokenURL, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.gh.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("token endpoint: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(rb)))
	}
	var out oauthTokenResponse
	if err := json.Unmarshal(rb, &out); err != nil {
		return nil, fmt.Errorf("decode token: %w", err)
	}
	if out.Error != "" {
		return nil, fmt.Errorf("oauth error: %s: %s", out.Error, out.ErrorDescription)
	}
	if out.AccessToken == "" {
		return nil, errors.New("empty access_token from github")
	}
	return &out, nil
}

type githubUser struct {
	ID        int64  `json:"id"`
	Login     string `json:"login"`
	Email     string `json:"email"`
	AvatarURL string `json:"avatar_url"`
}

func (s *server) fetchGitHubUser(ctx context.Context, accessToken string) (*githubUser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", githubAPIBase+"/user", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	resp, err := s.gh.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("get user: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(rb)))
	}
	var u githubUser
	if err := json.Unmarshal(rb, &u); err != nil {
		return nil, err
	}
	if u.ID == 0 || u.Login == "" {
		return nil, errors.New("empty user response from github")
	}
	return &u, nil
}

func clientIP(r *http.Request) string {
	if xf := r.Header.Get("X-Forwarded-For"); xf != "" {
		if i := strings.IndexByte(xf, ','); i > 0 {
			return strings.TrimSpace(xf[:i])
		}
		return strings.TrimSpace(xf)
	}
	host := r.RemoteAddr
	if i := strings.LastIndexByte(host, ':'); i > 0 {
		host = host[:i]
	}
	return host
}
