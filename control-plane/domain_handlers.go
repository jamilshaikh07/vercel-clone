package main

// HTTP handlers + K8s reconciliation for custom domains.
//
// Endpoints:
//   GET    /v1/projects/{id}/domains                        list
//   POST   /v1/projects/{id}/domains                        add (issues token)
//   POST   /v1/projects/{id}/domains/{hostname}/verify      DNS TXT check
//   DELETE /v1/projects/{id}/domains/{hostname}             remove + tear down route
//
// On a successful verify (or whenever we want to (re-)publish, e.g.
// after a production deploy) we apply a Traefik IngressRoute in the
// project's tenant namespace whose Host(`<hostname>`) maps to the
// project's production-alias Service (or the latest READY Service if
// no production alias exists yet).
//
// DNS verification happens against `_paas-verify.<hostname>` — same
// pattern Vercel/Netlify use. The TXT value is the token we returned
// on the original POST.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
)

// hostnameRE is a permissive sanity check; the DB has its own CHECK and
// the DNS lookup will fail loudly on anything genuinely malformed.
// Single-label hosts (no dot) are explicitly rejected because no public
// resolver will route them.
var hostnameRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)+$`)

// reservedHostnames is the set we refuse to register because they belong
// to the platform itself. Routing them to a tenant would silently break
// the dashboard or apex domain.
var reservedHostnames = map[string]bool{
	"spinup.in":     true,
	"www.spinup.in": true,
	// 'app.spinup.in' and 'paas.spinup.in' are reserved too in case we
	// ever split the marketing site from the dashboard; better to fail
	// a tenant's custom-domain attempt now than let them squat on a
	// hostname we'll want back.
	"app.spinup.in":  true,
	"paas.spinup.in": true,
}

type domainResponse struct {
	Hostname          string `json:"hostname"`
	VerificationToken string `json:"verification_token"`
	Verified          bool   `json:"verified"`
	CreatedAt         string `json:"created_at"`
	VerifiedAt        string `json:"verified_at,omitempty"`
	// VerifyRecord echoes the exact DNS record the user needs to create.
	// Including it in every response simplifies the dashboard markup —
	// no string interpolation needed client-side.
	VerifyName  string `json:"verify_name"`
	VerifyValue string `json:"verify_value"`
	// Target tells the user what to CNAME their hostname to. They MUST
	// add this CNAME in addition to the verification TXT for routing
	// to actually work.
	CNAMETarget string `json:"cname_target"`
}

func (s *server) handleListProjectDomains(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	rows, err := s.store.ListProjectDomains(r.Context(), proj.ID)
	if err != nil {
		s.log.Error("list domains failed", "project_id", proj.ID, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	out := make([]domainResponse, 0, len(rows))
	for _, d := range rows {
		out = append(out, dtoForDomain(d))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"domains":      out,
		"cname_target": tunnelTarget,
	})
}

type addDomainRequest struct {
	Hostname string `json:"hostname"`
}

func (s *server) handleAddProjectDomain(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	var req addDomainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	host := normaliseHostname(req.Hostname)
	if !hostnameRE.MatchString(host) {
		http.Error(w, "hostname must be a valid public FQDN (lowercase, ≥2 labels)", http.StatusBadRequest)
		return
	}
	if reservedHostnames[host] || strings.HasSuffix(host, "."+tenantHostZone) || host == tenantHostZone {
		http.Error(w, "this hostname belongs to the platform and can't be registered as a custom domain", http.StatusBadRequest)
		return
	}
	d, err := s.store.AddProjectDomain(r.Context(), proj.ID, host)
	if err != nil {
		if errors.Is(err, errDomainAlreadyExists) {
			http.Error(w, "hostname already registered on this platform", http.StatusConflict)
			return
		}
		s.log.Error("add domain failed", "project_id", proj.ID, "host", host, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	s.log.Info("custom domain added", "project_id", proj.ID, "host", host)
	writeJSON(w, http.StatusCreated, dtoForDomain(*d))
}

func (s *server) handleVerifyProjectDomain(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	host := normaliseHostname(r.PathValue("hostname"))
	d, err := s.store.GetProjectDomain(r.Context(), proj.ID, host)
	if err != nil {
		s.log.Error("get domain failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if d == nil {
		http.NotFound(w, r)
		return
	}
	if d.VerifiedAt != nil {
		// Idempotent — already verified, just re-publish the route to
		// catch any drift and return the current state.
		if err := s.publishDomainRoute(r.Context(), proj, host); err != nil {
			s.log.Warn("re-publish domain route failed", "host", host, "err", err)
		}
		writeJSON(w, http.StatusOK, dtoForDomain(*d))
		return
	}
	// Look up the TXT records at _paas-verify.<host>. We accept any
	// match because some DNS providers reformat multi-string TXT
	// records (split at 255 chars) — joining + trim handles that.
	if ok, found := checkVerificationTXT(r.Context(), host, d.VerificationToken); !ok {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"verified":     false,
			"hostname":     host,
			"verify_name":  "_paas-verify." + host,
			"verify_value": d.VerificationToken,
			"hint":         "TXT record not found or doesn't match. DNS can take a few minutes to propagate.",
			"found":        found, // what we DID see, for debugging
		})
		return
	}
	verified, err := s.store.MarkDomainVerified(r.Context(), proj.ID, host)
	if err != nil {
		s.log.Error("mark verified failed", "host", host, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !verified {
		// Race with another verifier; treat as success and fall through
		// so we still publish the route and return the latest state.
		s.log.Info("domain already verified concurrently", "host", host)
	}
	if err := s.publishDomainRoute(r.Context(), proj, host); err != nil {
		// Don't fail the verify — DB state is correct; the route can
		// be retried via redeploy or by re-clicking Verify.
		s.log.Error("publish domain route failed", "host", host, "err", err)
	} else {
		s.log.Info("custom domain verified + published", "host", host, "project_id", proj.ID)
	}
	updated, _ := s.store.GetProjectDomain(r.Context(), proj.ID, host)
	if updated != nil {
		writeJSON(w, http.StatusOK, dtoForDomain(*updated))
	} else {
		writeJSON(w, http.StatusOK, map[string]any{"verified": true, "hostname": host})
	}
}

func (s *server) handleDeleteProjectDomain(w http.ResponseWriter, r *http.Request) {
	proj := s.authoriseProject(w, r)
	if proj == nil {
		return
	}
	host := normaliseHostname(r.PathValue("hostname"))
	if host == "" {
		http.Error(w, "missing hostname", http.StatusBadRequest)
		return
	}
	// Tear down the IngressRoute first so the host stops resolving to a
	// stale Service if it's already published. DELETE before DB write
	// means a transient kube-API failure leaves the DB row in place for
	// the user to retry — that's the conservative ordering.
	if err := s.deleteDomainRoute(r.Context(), proj, host); err != nil {
		// Not fatal: route might not exist or have been removed manually.
		s.log.Warn("delete domain route failed (continuing)", "host", host, "err", err)
	}
	deleted, err := s.store.DeleteProjectDomain(r.Context(), proj.ID, host)
	if err != nil {
		s.log.Error("delete domain failed", "host", host, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !deleted {
		http.NotFound(w, r)
		return
	}
	s.log.Info("custom domain removed", "host", host, "project_id", proj.ID)
	w.WriteHeader(http.StatusNoContent)
}

// dtoForDomain converts a store row into the dashboard-facing shape.
// Always includes the verify-record fields so the UI doesn't need to
// know how to construct them.
func dtoForDomain(d projectDomain) domainResponse {
	out := domainResponse{
		Hostname:          d.Hostname,
		VerificationToken: d.VerificationToken,
		Verified:          d.VerifiedAt != nil,
		CreatedAt:         d.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		VerifyName:        "_paas-verify." + d.Hostname,
		VerifyValue:       d.VerificationToken,
		CNAMETarget:       tunnelTarget,
	}
	if d.VerifiedAt != nil {
		out.VerifiedAt = d.VerifiedAt.UTC().Format("2006-01-02T15:04:05Z")
	}
	return out
}

// checkVerificationTXT looks up TXT records at _paas-verify.<host> and
// returns (matched, observed). `observed` is the joined record set we
// saw, returned to the user as a hint when match failed so they can
// see WHY their record didn't match (e.g. trailing whitespace from a
// DNS UI).
func checkVerificationTXT(ctx context.Context, host, expected string) (bool, []string) {
	resolver := net.DefaultResolver
	name := "_paas-verify." + host
	records, err := resolver.LookupTXT(ctx, name)
	if err != nil {
		return false, nil
	}
	for _, r := range records {
		// DNS TXT records can be split across multiple strings; net/lookup
		// already joins per-record. Trim only — the user's record might
		// have stray whitespace from the DNS UI.
		if strings.TrimSpace(r) == expected {
			return true, records
		}
	}
	return false, records
}

// publishDomainRoute applies a Traefik IngressRoute that maps
// Host(`<hostname>`) → the Service of the project's latest READY
// deployment. We source the target from the deployments table rather
// than k8s listing order so the choice is deterministic and matches
// what the user already sees on the dashboard's "latest deploy" UI.
//
// A subsequent production-branch deploy will re-point this route to
// the new Service through the builder's custom-domain hook (builder.go).
func (s *server) publishDomainRoute(ctx context.Context, proj *projectInfo, host string) error {
	if proj.TenantLogin == "" {
		return fmt.Errorf("project has no tenant namespace yet")
	}
	deps, err := s.store.ListDeploymentsForProjects(ctx, []string{proj.ID}, 5)
	if err != nil {
		return fmt.Errorf("list deployments: %w", err)
	}
	var targetSHA string
	for _, d := range deps[proj.ID] {
		if d.Status == "ready" {
			targetSHA = d.CommitSHA
			break
		}
	}
	if targetSHA == "" {
		return fmt.Errorf("no READY deployment to route to yet — push a commit first")
	}
	ns := tenantNamespaceFor(proj.TenantLogin)
	// Deployment name convention matches builder.go runOne: <slug>-<short7>.
	deployName := fmt.Sprintf("%s-%s", proj.Slug, short(targetSHA))
	routeName := customDomainRouteName(host)
	manifest := productionAliasManifest(routeName, ns, host, deployName, proj.Slug)
	return s.k8s.applyIngressRoute(ctx, ns, routeName, manifest)
}

// deleteDomainRoute removes the custom-domain IngressRoute, if any.
// Idempotent — a 404 from kube is treated as success.
func (s *server) deleteDomainRoute(ctx context.Context, proj *projectInfo, host string) error {
	if proj.TenantLogin == "" {
		return nil
	}
	ns := tenantNamespaceFor(proj.TenantLogin)
	return s.k8s.deleteIngressRoute(ctx, ns, customDomainRouteName(host))
}

// customDomainRouteName returns the deterministic IngressRoute name for
// a custom hostname. We replace dots with dashes because DNS-1123 names
// don't allow dots; prefix with "custom-" so the route is greppable in
// the cluster.
func customDomainRouteName(host string) string {
	return "custom-" + strings.NewReplacer(".", "-", "_", "-").Replace(host)
}
