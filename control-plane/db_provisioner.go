package main

// db_provisioner.go: opt-in per-project Postgres provisioning.
//
// We share a single CNPG cluster (paas-db) across all tenants because spinning
// up a Cluster per user is wildly overkill for student landing pages. Isolation
// is achieved at the Postgres role+database level:
//
//   * tenant role  has NO superuser / createdb / createrole privileges
//   * tenant database is OWNED by the tenant role; default ACLs revoke PUBLIC
//   * tenant pods reach the cluster via paas-db-rw + a NetworkPolicy gate
//     (see tenant.go), with their personal DATABASE_URL injected from a
//     Secret in the tenant namespace
//
// The tenant role CANNOT read other tenants' databases — Postgres enforces
// the database-owner check before any query plan runs.

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
)

// dbProvisioner connects to the CNPG cluster as the postgres superuser and
// runs DDL to create per-tenant roles + databases. The superuser DSN comes
// from PG_SUPERUSER_URI (mounted from the paas-db-superuser Secret).
type dbProvisioner struct {
	superuserURI string
}

func newDBProvisioner(superuserURI string) *dbProvisioner {
	return &dbProvisioner{superuserURI: superuserURI}
}

// genPassword returns 24 url-safe random bytes. Plenty for an internal DSN
// that only travels over the K8s network and the control-plane database.
func genPassword() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	// URLEncoding is safe inside a postgres URI's password field after escaping.
	return base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(b), nil
}

// Provision is idempotent — running it twice for the same (roleName, dbName)
// is safe and returns nil. Postgres DDL doesn't support transactions for
// CREATE DATABASE so we sequence the calls carefully.
func (p *dbProvisioner) Provision(ctx context.Context, roleName, dbName, password string) error {
	if err := validateIdent(roleName); err != nil {
		return fmt.Errorf("role name: %w", err)
	}
	if err := validateIdent(dbName); err != nil {
		return fmt.Errorf("db name: %w", err)
	}

	conn, err := pgx.Connect(ctx, p.superuserURI)
	if err != nil {
		return fmt.Errorf("connect superuser: %w", err)
	}
	defer conn.Close(ctx)

	// CREATE ROLE — quote_literal the password since Postgres doesn't accept
	// parameter placeholders inside DDL. The validator above guarantees the
	// identifier is safe; the password is single-quoted with escapes doubled.
	pwLit := sqlSingleQuote(password)

	// Idempotent role: check pg_roles first.
	var roleExists bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, roleName,
	).Scan(&roleExists); err != nil {
		return fmt.Errorf("check role: %w", err)
	}
	if !roleExists {
		ddl := fmt.Sprintf(
			`CREATE ROLE %s WITH LOGIN PASSWORD %s NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT`,
			quoteIdent(roleName), pwLit,
		)
		if _, err := conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("create role: %w", err)
		}
	} else {
		// Role pre-existed (re-provision after a crash): rotate the password
		// so the new Secret matches what's actually authenticating.
		ddl := fmt.Sprintf(`ALTER ROLE %s WITH PASSWORD %s`, quoteIdent(roleName), pwLit)
		if _, err := conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("alter role password: %w", err)
		}
	}

	// CREATE DATABASE: also idempotent.
	var dbExists bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, dbName,
	).Scan(&dbExists); err != nil {
		return fmt.Errorf("check database: %w", err)
	}
	if !dbExists {
		ddl := fmt.Sprintf(`CREATE DATABASE %s OWNER %s`, quoteIdent(dbName), quoteIdent(roleName))
		if _, err := conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("create database: %w", err)
		}
	}

	// Lock down: revoke public access on the tenant database. This blocks
	// other roles (including 'app') from connecting unless explicitly granted.
	for _, ddl := range []string{
		fmt.Sprintf(`REVOKE ALL ON DATABASE %s FROM PUBLIC`, quoteIdent(dbName)),
		fmt.Sprintf(`GRANT ALL ON DATABASE %s TO %s`, quoteIdent(dbName), quoteIdent(roleName)),
	} {
		if _, err := conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("lock down db: %w", err)
		}
	}

	return nil
}

// validateIdent allows lowercase letters, digits, and underscore; max 63 (PG limit).
// We pick our own role/db names so this is just a paranoid guard against future
// callers, never an attacker-controlled input.
func validateIdent(s string) error {
	if len(s) == 0 || len(s) > 63 {
		return fmt.Errorf("invalid length %d", len(s))
	}
	for i, r := range s {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_'
		if i == 0 && (r >= '0' && r <= '9') {
			return fmt.Errorf("must not start with a digit: %q", s)
		}
		if !ok {
			return fmt.Errorf("disallowed character %q in %q", r, s)
		}
	}
	return nil
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func sqlSingleQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// buildDSN composes the postgres URI handed to the tenant pod. The fields
// are escaped per RFC 3986 so passwords containing reserved chars survive.
func buildDSN(host string, port int, user, password, dbName string) string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   fmt.Sprintf("%s:%d", host, port),
		Path:   "/" + dbName,
	}
	q := u.Query()
	q.Set("sslmode", "require")
	u.RawQuery = q.Encode()
	return u.String()
}
