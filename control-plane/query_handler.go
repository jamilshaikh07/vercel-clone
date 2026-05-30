package main

// query_handler.go: in-dashboard SQL console for tenant Postgres.
//
// Design notes:
//
//   - We connect to the tenant's database as the tenant role (NOT the
//     paas-db-superuser). This means the role's own privileges define
//     what the query can do. Since the role is NOSUPERUSER NOCREATEDB
//     NOCREATEROLE and only has CONNECT on its own database, the
//     blast radius is naturally bounded.
//
//   - We deliberately don't try to parse / sanitize the user's SQL.
//     This is their database; they can DROP TABLE if they want to.
//     What we DO bound is server-side cost: query timeout, row cap,
//     SQL length cap.
//
//   - We return only the first result set. Multi-statement scripts
//     work for DDL/insert chains but only the last SELECT's columns
//     are shown — same convention as Supabase/Vercel's SQL editor.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	queryMaxSQLBytes = 100 << 10 // 100 KiB
	queryMaxRows     = 1000
	queryTimeout     = 10 * time.Second
)

type queryRequest struct {
	SQL string `json:"sql"`
}

type queryColumn struct {
	Name string `json:"name"`
	// Type is the pgx-reported OID name (e.g. "int4", "text"). Cheap
	// metadata that lets the UI right-align numbers vs strings.
	Type string `json:"type"`
}

type queryResponse struct {
	Columns   []queryColumn   `json:"columns"`
	Rows      [][]any         `json:"rows"`
	RowCount  int64           `json:"row_count"`
	Truncated bool            `json:"truncated"`
	Command   string          `json:"command"`           // e.g. SELECT, INSERT, UPDATE
	Elapsed   int64           `json:"elapsed_ms"`
	Error     *queryErrorBody `json:"error,omitempty"`
}

type queryErrorBody struct {
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
	Detail  string `json:"detail,omitempty"`
	Hint    string `json:"hint,omitempty"`
	// Position is 1-based char index inside the SQL where Postgres
	// flagged the problem — invaluable for the UI to highlight.
	Position int `json:"position,omitempty"`
}

func (s *server) handleRunQuery(w http.ResponseWriter, r *http.Request) {
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

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, queryMaxSQLBytes+1024))
	if err != nil {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	var req queryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	req.SQL = strings.TrimSpace(req.SQL)
	if req.SQL == "" {
		http.Error(w, "sql is required", http.StatusBadRequest)
		return
	}
	if len(req.SQL) > queryMaxSQLBytes {
		http.Error(w, "sql too long", http.StatusRequestEntityTooLarge)
		return
	}

	// Lookup the tenant DB binding the caller owns.
	lookupCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	pd, err := s.store.GetProjectDatabaseForOwner(lookupCtx, id, scope)
	if err != nil {
		s.log.Error("query: get project db failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if pd == nil {
		http.Error(w, "no database attached to this project — click '+ Add Postgres' first", http.StatusNotFound)
		return
	}

	// Connect AS THE TENANT ROLE — not the superuser. ACL is enforced
	// by Postgres; we don't need to filter the SQL.
	dsn := buildDSN(pd.Host, pd.Port, pd.RoleName, pd.Password, pd.DBName)
	execCtx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	start := time.Now()
	resp := runTenantQuery(execCtx, dsn, req.SQL)
	resp.Elapsed = time.Since(start).Milliseconds()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// runTenantQuery does the actual connect+execute+marshal cycle.
//
// We use pgconn's simple-protocol Exec (not pgx.Query, which forces the
// extended protocol and rejects multi-statement input with
// "cannot insert multiple commands into a prepared statement"). The
// simple protocol matches what Vercel/Supabase/psql do: a user can
// paste a chain of CREATE TABLE / INSERT / SELECT and we walk each
// result. We surface the LAST result set that contains rows; if every
// command was DDL/DML with no SELECT, we report the last command tag.
//
// Error paths return a populated queryResponse (HTTP 200 with .error)
// so the UI cleanly distinguishes "your SQL had a bug" from "network
// is broken".
func runTenantQuery(ctx context.Context, dsn, sql string) queryResponse {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return queryResponse{Error: &queryErrorBody{Message: "connect: " + err.Error()}}
	}
	defer conn.Close(context.Background())

	mrr := conn.PgConn().Exec(ctx, sql)
	defer mrr.Close()

	out := queryResponse{Rows: [][]any{}}
	var lastTag pgconn.CommandTag

	for mrr.NextResult() {
		rr := mrr.ResultReader()
		fields := rr.FieldDescriptions()

		// Reset accumulators each iteration — we only keep the LAST
		// result set's columns/rows (or the last command tag if none
		// have columns).
		cols := make([]queryColumn, len(fields))
		for i, f := range fields {
			cols[i] = queryColumn{
				Name: string(f.Name),
				Type: pgOIDName(uint32(f.DataTypeOID)),
			}
		}
		hasRows := len(fields) > 0

		var rows [][]any
		var rowCount int64
		truncated := false
		for rr.NextRow() {
			if rowCount >= queryMaxRows {
				truncated = true
				break
			}
			raw := rr.Values()
			vals := make([]any, len(raw))
			for i := range raw {
				if raw[i] == nil {
					vals[i] = nil
					continue
				}
				// rr.Values returns the on-the-wire text representation
				// for the simple protocol — always a []byte we can stringify.
				vals[i] = string(raw[i])
			}
			rows = append(rows, vals)
			rowCount++
		}
		tag, err := rr.Close()
		if err != nil {
			out.Error = pgErrorToBody(err)
			return out
		}
		lastTag = tag

		if hasRows {
			out.Columns = cols
			out.Rows = rows
			out.Truncated = truncated
			out.RowCount = rowCount
		}
	}
	if err := mrr.Close(); err != nil {
		out.Error = pgErrorToBody(err)
		return out
	}

	tagStr := lastTag.String()
	if parts := strings.Fields(tagStr); len(parts) > 0 {
		out.Command = parts[0]
	}
	// For non-SELECT trailing commands, use the server-reported row count.
	if out.Command != "SELECT" && len(out.Columns) == 0 {
		out.RowCount = lastTag.RowsAffected()
	}
	return out
}

func pgErrorToBody(err error) *queryErrorBody {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return &queryErrorBody{
			Message:  pgErr.Message,
			Code:     pgErr.Code,
			Detail:   pgErr.Detail,
			Hint:     pgErr.Hint,
			Position: int(pgErr.Position),
		}
	}
	return &queryErrorBody{Message: err.Error()}
}

// pgOIDName maps the handful of Postgres OIDs we care about to short
// type names. Anything else falls through as "oid:<num>" — the UI just
// uses this for right-alignment hints, not real type semantics.
func pgOIDName(oid uint32) string {
	switch oid {
	case 16:
		return "bool"
	case 17:
		return "bytea"
	case 20, 21, 23:
		return "int"
	case 25, 1043:
		return "text"
	case 700, 701, 1700:
		return "numeric"
	case 1082:
		return "date"
	case 1114, 1184:
		return "timestamp"
	case 2950:
		return "uuid"
	case 114, 3802:
		return "json"
	}
	return "" // empty → UI treats as default (left-aligned text)
}
