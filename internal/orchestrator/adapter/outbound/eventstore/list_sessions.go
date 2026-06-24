// SPDX-License-Identifier: Apache-2.0

package eventstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// This file is the read-only ListSessions store method backing the admin/tenant
// session-management API (Feature I / ADR-0027). It is an ADDITIVE method on the
// adapter-side EventStore consumer-superset, NOT on the frozen app.EventLogPort,
// so the `var _ app.EventLogPort = (*Store)(nil)` assertion is unaffected.
//
// The query/cursor TYPES are owned by the inbound/grpc edge (the consumer that
// defines the EventStore interface), and aliased here so *Store.ListSessions
// satisfies igrpc.EventStore exactly — the same direction Feature O's cost-read
// types take (eventstore imports inbound/grpc for the read-superset types).
//
// Like every other store read it goes through beginTenantTx -> SET LOCAL
// app.current_tenant -> RLS, so a foreign tenant sees nothing even though the SQL
// applies NO tenant_id filter — the request tenant is a guard at the edge, never a
// filter key here. Paging is keyset on the composite (created_at, id) total order
// (no OFFSET, so deep pages do not degrade), with LIMIT page_size+1 (passed by the
// server) to detect whether a further page exists.

// ListSessionsQuery is the read-only filter/keyset input to [Store.ListSessions].
// It is an alias of the edge-owned [igrpc.ListSessionsQuery] so *Store satisfies
// the igrpc.EventStore interface without a wrapper.
type ListSessionsQuery = igrpc.ListSessionsQuery

// listCursor is the keyset position a page_token encodes; aliased from the
// edge-owned [igrpc.ListCursor] so the store and the server agree on the cursor
// shape byte-for-byte.
type listCursor = igrpc.ListCursor

// listSessionColumns is the session column list (in order) that scanSessionRows
// scans. It mirrors loadSessionTx's projection so a SessionSummary cannot drift
// from a freshly-loaded domain.Session.
const listSessionColumns = "id, tenant_id, parent_id, forked_from_seq, status, head_seq, last_event_at, created_at, updated_at, mode"

// selectSessionsListSQL is the keyset list query (ascending). The predicates are
// NULL-guarded so an empty filter disables that clause:
//   - $1 (text[]): status OR-filter — NULL/empty lists all statuses.
//   - $2 (timestamptz): created_at >= lower bound — NULL disables it.
//   - $3 (timestamptz): created_at <  upper bound (exclusive, half-open) — NULL disables it.
//   - $4 (timestamptz), $5 (text): the (created_at, id) keyset cursor — NULL on the
//     first page; on a continuation, rows STRICTLY AFTER the cursor in (created_at,
//     id) order.
//   - $6 (int): LIMIT (the server passes page_size+1 to detect has_more).
//
// NO OFFSET (keyset paging only). It selects the same session columns loadSessionTx
// scans so the projection cannot silently drift from domain.Session.
const selectSessionsListSQL = `SELECT ` + listSessionColumns + `
  FROM sessions
 WHERE ($1::text[] IS NULL OR status = ANY($1))
   AND ($2::timestamptz IS NULL OR created_at >= $2)
   AND ($3::timestamptz IS NULL OR created_at <  $3)
   AND ($4::timestamptz IS NULL OR (created_at, id) > ($4, $5))
 ORDER BY created_at ASC, id ASC
 LIMIT $6`

// selectSessionsListDescSQL is selectSessionsListSQL flipped to newest-first: the
// ORDER BY descends and the keyset cursor comparison reverses (rows strictly
// BEFORE the cursor in (created_at, id) order). It is derived from the ascending
// constant by substituting the two direction-bearing fragments so the column list,
// filters, and LIMIT stay byte-identical between the two directions.
var selectSessionsListDescSQL = func() string {
	q := strings.Replace(selectSessionsListSQL, "(created_at, id) > ($4, $5)", "(created_at, id) < ($4, $5)", 1)
	q = strings.Replace(q, "ORDER BY created_at ASC, id ASC", "ORDER BY created_at DESC, id DESC", 1)
	return q
}()

// listCursorMagic prefixes the encoded cursor payload so a base64 blob that is not
// one of our cursors (e.g. base64 of arbitrary text) is rejected by decode rather
// than mis-parsed into a zero cursor.
const listCursorMagic = "blc1:"

// listCursorWire is the JSON wire shape of a listCursor inside the opaque token. It
// is a fixed local struct (independent of the edge type's field tags) so the token
// codec is self-contained and stable.
type listCursorWire struct {
	C int64  `json:"c"` // created_at ms
	I string `json:"i"` // id
	D bool   `json:"d"` // descending
}

// encodeListCursor encodes a [listCursor] as an opaque, URL-safe base64 token.
// Clients MUST treat it as opaque.
func encodeListCursor(c listCursor) string {
	b, err := json.Marshal(listCursorWire{C: c.CreatedAtMs, I: c.ID, D: c.Descending})
	if err != nil {
		// listCursorWire is a fixed, always-marshalable struct; this cannot fail.
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(append([]byte(listCursorMagic), b...))
}

// decodeListCursor decodes an opaque page_token back into a [listCursor]. An empty
// token is the first page (the zero cursor), NOT an error. A malformed/garbage
// token is a typed error the server maps to InvalidArgument — never a silent
// fall-through to the first page (which would leak rows / break paging).
func decodeListCursor(token string) (listCursor, error) {
	if token == "" {
		return listCursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return listCursor{}, fmt.Errorf("eventstore: malformed page_token (not base64): %w", err)
	}
	if !strings.HasPrefix(string(raw), listCursorMagic) {
		return listCursor{}, fmt.Errorf("eventstore: malformed page_token (bad prefix)")
	}
	var w listCursorWire
	if err := json.Unmarshal(raw[len(listCursorMagic):], &w); err != nil {
		return listCursor{}, fmt.Errorf("eventstore: malformed page_token (bad payload): %w", err)
	}
	return listCursor{CreatedAtMs: w.C, ID: w.I, Descending: w.D}, nil
}

// ListSessions returns the caller-tenant's sessions matching q, in (created_at, id)
// order (ascending, or descending when q.Descending), after q.Cursor, capped at
// q.Limit. It is RLS-scoped via beginTenantTx (the SET LOCAL tenant GUC), so a
// foreign tenant sees nothing even though the SQL carries no tenant_id filter; a
// context with no tenant fails closed (beginTenantTx returns the no-tenant error).
// It is read-only and side-effect-free.
func (s *Store) ListSessions(ctx context.Context, q ListSessionsQuery) ([]domain.Session, error) {
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	// Build the parameter set. Nil-able args become NULL so the SQL's NULL-guards
	// disable the corresponding clause.
	var statusArg any
	if len(q.Statuses) > 0 {
		ss := make([]string, len(q.Statuses))
		for i, st := range q.Statuses {
			ss[i] = string(st)
		}
		statusArg = ss
	}
	var afterArg, beforeArg any
	if !q.CreatedAfter.IsZero() {
		afterArg = q.CreatedAfter
	}
	if !q.CreatedBefore.IsZero() {
		beforeArg = q.CreatedBefore
	}
	var cursorTimeArg, cursorIDArg any
	if q.Cursor.ID != "" {
		cursorTimeArg = time.UnixMilli(q.Cursor.CreatedAtMs).UTC()
		cursorIDArg = q.Cursor.ID
	}

	query := selectSessionsListSQL
	if q.Descending {
		query = selectSessionsListDescSQL
	}

	rows, err := tx.Query(ctx, query, statusArg, afterArg, beforeArg, cursorTimeArg, cursorIDArg, q.Limit)
	if err != nil {
		return nil, fmt.Errorf("eventstore: list sessions query: %w", err)
	}
	sessions, err := scanSessionRows(rows)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("eventstore: commit list-sessions: %w", err)
	}
	return sessions, nil
}

// scanSessionRows reads [listSessionColumns]-ordered rows into [domain.Session]s,
// mirroring loadSessionTx's nullable handling so the projection matches a loaded
// session exactly.
func scanSessionRows(rows pgx.Rows) ([]domain.Session, error) {
	var out []domain.Session
	for rows.Next() {
		var (
			sess          domain.Session
			parentID      *string
			forkedFromSeq *int64
			lastEventAt   *time.Time
			modeStr       string
		)
		if err := rows.Scan(
			&sess.ID, &sess.TenantID, &parentID, &forkedFromSeq, &sess.Status, &sess.HeadSeq,
			&lastEventAt, &sess.CreatedAt, &sess.UpdatedAt, &modeStr,
		); err != nil {
			return nil, fmt.Errorf("eventstore: scanning session row: %w", err)
		}
		sess.Mode = domain.PermissionMode(modeStr)
		if parentID != nil {
			sess.ParentID = *parentID
		}
		if forkedFromSeq != nil {
			sess.ForkedFromSeq = *forkedFromSeq
		}
		if lastEventAt != nil {
			sess.LastEventAt = *lastEventAt
		}
		out = append(out, sess)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventstore: iterating session rows: %w", err)
	}
	return out, nil
}
