// SPDX-License-Identifier: Apache-2.0

package grpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// This file implements the admin/tenant session-management read surface
// (Feature I / ADR-0027): the two additive RPCs ListSessions and GetSessionUsage,
// plus the read-only query/cursor types the EventStore consumer-superset exposes.
// Both reuse the SAME ownership path as the rest of the edge (authorizeTenant /
// authorizeSession, zero new auth); STOP is NOT here — it remains the existing
// Control{InterruptAction}.

// ---- read-only query/cursor types (the EventStore superset contract) --------

// ListSessionsQuery is the read-only filter/keyset input to
// [EventStore.ListSessions]. It is owned by this edge (the consumer that defines
// the EventStore interface) and aliased by the eventstore adapter so *Store
// satisfies the interface without a wrapper. It is domain/stdlib-typed (no gen/),
// keeping the store transport-agnostic.
type ListSessionsQuery struct {
	// Statuses is the status OR-filter; empty lists every status.
	Statuses []domain.SessionStatus
	// CreatedAfter keeps created_at >= it (inclusive); the zero time disables it.
	CreatedAfter time.Time
	// CreatedBefore keeps created_at < it (exclusive, half-open); the zero time
	// disables it.
	CreatedBefore time.Time
	// Cursor is the keyset cursor; the zero cursor (empty ID) is the first page.
	Cursor ListCursor
	// Limit caps the rows returned. The server clamps it to the hard cap and passes
	// page_size+1 so the store can report whether a further page exists.
	Limit int
	// Descending lists newest-first when true.
	Descending bool
}

// ListCursor is the keyset position a page_token encodes: the (created_at, id) of
// the last row of the prior page, plus the walk direction. The zero value (empty
// ID) is the first page.
type ListCursor struct {
	// CreatedAtMs is the cursor row's created_at as Unix epoch ms.
	CreatedAtMs int64
	// ID is the cursor row's id (the (created_at, id) tie-break).
	ID string
	// Descending is the walk direction carried in the token so a paging walk cannot
	// reverse mid-walk.
	Descending bool
}

// ---- page-token codec (opaque to clients) -----------------------------------

// pageTokenMagic prefixes the encoded cursor payload so a base64 blob that is not
// one of our tokens is rejected by decode rather than mis-parsed into a zero
// cursor (which would silently leak the first page).
const pageTokenMagic = "blc1:"

// pageTokenWire is the JSON wire shape inside the opaque page_token.
type pageTokenWire struct {
	C int64  `json:"c"` // created_at ms
	I string `json:"i"` // id
	D bool   `json:"d"` // descending
}

// encodePageToken encodes a [ListCursor] as an opaque, URL-safe base64 token.
func encodePageToken(c ListCursor) string {
	b, err := json.Marshal(pageTokenWire{C: c.CreatedAtMs, I: c.ID, D: c.Descending})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(append([]byte(pageTokenMagic), b...))
}

// decodePageToken decodes an opaque page_token into a [ListCursor]. An empty token
// is the first page (the zero cursor), NOT an error. A malformed/garbage token is
// a typed error the caller maps to InvalidArgument.
func decodePageToken(token string) (ListCursor, error) {
	if token == "" {
		return ListCursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return ListCursor{}, fmt.Errorf("malformed page_token (not base64): %w", err)
	}
	if !strings.HasPrefix(string(raw), pageTokenMagic) {
		return ListCursor{}, fmt.Errorf("malformed page_token (bad prefix)")
	}
	var w pageTokenWire
	if err := json.Unmarshal(raw[len(pageTokenMagic):], &w); err != nil {
		return ListCursor{}, fmt.Errorf("malformed page_token (bad payload): %w", err)
	}
	return ListCursor{CreatedAtMs: w.C, ID: w.I, Descending: w.D}, nil
}

// ---- page-size clamping -----------------------------------------------------

const (
	// defaultListPageSize is the page size used when the request asks for <= 0.
	defaultListPageSize = 50
	// maxListPageSize is the hard cap; a larger request is clamped (never rejected)
	// so a list cannot be turned into an unbounded scan (DoS bound).
	maxListPageSize = 200
)

// clampPageSize resolves the requested page_size to the effective limit: <= 0 ->
// default, > cap -> cap, else the request.
func clampPageSize(req int32) int {
	switch {
	case req <= 0:
		return defaultListPageSize
	case req > maxListPageSize:
		return maxListPageSize
	default:
		return int(req)
	}
}

// ---- ListSessions -----------------------------------------------------------

// ListSessions lists the caller-tenant's sessions (control/lineage projection
// only) with an optional status OR-filter and a half-open created_at window,
// keyset-paginated on (created_at, id) via an opaque page_token (ADR-0027).
//
// Authorization is authorizeTenant ONLY (a tenant-range guard): the request
// tenant_id must match the principal when non-empty, but it is never a filter key
// — the actual row set is RLS-scoped to the principal's tenant at the store. There
// is NO per-session authorizeSession (that is GetSessionUsage's concern) and NO
// per-row fold (avoids an N+1). A malformed page_token is InvalidArgument.
func (s *Server) ListSessions(ctx context.Context, req *genproto.ListSessionsRequest) (*genproto.ListSessionsResponse, error) {
	tenant, err := s.authorizeTenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}

	cursor, err := decodePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "orchestrator: %v", err)
	}
	// Direction is carried in the token, so a continuation cannot reverse mid-walk:
	// when a cursor is present its Descending governs; only the first page reads the
	// request's descending flag.
	descending := req.GetDescending()
	if cursor.ID != "" {
		descending = cursor.Descending
	}

	limit := clampPageSize(req.GetPageSize())
	q := ListSessionsQuery{
		Statuses:      fromGenStatuses(req.GetStatus()),
		CreatedAfter:  fromEpochMs(req.GetCreatedAfterMs()),
		CreatedBefore: fromEpochMs(req.GetCreatedBeforeMs()),
		Cursor:        cursor,
		Descending:    descending,
		// Fetch one extra row to decide whether a further page exists (keyset
		// has_more), without an OFFSET or a second COUNT query.
		Limit: limit + 1,
	}

	sessions, err := s.log.ListSessions(ctx, q)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "orchestrator: list sessions: %v", err)
	}

	// If the store returned the sentinel extra row, drop it and mint a next-page
	// token from the LAST row of the page; otherwise this is the final page.
	var nextToken string
	if len(sessions) > limit {
		sessions = sessions[:limit]
		last := sessions[len(sessions)-1]
		nextToken = encodePageToken(ListCursor{
			CreatedAtMs: last.CreatedAt.UnixMilli(),
			ID:          last.ID,
			Descending:  descending,
		})
	}

	out := make([]*genproto.SessionSummary, 0, len(sessions))
	for _, sess := range sessions {
		if sess.TenantID == "" {
			sess.TenantID = tenant
		}
		out = append(out, toGenSessionSummary(sess))
	}
	return &genproto.ListSessionsResponse{Sessions: out, NextPageToken: nextToken}, nil
}

// ---- GetSessionUsage --------------------------------------------------------

// GetSessionUsage returns accumulated per-session usage/cost/turns for an owned
// session (ADR-0027). It reuses the FULL ownership path (authorizeTenant +
// authorizeSession): a foreign-tenant session is PermissionDenied, a missing /
// RLS-invisible one is NotFound. v1 sources the totals from the existing
// foldTotals — the SAME fold GetSession uses — so the values exactly equal
// GetSession's, and stamps source = USAGE_SOURCE_EVENT_FOLD (Feature O's
// cost-rollup is unmerged on this branch; USAGE_SOURCE_COST_ROLLUP is reserved
// enum-space for a future, wire-shape-preserving fallback inside this method).
func (s *Server) GetSessionUsage(ctx context.Context, req *genproto.GetSessionUsageRequest) (*genproto.GetSessionUsageResponse, error) {
	tenant, err := s.authorizeTenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	if _, err := s.authorizeSession(ctx, tenant, req.GetSessionId()); err != nil {
		return nil, err
	}

	usage, cost, turns := s.foldTotals(ctx, req.GetSessionId())
	return &genproto.GetSessionUsageResponse{
		SessionId: req.GetSessionId(),
		TenantId:  tenant,
		Usage:     toGenUsage(usage),
		CostUsd:   cost,
		NumTurns:  turns,
		Source:    genproto.UsageSource_USAGE_SOURCE_EVENT_FOLD,
	}, nil
}

// ---- mappers ----------------------------------------------------------------

// toGenSessionSummary maps a [domain.Session] to the wire [genproto.SessionSummary]
// (control/lineage projection only — no usage/cost). Timestamps are Unix epoch ms;
// a zero LastEventAt maps to 0 (no event yet). This is adapter-layer time mapping
// (forbidigo-exempt: no time.Now, only UnixMilli on stored times).
func toGenSessionSummary(s domain.Session) *genproto.SessionSummary {
	return &genproto.SessionSummary{
		SessionId:       s.ID,
		TenantId:        s.TenantID,
		Status:          toGenStatus(s.Status),
		Mode:            toGenMode(s.Mode),
		HeadSeq:         s.HeadSeq,
		ParentSessionId: s.ParentID,
		ForkedFromSeq:   s.ForkedFromSeq,
		CreatedAtMs:     toEpochMs(s.CreatedAt),
		UpdatedAtMs:     toEpochMs(s.UpdatedAt),
		LastEventAtMs:   toEpochMs(s.LastEventAt),
	}
}

// fromGenStatuses maps the wire SessionStatus OR-filter to domain statuses,
// dropping UNSPECIFIED entries (an UNSPECIFIED in the set is meaningless as a
// filter and is ignored rather than matching nothing).
func fromGenStatuses(in []genproto.SessionStatus) []domain.SessionStatus {
	if len(in) == 0 {
		return nil
	}
	out := make([]domain.SessionStatus, 0, len(in))
	for _, st := range in {
		switch st {
		case genproto.SessionStatus_SESSION_STATUS_ACTIVE:
			out = append(out, domain.StatusActive)
		case genproto.SessionStatus_SESSION_STATUS_FINISHED:
			out = append(out, domain.StatusFinished)
		case genproto.SessionStatus_SESSION_STATUS_FAILED:
			out = append(out, domain.StatusFailed)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// fromEpochMs maps a Unix epoch-ms filter bound to a time.Time; 0 maps to the zero
// time (the store reads the zero time as "no bound").
func fromEpochMs(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// toEpochMs maps a time.Time to Unix epoch ms; the zero time maps to 0.
func toEpochMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}
