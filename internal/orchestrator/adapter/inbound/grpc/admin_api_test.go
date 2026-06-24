package grpc

// TDD (red) tests for Feature I — Admin/Tenant session-management API.
//
// Authored BEFORE the implementation; EXPECTED to fail to compile / fail at run
// time until the feature lands. They pin the resolved decisions in
// C:/Users/123/Documents/harness-wave2-build/admin-api/DECISIONS.md (two dated
// 2026-06-24 sections) and SPEC.md (AC-1…AC-15):
//
//   - Carrier: two ADDITIVE gRPC RPCs on OrchestratorService — ListSessions +
//     GetSessionUsage — reusing the SAME authorizeTenant / authorizeSession
//     ownership path (zero new auth). GET ONE reuses the existing GetSession;
//     STOP reuses the existing Control{InterruptAction} (no new rpc, no new kill;
//     STOP idempotency is regression-pinned in server_test.go's Control tests).
//     REST + MCP facades are thin shells over these same *Server methods (covered
//     in their packages).
//   - ListSessions: filter by repeated SessionStatus + half-open
//     [created_after_ms, created_before_ms) window; keyset pagination on the
//     COMPOSITE (created_at, id) exposed as an OPAQUE page_token; page_size
//     default 50 / hard cap 200 (clamped, not rejected); descending flag preserved
//     across pages. Returns repeated SessionSummary (control/lineage projection
//     ONLY — no per-row usage/cost, to avoid an N+1 fold).
//   - GetSessionUsage: v1 sources usage from the existing foldTotals(ctx,
//     sessionID) — the SAME fold GetSession uses — and tags the result
//     source = USAGE_SOURCE_EVENT_FOLD (O's cost-rollup is unmerged on this
//     branch; COST_ROLLUP is a reserved future value). A session interrupted via
//     STOP carries a TurnAborted whose partial usage/cost is included (accounted,
//     never re-billed).
//   - All timestamps/time filters are int64 Unix epoch milliseconds (proto/ uses
//     no well-known types).
//
// These reference symbols that do NOT yet exist (genproto.ListSessionsRequest/
// Response, genproto.SessionSummary, genproto.GetSessionUsageRequest/Response,
// genproto.UsageSource, Server.ListSessions, Server.GetSessionUsage, and the new
// read-only EventStore method ListSessions + its ListSessionsQuery). Their
// absence is the red proof of test-first authoring. The fake's new ListSessions
// method below applies the status/time filter + (created_at, id) keyset over its
// in-memory session map, exactly mirroring what *eventstore.Store.ListSessions
// returns over the sessions table — so the server tests exercise the real
// page-size clamp, opaque page_token codec, summary mapping, and auth without
// Postgres. When the EventStore interface grows ListSessions, the existing
// `_ EventStore = (*tailingEventLog)(nil)` assertion in server_test.go keeps the
// fake honest.

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ---------------------------------------------------------------------------
// Test fake extensions — the read-side ListSessions method.
//
// ListSessions is the NEW read-only method on the EventStore consumer-superset
// (NOT on the frozen app.EventLogPort). The fake holds a small per-session
// control/lineage projection (status, created_at, lineage, head_seq) keyed by id
// and applies the SAME query the production store runs: status OR-filter +
// half-open created_at window + (created_at, id) keyset + Limit + Descending —
// so the server's page_size clamp, opaque page_token codec, and SessionSummary
// mapping are what is under test, not the fake.
// ---------------------------------------------------------------------------

// adminSession is the fake's stored control/lineage projection for one session
// (the subset ListSessions surfaces). It carries the timestamps the keyset orders
// by; the fake fabricates CreatedAt deterministically from the seed.
type adminSession struct {
	id            string
	tenant        string
	status        domain.SessionStatus
	mode          domain.PermissionMode
	headSeq       int64
	parentID      string
	forkedFromSeq int64
	createdAt     time.Time
	updatedAt     time.Time
	lastEventAt   time.Time
}

// seedAdminSession records a session in the fake's control/lineage map (for
// ListSessions) AND in the ownership/head maps (so authorizeSession/foldTotals
// see it too). createdAtMs is the session's created_at in Unix epoch ms.
func seedAdminSession(l *tailingEventLog, id, tenant string, st domain.SessionStatus, mode domain.PermissionMode, createdAtMs int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.adminSessions == nil {
		l.adminSessions = map[string]adminSession{}
	}
	created := time.UnixMilli(createdAtMs).UTC()
	l.adminSessions[id] = adminSession{
		id: id, tenant: tenant, status: st, mode: mode, headSeq: 1,
		createdAt: created, updatedAt: created,
	}
	// Mirror the ownership/control maps the rest of the fake reads from so the
	// session is also visible to authorizeSession / LoadSession / foldTotals.
	l.tenants[id] = tenant
	l.modes[id] = mode
	if _, ok := l.heads[id]; !ok {
		l.heads[id] = 1
	}
	if _, ok := l.events[id]; !ok {
		l.events[id] = []domain.EventEnvelope{{
			Type: domain.EventSessionStarted, Seq: 1, SessionID: id,
			TenantID: tenant, Actor: domain.ActorSystem, Event: domain.SessionStarted{},
		}}
	}
}

// ListSessions returns the tenant's session control/lineage projections matching
// the query, ordered by (created_at, id) — ascending or descending — after the
// keyset cursor, capped at Limit. It mirrors the production store's keyset query.
// The fake is single-tenant per test (RLS does the tenant-scoping in production),
// so it returns every stored session that passes the status/time/keyset filters.
func (l *tailingEventLog) ListSessions(_ context.Context, q ListSessionsQuery) ([]domain.Session, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	statusSet := map[domain.SessionStatus]bool{}
	for _, s := range q.Statuses {
		statusSet[s] = true
	}

	var rows []adminSession
	for _, s := range l.adminSessions {
		if len(statusSet) > 0 && !statusSet[s.status] {
			continue
		}
		if !q.CreatedAfter.IsZero() && s.createdAt.Before(q.CreatedAfter) {
			continue
		}
		if !q.CreatedBefore.IsZero() && !s.createdAt.Before(q.CreatedBefore) {
			continue // half-open [after, before): before is exclusive
		}
		rows = append(rows, s)
	}

	less := func(a, b adminSession) bool {
		if !a.createdAt.Equal(b.createdAt) {
			return a.createdAt.Before(b.createdAt)
		}
		return a.id < b.id // PK tie-break: total order on (created_at, id)
	}
	sort.Slice(rows, func(i, j int) bool {
		if q.Descending {
			return less(rows[j], rows[i])
		}
		return less(rows[i], rows[j])
	})

	// Apply the keyset cursor: keep rows strictly after the cursor in sort order.
	// A zero-value cursor (empty ID) is the first page (SPEC §2.7).
	if q.Cursor.ID != "" {
		curRow := adminSession{id: q.Cursor.ID, createdAt: time.UnixMilli(q.Cursor.CreatedAtMs).UTC()}
		filtered := rows[:0:0]
		for _, s := range rows {
			after := less(curRow, s)
			if q.Descending {
				after = less(s, curRow)
			}
			if after {
				filtered = append(filtered, s)
			}
		}
		rows = filtered
	}

	if q.Limit > 0 && len(rows) > q.Limit {
		rows = rows[:q.Limit]
	}

	out := make([]domain.Session, 0, len(rows))
	for _, s := range rows {
		out = append(out, domain.Session{
			ID: s.id, TenantID: s.tenant, Status: s.status, Mode: s.mode,
			HeadSeq: s.headSeq, ParentID: s.parentID, ForkedFromSeq: s.forkedFromSeq,
			CreatedAt: s.createdAt, UpdatedAt: s.updatedAt, LastEventAt: s.lastEventAt,
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// ListSessions — carrier, status filter, time window, keyset pagination.
// ---------------------------------------------------------------------------

// TestListSessions_NoFilterReturnsTenantSessions (AC-1): with several sessions
// seeded for the caller's tenant, ListSessions{} (no filters) returns one
// SessionSummary per caller-tenant session, each carrying the control/lineage
// projection (id, status, mode, head_seq, timestamps). No usage/cost field is on
// a summary (it has none to populate).
func TestListSessions_NoFilterReturnsTenantSessions(t *testing.T) {
	log := newTailingEventLog()
	seedAdminSession(log, "s-1", "tenant-A", domain.StatusActive, domain.ModeDefault, 1_000)
	seedAdminSession(log, "s-2", "tenant-A", domain.StatusFinished, domain.ModePlan, 2_000)
	seedAdminSession(log, "s-3", "tenant-A", domain.StatusFailed, domain.ModeAcceptEdits, 3_000)

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-A", PageSize: 100,
	})
	require.NoError(t, err)

	got := resp.GetSessions()
	require.Len(t, got, 3, "all three caller-tenant sessions are listed")

	byID := map[string]*genproto.SessionSummary{}
	for _, s := range got {
		assert.Equal(t, "tenant-A", s.GetTenantId(), "every summary carries its tenant")
		assert.Greater(t, s.GetCreatedAtMs(), int64(0), "every summary carries created_at_ms")
		byID[s.GetSessionId()] = s
	}
	require.Contains(t, byID, "s-2")
	assert.Equal(t, genproto.SessionStatus_SESSION_STATUS_FINISHED, byID["s-2"].GetStatus(), "status projected")
	assert.Equal(t, genproto.PermissionMode_PERMISSION_MODE_PLAN, byID["s-2"].GetMode(), "mode projected")
	assert.Equal(t, int64(2_000), byID["s-2"].GetCreatedAtMs(), "created_at_ms is Unix epoch ms")
}

// TestListSessions_StatusFilter (AC-2): status=[ACTIVE] returns only active;
// status=[ACTIVE, FAILED] returns the union; empty status returns all.
func TestListSessions_StatusFilter(t *testing.T) {
	log := newTailingEventLog()
	seedAdminSession(log, "a1", "tenant-A", domain.StatusActive, domain.ModeDefault, 1_000)
	seedAdminSession(log, "a2", "tenant-A", domain.StatusActive, domain.ModeDefault, 2_000)
	seedAdminSession(log, "f1", "tenant-A", domain.StatusFinished, domain.ModeDefault, 3_000)
	seedAdminSession(log, "x1", "tenant-A", domain.StatusFailed, domain.ModeDefault, 4_000)

	h := devHarness(t, "tenant-A", noopRunner(log), log)

	onlyActive, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-A", PageSize: 100,
		Status: []genproto.SessionStatus{genproto.SessionStatus_SESSION_STATUS_ACTIVE},
	})
	require.NoError(t, err)
	require.Len(t, onlyActive.GetSessions(), 2, "status=[ACTIVE] returns exactly the active sessions")
	for _, s := range onlyActive.GetSessions() {
		assert.Equal(t, genproto.SessionStatus_SESSION_STATUS_ACTIVE, s.GetStatus())
	}

	union, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-A", PageSize: 100,
		Status: []genproto.SessionStatus{
			genproto.SessionStatus_SESSION_STATUS_ACTIVE,
			genproto.SessionStatus_SESSION_STATUS_FAILED,
		},
	})
	require.NoError(t, err)
	require.Len(t, union.GetSessions(), 3, "status=[ACTIVE, FAILED] returns the union (2 active + 1 failed)")

	all, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-A", PageSize: 100,
	})
	require.NoError(t, err)
	assert.Len(t, all.GetSessions(), 4, "empty status returns all statuses")
}

// TestListSessions_TimeWindowHalfOpen (AC-3): created_after_ms/created_before_ms
// apply a half-open [after, before) window; a 0 bound is disabled; rows outside
// are absent.
func TestListSessions_TimeWindowHalfOpen(t *testing.T) {
	log := newTailingEventLog()
	seedAdminSession(log, "t1000", "tenant-A", domain.StatusActive, domain.ModeDefault, 1_000)
	seedAdminSession(log, "t2000", "tenant-A", domain.StatusActive, domain.ModeDefault, 2_000)
	seedAdminSession(log, "t3000", "tenant-A", domain.StatusActive, domain.ModeDefault, 3_000)
	seedAdminSession(log, "t4000", "tenant-A", domain.StatusActive, domain.ModeDefault, 4_000)

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-A", PageSize: 100,
		CreatedAfterMs:  2_000, // inclusive lower bound
		CreatedBeforeMs: 4_000, // exclusive upper bound
	})
	require.NoError(t, err)

	ids := map[string]bool{}
	for _, s := range resp.GetSessions() {
		ids[s.GetSessionId()] = true
	}
	assert.False(t, ids["t1000"], "before the lower bound is excluded")
	assert.True(t, ids["t2000"], "the lower bound is inclusive")
	assert.True(t, ids["t3000"], "inside the window is included")
	assert.False(t, ids["t4000"], "the upper bound is exclusive (half-open)")
	assert.Len(t, resp.GetSessions(), 2, "exactly the [2000, 4000) window")
}

// TestListSessions_KeysetPaginationNoOverlapNoGap (AC-4): page_size=N returns
// exactly N + a non-empty next_page_token; the next page continues with no
// overlap and no gap; exhausting the list yields an empty next_page_token.
func TestListSessions_KeysetPaginationNoOverlapNoGap(t *testing.T) {
	log := newTailingEventLog()
	for i := 1; i <= 5; i++ {
		seedAdminSession(log, idAt(i), "tenant-A", domain.StatusActive, domain.ModeDefault, int64(i*1000))
	}

	h := devHarness(t, "tenant-A", noopRunner(log), log)

	page1, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-A", PageSize: 2,
	})
	require.NoError(t, err)
	require.Len(t, page1.GetSessions(), 2, "page_size=2 returns exactly 2")
	require.NotEmpty(t, page1.GetNextPageToken(), "more rows remain, so a next_page_token is returned")

	seen := map[string]int{}
	for _, s := range page1.GetSessions() {
		seen[s.GetSessionId()]++
	}

	page2, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-A", PageSize: 2, PageToken: page1.GetNextPageToken(),
	})
	require.NoError(t, err)
	require.Len(t, page2.GetSessions(), 2, "the second page is full")
	for _, s := range page2.GetSessions() {
		assert.Zero(t, seen[s.GetSessionId()], "page 2 has no overlap with page 1 (keyset)")
		seen[s.GetSessionId()]++
	}

	page3, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-A", PageSize: 2, PageToken: page2.GetNextPageToken(),
	})
	require.NoError(t, err)
	require.Len(t, page3.GetSessions(), 1, "the final (short) page holds the last row")
	for _, s := range page3.GetSessions() {
		assert.Zero(t, seen[s.GetSessionId()], "page 3 has no overlap (keyset)")
		seen[s.GetSessionId()]++
	}
	assert.Empty(t, page3.GetNextPageToken(), "the last page returns an empty next_page_token")
	assert.Len(t, seen, 5, "every row appears exactly once across the three pages (no drop, no dup)")
}

// TestListSessions_SameMillisecondTieBreak (AC-5): two sessions with the SAME
// created_at millisecond both appear across paging with no drop and no duplicate
// — the composite (created_at, id) tie-break.
func TestListSessions_SameMillisecondTieBreak(t *testing.T) {
	log := newTailingEventLog()
	// Three sessions all at the exact same created_at ms — the keyset MUST use the
	// id tie-break or a single-key (created_at) cursor would drop/duplicate at the
	// boundary.
	seedAdminSession(log, "tie-a", "tenant-A", domain.StatusActive, domain.ModeDefault, 5_000)
	seedAdminSession(log, "tie-b", "tenant-A", domain.StatusActive, domain.ModeDefault, 5_000)
	seedAdminSession(log, "tie-c", "tenant-A", domain.StatusActive, domain.ModeDefault, 5_000)

	h := devHarness(t, "tenant-A", noopRunner(log), log)

	seen := map[string]int{}
	token := ""
	for pages := 0; pages < 10; pages++ {
		resp, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
			TenantId: "tenant-A", PageSize: 1, PageToken: token,
		})
		require.NoError(t, err)
		for _, s := range resp.GetSessions() {
			seen[s.GetSessionId()]++
		}
		token = resp.GetNextPageToken()
		if token == "" {
			break
		}
	}
	require.Len(t, seen, 3, "all three same-ms sessions appear (id tie-break, no drop)")
	for id, n := range seen {
		assert.Equal(t, 1, n, "%s appears exactly once (no duplicate at the ms boundary)", id)
	}
}

// TestListSessions_DescendingPreservedAcrossPages (AC-6): descending=true returns
// newest-first, and the direction is preserved across pages (the returned
// page_token cannot reverse direction mid-walk).
func TestListSessions_DescendingPreservedAcrossPages(t *testing.T) {
	log := newTailingEventLog()
	for i := 1; i <= 4; i++ {
		seedAdminSession(log, idAt(i), "tenant-A", domain.StatusActive, domain.ModeDefault, int64(i*1000))
	}

	h := devHarness(t, "tenant-A", noopRunner(log), log)

	page1, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-A", PageSize: 2, Descending: true,
	})
	require.NoError(t, err)
	require.Len(t, page1.GetSessions(), 2)
	assert.Equal(t, int64(4_000), page1.GetSessions()[0].GetCreatedAtMs(), "descending: newest first")
	assert.Equal(t, int64(3_000), page1.GetSessions()[1].GetCreatedAtMs())

	page2, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-A", PageSize: 2, PageToken: page1.GetNextPageToken(),
	})
	require.NoError(t, err)
	require.Len(t, page2.GetSessions(), 2)
	// Direction is carried in the token, so page 2 continues DESCENDING even though
	// the request did not re-set descending.
	assert.Equal(t, int64(2_000), page2.GetSessions()[0].GetCreatedAtMs(), "descending preserved across pages via token")
	assert.Equal(t, int64(1_000), page2.GetSessions()[1].GetCreatedAtMs())
}

// TestListSessions_PageSizeDefaultAndHardCap (AC-7): page_size<=0 resolves to the
// default (50); page_size above 200 is clamped (returns <=200, not an error).
func TestListSessions_PageSizeDefaultAndHardCap(t *testing.T) {
	log := newTailingEventLog()
	// Seed 60 sessions so the default-50 boundary is observable.
	for i := 1; i <= 60; i++ {
		seedAdminSession(log, idAt(i), "tenant-A", domain.StatusActive, domain.ModeDefault, int64(i*1000))
	}

	h := devHarness(t, "tenant-A", noopRunner(log), log)

	// page_size<=0 -> default 50.
	def, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-A", PageSize: 0,
	})
	require.NoError(t, err)
	assert.Len(t, def.GetSessions(), 50, "page_size<=0 resolves to the default of 50")
	assert.NotEmpty(t, def.GetNextPageToken(), "60 > 50 -> more pages remain")

	// page_size above the hard cap of 200 is clamped, NOT rejected.
	capped, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-A", PageSize: 1_000_000,
	})
	require.NoError(t, err, "an over-cap page_size is clamped, not rejected")
	assert.LessOrEqual(t, len(capped.GetSessions()), 200, "no page exceeds the hard cap of 200")
}

// TestListSessions_MalformedPageToken (defensive decode): a garbage page_token is
// rejected with InvalidArgument rather than silently treated as the first page.
func TestListSessions_MalformedPageToken(t *testing.T) {
	log := newTailingEventLog()
	seedAdminSession(log, "s-1", "tenant-A", domain.StatusActive, domain.ModeDefault, 1_000)

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	_, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-A", PageToken: "!!!not-a-valid-token!!!",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err),
		"a malformed page_token is an InvalidArgument, not a silent first page")
}

// ---------------------------------------------------------------------------
// ListSessions — security / ownership.
// ---------------------------------------------------------------------------

// TestListSessions_RejectsUnauthenticated (AC-13): an unauthenticated list is
// rejected in production-auth mode (no silent open management read edge).
func TestListSessions_RejectsUnauthenticated(t *testing.T) {
	log := newTailingEventLog()
	seedAdminSession(log, "s-1", "tenant-A", domain.StatusActive, domain.ModeDefault, 1_000)
	gate := newNotifyingGate()
	conn := startServer(t, prodAuthConfig(), log, gate, noopRunner(log))
	client := genproto.NewOrchestratorServiceClient(conn)

	_, err := client.ListSessions(context.Background(), &genproto.ListSessionsRequest{TenantId: "tenant-A"})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err),
		"an unauthenticated ListSessions is rejected (shared edge auth)")
}

// TestListSessions_RejectsMismatchedRequestTenant (AC-14): a non-empty
// request.tenant_id that does NOT match the authenticated principal is rejected
// (the request tenant is a guard only, never the filter key).
func TestListSessions_RejectsMismatchedRequestTenant(t *testing.T) {
	log := newTailingEventLog()
	seedAdminSession(log, "s-1", "tenant-A", domain.StatusActive, domain.ModeDefault, 1_000)

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	_, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{
		TenantId: "tenant-EVIL", // != principal tenant-A
	})
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err),
		"a mismatched request.tenant_id is rejected (guard-only, never a filter key)")
}

// TestListSessions_IgnoresRequestTenantAsFilter (AC-14 corollary): a MATCHING
// request.tenant_id is only a guard — the authority is the principal — so a
// matching tenant_id yields the identical result to omitting it entirely.
func TestListSessions_IgnoresRequestTenantAsFilter(t *testing.T) {
	log := newTailingEventLog()
	seedAdminSession(log, "s-1", "tenant-A", domain.StatusActive, domain.ModeDefault, 1_000)
	seedAdminSession(log, "s-2", "tenant-A", domain.StatusActive, domain.ModeDefault, 2_000)

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	withTenant, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{TenantId: "tenant-A", PageSize: 100})
	require.NoError(t, err)
	omitted, err := h.client.ListSessions(context.Background(), &genproto.ListSessionsRequest{PageSize: 100})
	require.NoError(t, err)
	assert.Equal(t, len(withTenant.GetSessions()), len(omitted.GetSessions()),
		"a matching request.tenant_id is a guard only; the principal is the authority")
}

// ---------------------------------------------------------------------------
// GetSessionUsage — per-session usage via foldTotals (source = EVENT_FOLD).
// ---------------------------------------------------------------------------

// TestGetSessionUsage_FoldsTurnFinished (AC-8): for a session with one
// TurnFinished{usage, cost, num_turns}, GetSessionUsage returns those folded
// totals and source == USAGE_SOURCE_EVENT_FOLD.
func TestGetSessionUsage_FoldsTurnFinished(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-u", "tenant-A")
	appendEvent(t, log, "sess-u", domain.ActorSystem, domain.TurnFinished{
		TurnID: "t1", Reason: domain.Success,
		Usage: llm.Usage{InputTokens: 100, OutputTokens: 20}, CostUSD: 0.10, NumTurns: 1,
	})

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.GetSessionUsage(context.Background(), &genproto.GetSessionUsageRequest{
		TenantId: "tenant-A", SessionId: "sess-u",
	})
	require.NoError(t, err)

	assert.Equal(t, "sess-u", resp.GetSessionId(), "response echoes the session id")
	assert.InDelta(t, 0.10, resp.GetCostUsd(), 1e-9, "cost folded from TurnFinished")
	assert.Equal(t, int64(1), resp.GetNumTurns(), "num_turns folded from TurnFinished")
	assert.Equal(t, int64(100), resp.GetUsage().GetInputTokens(), "input tokens folded")
	assert.Equal(t, int64(20), resp.GetUsage().GetOutputTokens(), "output tokens folded")
	assert.Equal(t, genproto.UsageSource_USAGE_SOURCE_EVENT_FOLD, resp.GetSource(),
		"v1 usage source is the server-side event fold (O's cost-rollup is unmerged)")
}

// TestGetSessionUsage_IncludesInterruptedPartial (AC-9): a session interrupted
// via STOP carries a TurnAborted{UsageSoFar, CostUSD}; the returned usage/cost
// INCLUDES the aborted partial (TurnFinished + TurnAborted), proving STOP's
// partial usage is accounted and not re-billed. source = EVENT_FOLD.
func TestGetSessionUsage_IncludesInterruptedPartial(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-ab", "tenant-A")
	appendEvent(t, log, "sess-ab", domain.ActorSystem, domain.TurnFinished{
		TurnID: "t1", Reason: domain.Success,
		Usage: llm.Usage{InputTokens: 100, OutputTokens: 20}, CostUSD: 0.10, NumTurns: 1,
	})
	// The interrupt (STOP) path appends a TurnAborted with the partial usage.
	appendEvent(t, log, "sess-ab", domain.ActorSystem, domain.TurnAborted{
		TurnID: "t2", Reason: domain.ErrorDuringExecution,
		UsageSoFar: llm.Usage{InputTokens: 30, OutputTokens: 5}, CostUSD: 0.03,
	})

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.GetSessionUsage(context.Background(), &genproto.GetSessionUsageRequest{
		TenantId: "tenant-A", SessionId: "sess-ab",
	})
	require.NoError(t, err)

	assert.InDelta(t, 0.13, resp.GetCostUsd(), 1e-9, "cost includes the aborted partial (0.10 + 0.03)")
	assert.Equal(t, int64(130), resp.GetUsage().GetInputTokens(), "input tokens include the aborted partial")
	assert.Equal(t, int64(25), resp.GetUsage().GetOutputTokens(), "output tokens include the aborted partial")
	assert.Equal(t, genproto.UsageSource_USAGE_SOURCE_EVENT_FOLD, resp.GetSource())
}

// TestGetSessionUsage_EqualsGetSession (AC-10): the folded usage/cost from
// GetSessionUsage EQUALS the usage/cost in GetSession for the same session (same
// underlying foldTotals).
func TestGetSessionUsage_EqualsGetSession(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-eq", "tenant-A")
	appendEvent(t, log, "sess-eq", domain.ActorSystem, domain.TurnFinished{
		TurnID: "t1", Reason: domain.Success,
		Usage: llm.Usage{InputTokens: 200, OutputTokens: 40}, CostUSD: 0.25, NumTurns: 1,
	})

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	usage, err := h.client.GetSessionUsage(context.Background(), &genproto.GetSessionUsageRequest{
		TenantId: "tenant-A", SessionId: "sess-eq",
	})
	require.NoError(t, err)
	sess, err := h.client.GetSession(context.Background(), &genproto.GetSessionRequest{
		TenantId: "tenant-A", SessionId: "sess-eq",
	})
	require.NoError(t, err)

	assert.InDelta(t, sess.GetSession().GetTotalCostUsd(), usage.GetCostUsd(), 1e-9,
		"GetSessionUsage cost == GetSession cost (same foldTotals)")
	assert.Equal(t, sess.GetSession().GetNumTurns(), usage.GetNumTurns(),
		"GetSessionUsage turns == GetSession turns")
	assert.Equal(t, sess.GetSession().GetTotalUsage().GetInputTokens(), usage.GetUsage().GetInputTokens(),
		"GetSessionUsage usage == GetSession usage")
}

// TestGetSessionUsage_RejectsForeignTenant (AC-15): a session owned by ANOTHER
// tenant -> PermissionDenied (shared authorizeSession ownership path).
func TestGetSessionUsage_RejectsForeignTenant(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-A", "tenant-A")

	h := devHarness(t, "tenant-B", noopRunner(log), log)
	_, err := h.client.GetSessionUsage(context.Background(), &genproto.GetSessionUsageRequest{
		TenantId: "tenant-B", SessionId: "sess-A",
	})
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err),
		"tenant B cannot read tenant A's session usage (ownership)")
}

// TestGetSessionUsage_NotFound (AC-15): a non-existent / RLS-invisible session ->
// NotFound.
func TestGetSessionUsage_NotFound(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-real", "tenant-A")

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	_, err := h.client.GetSessionUsage(context.Background(), &genproto.GetSessionUsageRequest{
		TenantId: "tenant-A", SessionId: "sess-missing",
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err),
		"a missing/RLS-invisible session is NotFound")
}

// TestGetSessionUsage_RejectsUnauthenticated (AC-13): an unauthenticated usage
// read is rejected in production-auth mode.
func TestGetSessionUsage_RejectsUnauthenticated(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-1", "tenant-A")
	gate := newNotifyingGate()
	conn := startServer(t, prodAuthConfig(), log, gate, noopRunner(log))
	client := genproto.NewOrchestratorServiceClient(conn)

	_, err := client.GetSessionUsage(context.Background(), &genproto.GetSessionUsageRequest{
		TenantId: "tenant-A", SessionId: "sess-1",
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err),
		"an unauthenticated GetSessionUsage is rejected (shared edge auth)")
}

// idAt returns a deterministic, lexicographically-sortable session id for the i-th
// seeded session (zero-padded so id order matches numeric order for the keyset
// tie-break assertions).
func idAt(i int) string {
	const digits = "0123456789"
	return "s-" + string([]byte{digits[(i/100)%10], digits[(i/10)%10], digits[i%10]})
}

// _ keeps the app import referenced even if a future edit drops its only use; the
// fake's seed paths construct app.AppendInput via appendEvent (read_api_test.go).
var _ = app.AppendInput{}
