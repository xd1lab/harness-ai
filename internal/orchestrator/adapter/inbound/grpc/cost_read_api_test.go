package grpc

// TDD (red) tests for Feature O — Session/Tenant cost read API.
//
// Authored BEFORE the implementation; EXPECTED to fail to compile / fail at run
// time until the feature lands. They pin the resolved decisions in
// C:/Users/123/Documents/harness-wave2-build/cost-read/DECISIONS.md (two v1
// entries, 2026-06-24) and SPEC.md (AC-1…AC-14):
//
//   - Carrier: two ADDITIVE gRPC RPCs on OrchestratorService —
//     GetSessionCost + GetTenantCost — reusing the SAME authorizeTenant /
//     authorizeSession ownership path (zero new auth). REST + MCP facades are
//     thin shells over these same *Server methods (covered in their packages).
//   - Per-session response: a CostTotals total plus repeated ModelCost by_model,
//     sorted by cost_usd descending. Σ(by_model.cost) == total.cost (a partition).
//   - Per-tenant response: a CostTotals total + by_model + session_count
//     (distinct sessions with cost rows). Tenant is the AUTHENTICATED principal;
//     request.tenant_id is a guard only (must match when non-empty), never a
//     filter key.
//   - Per-model is produced at the WRITE side (projectord correlates
//     TurnStarted.Model ⋈ TurnFinished/TurnAborted by TurnID); the read side only
//     SUM…GROUP BY model. A terminal event whose model could not be correlated
//     lands in the "unknown" bucket (server maps ""→"unknown"), never dropped.
//     Per-tool breakdown is DROPPED (events carry no tool cost).
//
// These reference symbols that do NOT yet exist (genproto.GetSessionCost*,
// genproto.GetTenantCost*, genproto.ModelCost, genproto.CostTotals,
// Server.GetSessionCost, Server.GetTenantCost, ModelCostRow, and the two new
// read-only EventStore methods SessionCostByModel/TenantCostByModel). Their
// absence is the red proof of test-first authoring. The fake's new methods below
// fold TurnStarted.Model ⋈ terminal-by-TurnID over its in-memory events, mirroring
// exactly what the projector writes into session_cost_events — so the server
// tests exercise the real mapping/aggregation/sort without Postgres. When the
// EventStore interface grows the two methods, the existing
// `_ EventStore = (*tailingEventLog)(nil)` assertion in server_test.go keeps the
// fake honest.

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ---------------------------------------------------------------------------
// Test fake extensions — the read-side cost aggregation methods.
//
// SessionCostByModel/TenantCostByModel are the two NEW read-only methods on the
// EventStore consumer-superset (NOT on the frozen app.EventLogPort). The fake
// computes them over its in-memory event map by correlating TurnStarted.Model to
// the matching TurnFinished/TurnAborted by (session, TurnID) — the SAME
// write-side correlation the projector performs — then SUM…GROUP BY model. This
// keeps the fake's read result identical to what the production read path returns
// over session_cost_events.
// ---------------------------------------------------------------------------

// SessionCostByModel returns the per-model cost/usage/turns rollup for one
// session, scoped to the session's owning tenant (mirrors
// `SELECT model, SUM(cost_usd)::float8, SUM(...), COUNT(*) FROM session_cost_events
// WHERE session_id=$1 GROUP BY model`). The model is the empty string when the
// terminal event could not be correlated to a TurnStarted (the server maps it to
// the "unknown" bucket).
func (l *tailingEventLog) SessionCostByModel(_ context.Context, sessionID string) ([]ModelCostRow, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return foldModelCost(l.events[sessionID]), nil
}

// TenantCostByModel returns the per-model rollup aggregated across EVERY session
// the fake holds (the fake is single-tenant in these tests; RLS does the
// tenant-scoping in production). It mirrors the tenant read with no session
// filter (the SUM…GROUP BY model over all of the principal tenant's rows).
//
// It is 2-return — (rows, error) — matching DECISIONS §E (the eventstore
// SessionCostByModel/TenantCostByModel pair) and the production
// *eventstore.Store method this fake stands in for. The per-tenant
// session_count is NOT carried here (a per-model GROUP BY row set cannot also
// be a scalar distinct-session count); it is surfaced by the SEPARATE
// TenantSessionCostCount method below. Keeping these two signatures consistent
// across the grpc fake and *eventstore.Store is what makes the single
// EventStore interface satisfiable by both (a Go method has exactly one
// signature).
func (l *tailingEventLog) TenantCostByModel(_ context.Context) ([]ModelCostRow, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var all []domain.EventEnvelope
	for _, evs := range l.events {
		all = append(all, evs...)
	}
	return foldModelCost(all), nil
}

// TenantSessionCostCount returns the number of DISTINCT sessions of the
// principal tenant that carry at least one cost-bearing terminal event — the
// source of GetTenantCostResponse.session_count. It is a SEPARATE read method
// (mirroring the eventstore selectTenantSessionCountSQL =
// COUNT(DISTINCT session_id)) precisely so TenantCostByModel can stay the
// 2-return (rows, error) shape the eventstore integration test also calls; the
// server's GetTenantCost reads the count from here. The fake is single-tenant
// in these tests; RLS does the tenant-scoping in production.
func (l *tailingEventLog) TenantSessionCostCount(_ context.Context) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var count int64
	for _, evs := range l.events {
		for _, env := range evs {
			switch env.Event.(type) {
			case domain.TurnFinished, domain.TurnAborted:
				count++
			default:
				continue
			}
			break // this session has cost; count it once
		}
	}
	return count, nil
}

// foldModelCost correlates TurnStarted.Model to terminal events by (session,
// TurnID) and folds the per-model cost/usage/turns. A terminal event with no
// matching TurnStarted contributes to the "" (unknown) bucket.
func foldModelCost(events []domain.EventEnvelope) []ModelCostRow {
	type turnKey struct{ session, turn string }
	modelByTurn := map[turnKey]string{}
	for _, env := range events {
		if ts, ok := env.Event.(domain.TurnStarted); ok {
			modelByTurn[turnKey{env.SessionID, ts.TurnID}] = ts.Model
		}
	}
	byModel := map[string]*ModelCostRow{}
	add := func(model string, cost float64, usage llm.Usage) {
		r := byModel[model]
		if r == nil {
			r = &ModelCostRow{Model: model}
			byModel[model] = r
		}
		r.CostUSD += cost
		r.Usage = addUsage(r.Usage, usage)
		r.Turns++
	}
	for _, env := range events {
		switch ev := env.Event.(type) {
		case domain.TurnFinished:
			add(modelByTurn[turnKey{env.SessionID, ev.TurnID}], ev.CostUSD, ev.Usage)
		case domain.TurnAborted:
			add(modelByTurn[turnKey{env.SessionID, ev.TurnID}], ev.CostUSD, ev.UsageSoFar)
		}
	}
	out := make([]ModelCostRow, 0, len(byModel))
	for _, r := range byModel {
		out = append(out, *r)
	}
	// Stable order so the server (which sorts by cost desc) has deterministic input.
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}

// seedCostStream creates a session owned by tenant and appends a representative
// cost-bearing spread: two turns on model "m1" and one turn on model "m2", plus a
// finished turn whose TurnStarted is ABSENT (so its model cannot be correlated —
// the "unknown" bucket). It returns the per-model expected totals for assertions.
func seedCostStream(t *testing.T, log *tailingEventLog, sessionID, tenant string) {
	t.Helper()
	log.seed(sessionID, tenant) // SessionStarted at seq 1

	// Turn 1 on m1: started + finished.
	appendEvent(t, log, sessionID, domain.ActorSystem, domain.TurnStarted{TurnID: "t1", Model: "m1"})
	appendEvent(t, log, sessionID, domain.ActorSystem, domain.TurnFinished{
		TurnID: "t1", Reason: domain.Success, Usage: llm.Usage{InputTokens: 100, OutputTokens: 20}, CostUSD: 0.10, NumTurns: 1,
	})
	// Turn 2 on m1: started + finished.
	appendEvent(t, log, sessionID, domain.ActorSystem, domain.TurnStarted{TurnID: "t2", Model: "m1"})
	appendEvent(t, log, sessionID, domain.ActorSystem, domain.TurnFinished{
		TurnID: "t2", Reason: domain.Success, Usage: llm.Usage{InputTokens: 200, OutputTokens: 40}, CostUSD: 0.20, NumTurns: 2,
	})
	// Turn 3 on m2: started + aborted (partial turn still billed).
	appendEvent(t, log, sessionID, domain.ActorSystem, domain.TurnStarted{TurnID: "t3", Model: "m2"})
	appendEvent(t, log, sessionID, domain.ActorSystem, domain.TurnAborted{
		TurnID: "t3", Reason: domain.ErrorDuringExecution, UsageSoFar: llm.Usage{InputTokens: 50, OutputTokens: 5}, CostUSD: 0.05,
	})
	// Turn 4: a finished turn whose TurnStarted is ABSENT -> uncorrelated -> "unknown".
	appendEvent(t, log, sessionID, domain.ActorSystem, domain.TurnFinished{
		TurnID: "t4", Reason: domain.Success, Usage: llm.Usage{InputTokens: 10, OutputTokens: 2}, CostUSD: 0.01, NumTurns: 3,
	})
}

// sumByModel reduces a by_model slice to a single total (cost, usage, turns).
func sumByModel(by []*genproto.ModelCost) (cost float64, in, out, turns int64) {
	for _, m := range by {
		cost += m.GetCostUsd()
		in += m.GetUsage().GetInputTokens()
		out += m.GetUsage().GetOutputTokens()
		turns += m.GetTurns()
	}
	return cost, in, out, turns
}

// ---------------------------------------------------------------------------
// GetSessionCost
// ---------------------------------------------------------------------------

// TestGetSessionCost_TotalsEqualEventSum (AC-1): the per-session total equals the
// fold of the seeded stream — 0.10+0.20+0.05+0.01 = 0.36 over 4 cost-bearing
// terminal events, summed usage 360 input / 67 output.
func TestGetSessionCost_TotalsEqualEventSum(t *testing.T) {
	log := newTailingEventLog()
	seedCostStream(t, log, "sess-cost", "tenant-A")

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.GetSessionCost(context.Background(), &genproto.GetSessionCostRequest{
		TenantId: "tenant-A", SessionId: "sess-cost",
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetTotal())

	assert.InDelta(t, 0.36, resp.GetTotal().GetCostUsd(), 1e-9, "session total cost = event sum")
	assert.Equal(t, int64(4), resp.GetTotal().GetTurns(), "4 cost-bearing terminal turns")
	assert.Equal(t, int64(360), resp.GetTotal().GetUsage().GetInputTokens(), "summed input tokens")
	assert.Equal(t, int64(67), resp.GetTotal().GetUsage().GetOutputTokens(), "summed output tokens")
	assert.Equal(t, "sess-cost", resp.GetSessionId(), "response echoes the session id")
}

// TestGetSessionCost_ByModelPartitionsTotal (AC-3): the by_model breakdown is a
// PARTITION of the total — Σ by_model[*].cost == total.cost (and likewise each
// usage field and turns) — and is sorted by cost_usd descending.
func TestGetSessionCost_ByModelPartitionsTotal(t *testing.T) {
	log := newTailingEventLog()
	seedCostStream(t, log, "sess-part", "tenant-A")

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.GetSessionCost(context.Background(), &genproto.GetSessionCostRequest{
		TenantId: "tenant-A", SessionId: "sess-part",
	})
	require.NoError(t, err)

	by := resp.GetByModel()
	require.NotEmpty(t, by, "by_model breakdown is present")

	// Partition: the per-model sums reconstruct the total exactly.
	cost, in, out, turns := sumByModel(by)
	assert.InDelta(t, resp.GetTotal().GetCostUsd(), cost, 1e-9, "Σ by_model cost == total cost")
	assert.Equal(t, resp.GetTotal().GetUsage().GetInputTokens(), in, "Σ by_model input tokens == total")
	assert.Equal(t, resp.GetTotal().GetUsage().GetOutputTokens(), out, "Σ by_model output tokens == total")
	assert.Equal(t, resp.GetTotal().GetTurns(), turns, "Σ by_model turns == total turns")

	// Sorted by cost_usd descending (m1=0.30 > m2=0.05 > unknown=0.01).
	require.True(t, sort.SliceIsSorted(by, func(i, j int) bool {
		return by[i].GetCostUsd() > by[j].GetCostUsd()
	}), "by_model is sorted by cost_usd descending")
	assert.Equal(t, "m1", by[0].GetModel(), "the most expensive model is first")
	assert.InDelta(t, 0.30, by[0].GetCostUsd(), 1e-9, "m1 folds both of its turns (0.10+0.20)")
}

// TestGetSessionCost_UnknownModelBucket (AC-4): a terminal event whose model
// could not be correlated lands in the "unknown" bucket (the server maps the
// empty model string to "unknown"), not dropped.
func TestGetSessionCost_UnknownModelBucket(t *testing.T) {
	log := newTailingEventLog()
	seedCostStream(t, log, "sess-unk", "tenant-A")

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.GetSessionCost(context.Background(), &genproto.GetSessionCostRequest{
		TenantId: "tenant-A", SessionId: "sess-unk",
	})
	require.NoError(t, err)

	var unknown *genproto.ModelCost
	for _, m := range resp.GetByModel() {
		assert.NotEmpty(t, m.GetModel(), "no model label is the empty string (empty must be mapped to 'unknown')")
		if m.GetModel() == "unknown" {
			unknown = m
		}
	}
	require.NotNil(t, unknown, "the uncorrelated turn lands in the 'unknown' bucket, not dropped")
	assert.InDelta(t, 0.01, unknown.GetCostUsd(), 1e-9, "the unknown-model turn's cost is retained")
	assert.Equal(t, int64(1), unknown.GetTurns(), "the unknown bucket counts its turn")
}

// TestGetSessionCost_RejectsForeignTenant (AC-9 edge): tenant B may not read
// tenant A's session cost — the shared authorizeSession ownership path.
func TestGetSessionCost_RejectsForeignTenant(t *testing.T) {
	log := newTailingEventLog()
	seedCostStream(t, log, "sess-A", "tenant-A")

	h := devHarness(t, "tenant-B", noopRunner(log), log)
	_, err := h.client.GetSessionCost(context.Background(), &genproto.GetSessionCostRequest{
		TenantId: "tenant-B", SessionId: "sess-A",
	})
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err),
		"tenant B cannot read tenant A's session cost (ownership, shared authorizeSession)")
}

// TestGetSessionCost_RejectsUnauthenticated (AC-8): an unauthenticated read is
// rejected in production-auth mode (no silent open cost edge).
func TestGetSessionCost_RejectsUnauthenticated(t *testing.T) {
	log := newTailingEventLog()
	seedCostStream(t, log, "sess-1", "tenant-A")
	gate := newNotifyingGate()
	conn := startServer(t, prodAuthConfig(), log, gate, noopRunner(log))
	client := genproto.NewOrchestratorServiceClient(conn)

	_, err := client.GetSessionCost(context.Background(), &genproto.GetSessionCostRequest{
		TenantId: "tenant-A", SessionId: "sess-1",
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err),
		"an unauthenticated session-cost read is rejected (shared edge auth)")
}

// ---------------------------------------------------------------------------
// GetTenantCost
// ---------------------------------------------------------------------------

// TestGetTenantCost_AggregatesAcrossSessions (AC-2): the per-tenant aggregate is
// the sum over EVERY session of the tenant, and session_count is the number of
// distinct sessions that carry cost.
func TestGetTenantCost_AggregatesAcrossSessions(t *testing.T) {
	log := newTailingEventLog()
	seedCostStream(t, log, "sess-1", "tenant-A") // 0.36
	seedCostStream(t, log, "sess-2", "tenant-A") // 0.36

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	resp, err := h.client.GetTenantCost(context.Background(), &genproto.GetTenantCostRequest{
		TenantId: "tenant-A",
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetTotal())

	assert.InDelta(t, 0.72, resp.GetTotal().GetCostUsd(), 1e-9, "tenant total = sum of both sessions")
	assert.Equal(t, int64(8), resp.GetTotal().GetTurns(), "tenant total turns = 4+4")
	assert.Equal(t, int64(2), resp.GetSessionCount(), "session_count = 2 distinct sessions with cost")

	// by_model still partitions the tenant total.
	cost, _, _, turns := sumByModel(resp.GetByModel())
	assert.InDelta(t, resp.GetTotal().GetCostUsd(), cost, 1e-9, "Σ by_model cost == tenant total cost")
	assert.Equal(t, resp.GetTotal().GetTurns(), turns, "Σ by_model turns == tenant total turns")
}

// TestGetTenantCost_RejectsMismatchedRequestTenant (AC-7): a non-empty
// request.tenant_id that does NOT match the authenticated principal is rejected
// (the request tenant is only a guard, never trusted).
func TestGetTenantCost_RejectsMismatchedRequestTenant(t *testing.T) {
	log := newTailingEventLog()
	seedCostStream(t, log, "sess-1", "tenant-A")

	h := devHarness(t, "tenant-A", noopRunner(log), log)
	_, err := h.client.GetTenantCost(context.Background(), &genproto.GetTenantCostRequest{
		TenantId: "tenant-EVIL", // != principal tenant-A
	})
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err),
		"a mismatched request.tenant_id is rejected (guard-only, never the filter key)")
}

// TestGetTenantCost_IgnoresRequestTenantAsFilter (AC-7): a MATCHING
// request.tenant_id is only a guard — the authority is the principal — so the
// result is identical to omitting tenant_id entirely.
func TestGetTenantCost_IgnoresRequestTenantAsFilter(t *testing.T) {
	log := newTailingEventLog()
	seedCostStream(t, log, "sess-1", "tenant-A")

	h := devHarness(t, "tenant-A", noopRunner(log), log)

	withTenant, err := h.client.GetTenantCost(context.Background(), &genproto.GetTenantCostRequest{TenantId: "tenant-A"})
	require.NoError(t, err)
	omitted, err := h.client.GetTenantCost(context.Background(), &genproto.GetTenantCostRequest{})
	require.NoError(t, err)

	assert.InDelta(t, withTenant.GetTotal().GetCostUsd(), omitted.GetTotal().GetCostUsd(), 1e-9,
		"a matching request.tenant_id is a guard only; the principal is the authority")
	assert.Equal(t, withTenant.GetSessionCount(), omitted.GetSessionCount(),
		"omitting tenant_id yields the identical result (principal-scoped)")
}

// TestGetTenantCost_RejectsUnauthenticated (AC-8): an unauthenticated tenant-cost
// read is rejected in production-auth mode.
func TestGetTenantCost_RejectsUnauthenticated(t *testing.T) {
	log := newTailingEventLog()
	seedCostStream(t, log, "sess-1", "tenant-A")
	gate := newNotifyingGate()
	conn := startServer(t, prodAuthConfig(), log, gate, noopRunner(log))
	client := genproto.NewOrchestratorServiceClient(conn)

	_, err := client.GetTenantCost(context.Background(), &genproto.GetTenantCostRequest{TenantId: "tenant-A"})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err),
		"an unauthenticated tenant-cost read is rejected (shared edge auth)")
}
