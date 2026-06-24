//go:build integration

package projection

// TDD (red) integration tests for Feature O's write path: the CostProjector
// wired into the Runner persists per-turn cost into session_cost_events as the
// worker tails the GLOBAL feed, idempotently over the xmin cursor, with per-model
// correlation across batch/restart boundaries.
//
// Authored BEFORE the implementation; EXPECTED to fail to compile / stay RED
// until migration 0006 (session_cost_events), the CostProjector, EventRow.Seq,
// and the Runner WithCostProjector wiring land. References symbols that do not
// yet exist (NewCostProjector, WithCostProjector) — that absence is the red proof.
//
// Pinned (SPEC AC-5/AC-6/AC-6b):
//   - cross-batch + restart per-model correlation: a TurnStarted in batch 1 and
//     its TurnFinished in batch 2 (after a simulated restart that clears the
//     in-flight map) still yields the correct model, via the events point lookup.
//   - idempotent over replay: re-running the worker from cursor 0 over the same
//     events leaves the row count unchanged (ON CONFLICT (global_id) DO NOTHING).
//   - rebuildable: TRUNCATE session_cost_events then re-fold from cursor 0
//     reproduces identical per-model aggregates (the projection is derived).

import (
	"context"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// tsPayload marshals a TurnStarted payload (carries the Model the projector
// correlates by TurnID).
func tsPayload(t *testing.T, turnID, model string) []byte {
	t.Helper()
	return mustJSON(t, domain.TurnStarted{TurnID: turnID, Model: model})
}

// tfPayloadTurn marshals a TurnFinished payload for a specific TurnID.
func tfPayloadTurn(t *testing.T, turnID string, cost float64, usage llm.Usage) []byte {
	t.Helper()
	return mustJSON(t, domain.TurnFinished{TurnID: turnID, Reason: domain.Success, Usage: usage, CostUSD: cost, NumTurns: 1})
}

// costRows reads the persisted session_cost_events for a session as model->row.
type scostRow struct {
	model string
	cost  float64
	turns int64
	in    int64
}

func readCostRows(ctx context.Context, t *testing.T, h *pharness, sessionID string) map[string]scostRow {
	t.Helper()
	rows, err := h.conn.Query(ctx, `
		SELECT model, SUM(cost_usd)::float8, COUNT(*), SUM(input_tokens)
		  FROM session_cost_events WHERE session_id = $1 GROUP BY model`, sessionID)
	if err != nil {
		t.Fatalf("read session_cost_events: %v", err)
	}
	defer rows.Close()
	out := map[string]scostRow{}
	for rows.Next() {
		var r scostRow
		if err := rows.Scan(&r.model, &r.cost, &r.turns, &r.in); err != nil {
			t.Fatalf("scan cost row: %v", err)
		}
		out[r.model] = r
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate cost rows: %v", err)
	}
	return out
}

func countCostRows(ctx context.Context, t *testing.T, h *pharness) int {
	t.Helper()
	var n int
	if err := h.conn.QueryRow(ctx, "SELECT COUNT(*) FROM session_cost_events").Scan(&n); err != nil {
		t.Fatalf("count session_cost_events: %v", err)
	}
	return n
}

// newCostRunner builds a Runner whose CostProjector writes to the harness conn.
func newCostRunner(h *pharness, sub string) *Runner {
	return NewRunner(
		Config{Subscription: sub, BatchSize: 100},
		NewSource(h.conn),
		WithCostProjector(NewCostProjector(h.conn)),
	)
}

// TestCostProjector_PersistsPerModel_Integration: started+finished turns across
// two models persist correct per-model cost rows in session_cost_events.
func TestCostProjector_PersistsPerModel_Integration(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := newUUID(t), newUUID(t)
	h.seedTenantSession(t, tenantID, sessionID)

	// One committed transaction: m1 (started+finished), m2 (started+finished).
	tx, err := h.conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	insertEventTx(ctx, t, tx, tenantID, sessionID, 1, string(domain.EventTurnStarted), tsPayload(t, "t1", "m1"))
	insertEventTx(ctx, t, tx, tenantID, sessionID, 2, string(domain.EventTurnFinished), tfPayloadTurn(t, "t1", 0.10, llm.Usage{InputTokens: 100}))
	insertEventTx(ctx, t, tx, tenantID, sessionID, 3, string(domain.EventTurnStarted), tsPayload(t, "t2", "m2"))
	insertEventTx(ctx, t, tx, tenantID, sessionID, 4, string(domain.EventTurnFinished), tfPayloadTurn(t, "t2", 0.25, llm.Usage{InputTokens: 200}))
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	r := newCostRunner(h, "cost-rollup")
	if err := r.runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}

	got := readCostRows(ctx, t, h, sessionID)
	if m1 := got["m1"]; !floatEq(m1.cost, 0.10) || m1.turns != 1 || m1.in != 100 {
		t.Fatalf("m1 row = %+v, want cost 0.10 / 1 turn / 100 input", m1)
	}
	if m2 := got["m2"]; !floatEq(m2.cost, 0.25) || m2.turns != 1 || m2.in != 200 {
		t.Fatalf("m2 row = %+v, want cost 0.25 / 1 turn / 200 input", m2)
	}
}

// TestCostProjector_CrossBatchRestartCorrelation_Integration (AC-5 slow path):
// the TurnStarted commits in one batch and its TurnFinished in a LATER batch
// processed by a FRESH Runner (simulating a restart that clears the in-flight
// map). The model is recovered via the events point lookup, not lost to unknown.
func TestCostProjector_CrossBatchRestartCorrelation_Integration(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := newUUID(t), newUUID(t)
	h.seedTenantSession(t, tenantID, sessionID)

	// Batch 1: only the TurnStarted (model m9) commits.
	txA, err := h.conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	insertEventTx(ctx, t, txA, tenantID, sessionID, 1, string(domain.EventTurnStarted), tsPayload(t, "tX", "m9"))
	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}

	// First worker catches up over batch 1, then is discarded (in-flight map gone).
	r1 := newCostRunner(h, "cost-rollup")
	if err := r1.runOnce(ctx); err != nil {
		t.Fatalf("runOnce r1: %v", err)
	}

	// Batch 2: the matching TurnFinished commits AFTER the restart.
	txB, err := h.conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin B: %v", err)
	}
	insertEventTx(ctx, t, txB, tenantID, sessionID, 2, string(domain.EventTurnFinished), tfPayloadTurn(t, "tX", 0.50, llm.Usage{InputTokens: 500}))
	if err := txB.Commit(ctx); err != nil {
		t.Fatalf("commit B: %v", err)
	}

	// A FRESH worker resumes from the saved cursor; its in-flight map is empty, so
	// the model must come from the point lookup over events.
	r2 := newCostRunner(h, "cost-rollup")
	if err := r2.runOnce(ctx); err != nil {
		t.Fatalf("runOnce r2: %v", err)
	}

	got := readCostRows(ctx, t, h, sessionID)
	if _, unknown := got[""]; unknown {
		t.Fatalf("the cross-batch turn landed in the unknown bucket; point-lookup recovery failed: %+v", got)
	}
	m9, ok := got["m9"]
	if !ok || !floatEq(m9.cost, 0.50) || m9.turns != 1 {
		t.Fatalf("m9 row = %+v (ok=%v), want cost 0.50 / 1 turn (model recovered cross-batch)", m9, ok)
	}
}

// TestCostProjector_IdempotentReplay_Integration (AC-6): replaying the SAME
// events from cursor 0 leaves the row count unchanged — ON CONFLICT (global_id)
// DO NOTHING turns the at-least-once re-read into an identity no-op.
func TestCostProjector_IdempotentReplay_Integration(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := newUUID(t), newUUID(t)
	h.seedTenantSession(t, tenantID, sessionID)

	tx, err := h.conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	insertEventTx(ctx, t, tx, tenantID, sessionID, 1, string(domain.EventTurnStarted), tsPayload(t, "t1", "m1"))
	insertEventTx(ctx, t, tx, tenantID, sessionID, 2, string(domain.EventTurnFinished), tfPayloadTurn(t, "t1", 0.10, llm.Usage{InputTokens: 100}))
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// First catch-up.
	if err := newCostRunner(h, "cost-rollup").runOnce(ctx); err != nil {
		t.Fatalf("runOnce 1: %v", err)
	}
	after1 := countCostRows(ctx, t, h)

	// Reset the cursor to 0 and replay the whole feed — re-processes the same
	// global_ids. With ON CONFLICT the count must not grow.
	if _, err := h.conn.Exec(ctx,
		"UPDATE event_subscriptions SET last_transaction_id='0'::xid8, last_global_id=0 WHERE name=$1", "cost-rollup"); err != nil {
		t.Fatalf("reset cursor: %v", err)
	}
	if err := newCostRunner(h, "cost-rollup").runOnce(ctx); err != nil {
		t.Fatalf("runOnce 2 (replay): %v", err)
	}
	after2 := countCostRows(ctx, t, h)

	if after1 != after2 {
		t.Fatalf("row count changed across replay %d -> %d: ON CONFLICT (global_id) did not make the re-read a no-op", after1, after2)
	}
}

// TestCostProjector_TruncateAndRefold_Integration (AC-6b): the projection is
// rebuildable — TRUNCATE session_cost_events then re-fold from cursor 0 yields
// identical per-model aggregates (the authoritative record is the event log).
func TestCostProjector_TruncateAndRefold_Integration(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	tenantID, sessionID := newUUID(t), newUUID(t)
	h.seedTenantSession(t, tenantID, sessionID)

	tx, err := h.conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	insertEventTx(ctx, t, tx, tenantID, sessionID, 1, string(domain.EventTurnStarted), tsPayload(t, "t1", "m1"))
	insertEventTx(ctx, t, tx, tenantID, sessionID, 2, string(domain.EventTurnFinished), tfPayloadTurn(t, "t1", 0.10, llm.Usage{InputTokens: 100}))
	insertEventTx(ctx, t, tx, tenantID, sessionID, 3, string(domain.EventTurnStarted), tsPayload(t, "t2", "m2"))
	insertEventTx(ctx, t, tx, tenantID, sessionID, 4, string(domain.EventTurnFinished), tfPayloadTurn(t, "t2", 0.40, llm.Usage{InputTokens: 400}))
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if err := newCostRunner(h, "cost-rollup").runOnce(ctx); err != nil {
		t.Fatalf("runOnce 1: %v", err)
	}
	before := readCostRows(ctx, t, h, sessionID)

	// Rebuild: truncate the projection + reset the cursor, then re-fold.
	if _, err := h.conn.Exec(ctx, "TRUNCATE session_cost_events"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, err := h.conn.Exec(ctx,
		"UPDATE event_subscriptions SET last_transaction_id='0'::xid8, last_global_id=0 WHERE name=$1", "cost-rollup"); err != nil {
		t.Fatalf("reset cursor: %v", err)
	}
	if err := newCostRunner(h, "cost-rollup").runOnce(ctx); err != nil {
		t.Fatalf("runOnce 2 (refold): %v", err)
	}
	after := readCostRows(ctx, t, h, sessionID)

	if len(before) != len(after) {
		t.Fatalf("rebuild changed model bucket count %d -> %d", len(before), len(after))
	}
	for model, b := range before {
		a, ok := after[model]
		if !ok || !floatEq(a.cost, b.cost) || a.turns != b.turns || a.in != b.in {
			t.Fatalf("model %q: rebuild = %+v, want identical to %+v", model, a, b)
		}
	}
}
