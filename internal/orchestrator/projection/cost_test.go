package projection

import (
	"encoding/json"
	"testing"

	"github.com/boltrope/boltrope/internal/orchestrator/domain"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// mustPayload marshals a domain event to its JSONB payload (the same encoding the
// event store persists), for hand-built projection rows.
func mustPayload(t *testing.T, e domain.Event) []byte {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal %T: %v", e, err)
	}
	return b
}

// turnFinishedRow builds an EventRow carrying a TurnFinished payload.
func turnFinishedRow(t *testing.T, txn uint64, gid int64, tenant, session string, cost float64, usage llm.Usage) EventRow {
	t.Helper()
	return EventRow{
		TransactionID: txn,
		GlobalID:      gid,
		TenantID:      tenant,
		SessionID:     session,
		Type:          domain.EventTurnFinished,
		Payload:       mustPayload(t, domain.TurnFinished{TurnID: "tf", Reason: domain.Success, Usage: usage, CostUSD: cost, NumTurns: 1}),
	}
}

// turnAbortedRow builds an EventRow carrying a TurnAborted payload.
func turnAbortedRow(t *testing.T, txn uint64, gid int64, tenant, session string, cost float64, usage llm.Usage) EventRow {
	t.Helper()
	return EventRow{
		TransactionID: txn,
		GlobalID:      gid,
		TenantID:      tenant,
		SessionID:     session,
		Type:          domain.EventTurnAborted,
		Payload:       mustPayload(t, domain.TurnAborted{TurnID: "ta", Reason: domain.ErrorDuringExecution, UsageSoFar: usage, CostUSD: cost}),
	}
}

// otherRow builds a non-cost-bearing EventRow (a TurnStarted) that the fold must
// ignore.
func otherRow(t *testing.T, txn uint64, gid int64, tenant, session string) EventRow {
	t.Helper()
	return EventRow{
		TransactionID: txn,
		GlobalID:      gid,
		TenantID:      tenant,
		SessionID:     session,
		Type:          domain.EventTurnStarted,
		Payload:       mustPayload(t, domain.TurnStarted{TurnID: "ts", Model: "m"}),
	}
}

// TestRollupFold_SumsPerSession covers the cost projection: TurnFinished and
// TurnAborted costs (and usage) fold into the correct per-(tenant, session)
// bucket; non-terminal events are ignored; the total equals the event sum.
func TestRollupFold_SumsPerSession(t *testing.T) {
	const (
		tenant = "tenant-a"
		sessA  = "session-1"
		sessB  = "session-2"
	)
	rows := []EventRow{
		otherRow(t, 1, 1, tenant, sessA), // ignored
		turnFinishedRow(t, 1, 2, tenant, sessA, 0.10, llm.Usage{InputTokens: 100, OutputTokens: 20}),
		turnFinishedRow(t, 2, 3, tenant, sessA, 0.25, llm.Usage{InputTokens: 200, OutputTokens: 40, CacheReadTokens: 10}),
		turnAbortedRow(t, 2, 4, tenant, sessA, 0.05, llm.Usage{InputTokens: 30, OutputTokens: 5}), // partial turn still billed
		turnFinishedRow(t, 3, 5, tenant, sessB, 1.00, llm.Usage{InputTokens: 500, OutputTokens: 100}),
		otherRow(t, 3, 6, tenant, sessB), // ignored
	}

	totals, err := RollupFold(nil, rows)
	if err != nil {
		t.Fatalf("RollupFold: %v", err)
	}

	a := totals[SessionKey{TenantID: tenant, SessionID: sessA}]
	if a == nil {
		t.Fatalf("no rollup for session A")
	}
	if got, want := a.CostUSD, 0.40; !floatEq(got, want) {
		t.Fatalf("session A cost = %v, want %v", got, want)
	}
	if a.Turns != 3 {
		t.Fatalf("session A turns = %d, want 3 (2 finished + 1 aborted)", a.Turns)
	}
	wantUsageA := llm.Usage{InputTokens: 330, OutputTokens: 65, CacheReadTokens: 10}
	if a.Usage != wantUsageA {
		t.Fatalf("session A usage = %+v, want %+v", a.Usage, wantUsageA)
	}

	b := totals[SessionKey{TenantID: tenant, SessionID: sessB}]
	if b == nil || !floatEq(b.CostUSD, 1.00) || b.Turns != 1 {
		t.Fatalf("session B rollup = %+v, want cost 1.00 / 1 turn", b)
	}
}

// TestRollupFold_AccumulatesAcrossBatches asserts that folding successive batches
// into the same accumulator keeps a running total (the worker folds one poll
// batch at a time).
func TestRollupFold_AccumulatesAcrossBatches(t *testing.T) {
	const tenant, sess = "t", "s"
	key := SessionKey{TenantID: tenant, SessionID: sess}

	totals, err := RollupFold(nil, []EventRow{
		turnFinishedRow(t, 1, 1, tenant, sess, 0.30, llm.Usage{InputTokens: 10}),
	})
	if err != nil {
		t.Fatalf("first fold: %v", err)
	}
	totals, err = RollupFold(totals, []EventRow{
		turnFinishedRow(t, 2, 2, tenant, sess, 0.70, llm.Usage{InputTokens: 5}),
	})
	if err != nil {
		t.Fatalf("second fold: %v", err)
	}
	if got := totals[key]; got == nil || !floatEq(got.CostUSD, 1.00) || got.Turns != 2 || got.Usage.InputTokens != 15 {
		t.Fatalf("accumulated rollup = %+v, want cost 1.00 / 2 turns / 15 input tokens", got)
	}
}

// TestRollupFold_TenantIsolation asserts identical session ids under different
// tenants never share a bucket (the rollup is per (tenant, session), never
// cross-tenant).
func TestRollupFold_TenantIsolation(t *testing.T) {
	const sharedSession = "session-x"
	rows := []EventRow{
		turnFinishedRow(t, 1, 1, "tenant-a", sharedSession, 0.10, llm.Usage{}),
		turnFinishedRow(t, 1, 2, "tenant-b", sharedSession, 0.99, llm.Usage{}),
	}
	totals, err := RollupFold(nil, rows)
	if err != nil {
		t.Fatalf("RollupFold: %v", err)
	}
	if a := totals[SessionKey{TenantID: "tenant-a", SessionID: sharedSession}]; a == nil || !floatEq(a.CostUSD, 0.10) {
		t.Fatalf("tenant-a rollup = %+v, want cost 0.10", a)
	}
	if b := totals[SessionKey{TenantID: "tenant-b", SessionID: sharedSession}]; b == nil || !floatEq(b.CostUSD, 0.99) {
		t.Fatalf("tenant-b rollup = %+v, want cost 0.99", b)
	}
	if len(totals) != 2 {
		t.Fatalf("got %d buckets, want 2 (no cross-tenant sharing)", len(totals))
	}
}

// TestRollupFold_MalformedPayloadIsError asserts a corrupt turn-terminal payload
// is a hard error (a silently dropped cost would make the rollup disagree with
// the log).
func TestRollupFold_MalformedPayloadIsError(t *testing.T) {
	rows := []EventRow{
		{TransactionID: 1, GlobalID: 1, TenantID: "t", SessionID: "s", Type: domain.EventTurnFinished, Payload: []byte("{not json")},
	}
	if _, err := RollupFold(nil, rows); err == nil {
		t.Fatal("RollupFold accepted a malformed TurnFinished payload, want error")
	}
}

// floatEq compares two USD costs within a cent's worth of float slack.
func floatEq(a, b float64) bool {
	const eps = 1e-9
	d := a - b
	return d < eps && d > -eps
}
