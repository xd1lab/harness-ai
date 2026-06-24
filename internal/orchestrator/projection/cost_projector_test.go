package projection

// TDD (red) unit tests for Feature O's CostProjector sink: the projectord write
// side that UPSERTs per-turn cost rows into session_cost_events as it tails
// TurnFinished/TurnAborted. Authored BEFORE the implementation; EXPECTED to fail
// to compile until CostProjector lands (it references CostProjector,
// NewCostProjector, EventRow.Seq — none exist yet). That absence is the red proof.
//
// Pinned decisions (DECISIONS §D, SPEC §3 / AC-4/5/6/10):
//   - Idempotency by EVENT IDENTITY: the insert is
//     `INSERT INTO session_cost_events (global_id, …) VALUES (…) ON CONFLICT
//     (global_id) DO NOTHING`. global_id is the natural PK, so re-processing the
//     same event (crash re-read from the saved cursor) is an identity no-op — no
//     additive double count, with or without a wrapping transaction.
//   - Per-model correlation at the WRITE side: TurnStarted.Model is correlated to
//     the later TurnFinished/TurnAborted by (session, TurnID). Fast path = an
//     in-flight map; on a miss (cross-batch / post-restart) a point lookup over
//     events by Seq; on a total miss the model is "" (the read side maps it to the
//     "unknown" bucket).
//   - Write-side tenant correctness by PER-ROW COPY (SPEC AC-10 amended): the
//     written tenant_id/session_id are COPIED from the source EventRow, never
//     trusted from a GUC. A best-effort `SELECT set_config('app.current_tenant',
//     <event.tenant_id>, true)` precedes the insert as documented intent /
//     future RLS-restorability, but correctness does not depend on it in v1.
//   - Non-terminal events write nothing (parity with RollupFold).

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// recordedExec is one Exec call the recording conn captured.
type recordedExec struct {
	sql  string
	args []any
}

// recordingConn is a [Conn] that records every Exec (the cost INSERTs and the
// best-effort set_config) and serves a canned point-lookup row for the slow-path
// model recovery query. It lets the unit tests assert the emitted SQL shape and
// per-row arguments without a database.
type recordingConn struct {
	mu    sync.Mutex
	execs []recordedExec
	// lookupModel is returned by QueryRow for the TurnStarted point-lookup
	// (payload->>'Model'); lookupErr (e.g. pgx.ErrNoRows) forces a miss.
	lookupModel string
	lookupErr   error
}

func (c *recordingConn) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &fakeRows{}, nil
}

func (c *recordingConn) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	if c.lookupErr != nil {
		return fakeRow{err: c.lookupErr}
	}
	return strRow{val: c.lookupModel}
}

func (c *recordingConn) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execs = append(c.execs, recordedExec{sql: sql, args: args})
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

var _ Conn = (*recordingConn)(nil)

// strRow is a one-string pgx.Row for the model point-lookup.
type strRow struct{ val string }

func (r strRow) Scan(dest ...any) error {
	if len(dest) != 1 {
		return errScanArity
	}
	if p, ok := dest[0].(*string); ok {
		*p = r.val
		return nil
	}
	return errScanType
}

var (
	errScanArity = pgErr("strRow: want 1 dest")
	errScanType  = pgErr("strRow: dest not *string")
)

type pgErr string

func (e pgErr) Error() string { return string(e) }

// inserts filters the recorded calls to the cost INSERTs (excludes set_config).
func (c *recordingConn) inserts() []recordedExec {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []recordedExec
	for _, e := range c.execs {
		if strings.Contains(e.sql, "INSERT INTO session_cost_events") {
			out = append(out, e)
		}
	}
	return out
}

// sawSetConfig reports whether a best-effort set_config('app.current_tenant', …)
// was issued (for any of the recorded execs) with the given tenant value.
func (c *recordingConn) sawSetConfig(tenant string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.execs {
		if strings.Contains(e.sql, "set_config") && strings.Contains(e.sql, "app.current_tenant") {
			for _, a := range e.args {
				if s, ok := a.(string); ok && s == tenant {
					return true
				}
			}
		}
	}
	return false
}

// tfRowSeq / tsRowSeq / taRowSeq build EventRows carrying a Seq (Feature O adds
// EventRow.Seq for the slow-path point lookup; see source.go T-10).
func tsRowSeq(t *testing.T, gid, seq int64, tenant, session, turnID, model string) EventRow {
	t.Helper()
	return EventRow{
		GlobalID: gid, Seq: seq, TenantID: tenant, SessionID: session,
		Type: domain.EventTurnStarted, Payload: mustPayload(t, domain.TurnStarted{TurnID: turnID, Model: model}),
	}
}

func tfRowSeq(t *testing.T, gid, seq int64, tenant, session, turnID string, cost float64, usage llm.Usage) EventRow {
	t.Helper()
	return EventRow{
		GlobalID: gid, Seq: seq, TenantID: tenant, SessionID: session,
		Type:    domain.EventTurnFinished,
		Payload: mustPayload(t, domain.TurnFinished{TurnID: turnID, Reason: domain.Success, Usage: usage, CostUSD: cost, NumTurns: 1}),
	}
}

// firstStringArg returns the first arg of a recorded exec equal to want (used to
// assert tenant_id/session_id/model were copied into the insert).
func argsContain(e recordedExec, want any) bool {
	for _, a := range e.args {
		if a == want {
			return true
		}
	}
	return false
}

// TestCostProjector_CorrelatesModelByTurnID (AC-5 fast path): a TurnStarted then
// its TurnFinished in the SAME batch -> the written row carries the started
// model.
func TestCostProjector_CorrelatesModelByTurnID(t *testing.T) {
	conn := &recordingConn{}
	p := NewCostProjector(conn)
	ctx := context.Background()

	rows := []EventRow{
		tsRowSeq(t, 1, 1, "ten", "sess", "t1", "claude-x"),
		tfRowSeq(t, 2, 2, "ten", "sess", "t1", 0.10, llm.Usage{InputTokens: 100}),
	}
	if err := p.Project(ctx, rows); err != nil {
		t.Fatalf("Project: %v", err)
	}

	ins := conn.inserts()
	if len(ins) != 1 {
		t.Fatalf("got %d cost inserts, want 1 (one terminal event)", len(ins))
	}
	if !argsContain(ins[0], "claude-x") {
		t.Fatalf("insert args %v missing correlated model 'claude-x'", ins[0].args)
	}
}

// TestCostProjector_UnknownModelOnMiss (AC-4): a TurnFinished with no in-flight
// TurnStarted AND a point-lookup miss -> model "" (the read side renders it as
// "unknown").
func TestCostProjector_UnknownModelOnMiss(t *testing.T) {
	conn := &recordingConn{lookupErr: pgx.ErrNoRows} // slow-path miss too
	p := NewCostProjector(conn)

	rows := []EventRow{
		tfRowSeq(t, 5, 5, "ten", "sess", "orphan", 0.07, llm.Usage{InputTokens: 10}),
	}
	if err := p.Project(context.Background(), rows); err != nil {
		t.Fatalf("Project: %v", err)
	}

	ins := conn.inserts()
	if len(ins) != 1 {
		t.Fatalf("got %d inserts, want 1", len(ins))
	}
	if !argsContain(ins[0], "") {
		t.Fatalf("insert args %v missing the empty (unknown) model on a total miss", ins[0].args)
	}
}

// TestCostProjector_RecoversModelViaPointLookup (AC-5 slow path): a TurnFinished
// whose TurnStarted is NOT in this batch (cross-batch / post-restart) recovers
// the model via the events point lookup keyed on Seq.
func TestCostProjector_RecoversModelViaPointLookup(t *testing.T) {
	conn := &recordingConn{lookupModel: "recovered-model"} // QueryRow returns the prior TurnStarted.Model
	p := NewCostProjector(conn)

	rows := []EventRow{
		tfRowSeq(t, 9, 9, "ten", "sess", "t-prev", 0.20, llm.Usage{InputTokens: 200}),
	}
	if err := p.Project(context.Background(), rows); err != nil {
		t.Fatalf("Project: %v", err)
	}

	ins := conn.inserts()
	if len(ins) != 1 || !argsContain(ins[0], "recovered-model") {
		t.Fatalf("insert args %v missing the point-lookup-recovered model", insArgs(ins))
	}
}

// TestCostProjector_InsertOnConflictDoNothing (AC-6): the emitted SQL is
// `INSERT … ON CONFLICT (global_id) DO NOTHING`, and feeding the SAME global_id
// twice issues an idempotent write (the DB makes the second a no-op; the
// projector does not additively double-count by construction).
func TestCostProjector_InsertOnConflictDoNothing(t *testing.T) {
	conn := &recordingConn{}
	p := NewCostProjector(conn)
	row := tfRowSeq(t, 42, 42, "ten", "sess", "t1", 0.10, llm.Usage{InputTokens: 100})

	if err := p.Project(context.Background(), []EventRow{row}); err != nil {
		t.Fatalf("Project (first): %v", err)
	}
	if err := p.Project(context.Background(), []EventRow{row}); err != nil {
		t.Fatalf("Project (replay): %v", err)
	}

	ins := conn.inserts()
	if len(ins) == 0 {
		t.Fatal("no cost insert emitted")
	}
	for _, e := range ins {
		if !strings.Contains(e.sql, "ON CONFLICT (global_id) DO NOTHING") {
			t.Fatalf("insert SQL lacks the idempotency clause:\n%s", e.sql)
		}
		if !strings.Contains(e.sql, "INSERT INTO session_cost_events") {
			t.Fatalf("insert SQL targets the wrong table:\n%s", e.sql)
		}
	}
	// Both replays carry the same global_id (the DB collapses the second).
	if !argsContain(ins[0], int64(42)) {
		t.Fatalf("insert args %v missing global_id 42", ins[0].args)
	}
}

// TestCostProjector_CopiesTenantPerRow (AC-10 amended): the written tenant_id and
// session_id equal the SOURCE row's fields (per-row copy, not a GUC), and a
// best-effort set_config('app.current_tenant', <that tenant>, true) precedes the
// insert.
func TestCostProjector_CopiesTenantPerRow(t *testing.T) {
	conn := &recordingConn{}
	p := NewCostProjector(conn)
	row := tfRowSeq(t, 7, 7, "tenant-copy", "session-copy", "t1", 0.10, llm.Usage{InputTokens: 1})

	if err := p.Project(context.Background(), []EventRow{row}); err != nil {
		t.Fatalf("Project: %v", err)
	}

	ins := conn.inserts()
	if len(ins) != 1 {
		t.Fatalf("got %d inserts, want 1", len(ins))
	}
	if !argsContain(ins[0], "tenant-copy") {
		t.Fatalf("insert args %v missing per-row-copied tenant_id", ins[0].args)
	}
	if !argsContain(ins[0], "session-copy") {
		t.Fatalf("insert args %v missing per-row-copied session_id", ins[0].args)
	}
	if !conn.sawSetConfig("tenant-copy") {
		t.Fatal("no best-effort set_config('app.current_tenant', 'tenant-copy', true) before the insert")
	}
}

// TestCostProjector_SkipsNonTerminal (parity with RollupFold): non-terminal
// events (e.g. the TurnStarted itself, a MessageAppended) write no cost rows.
func TestCostProjector_SkipsNonTerminal(t *testing.T) {
	conn := &recordingConn{}
	p := NewCostProjector(conn)

	rows := []EventRow{
		tsRowSeq(t, 1, 1, "ten", "sess", "t1", "m"), // started: no cost row
		{GlobalID: 2, Seq: 2, TenantID: "ten", SessionID: "sess", Type: domain.EventMessageAppended, Payload: []byte(`{}`)},
	}
	if err := p.Project(context.Background(), rows); err != nil {
		t.Fatalf("Project: %v", err)
	}
	if got := len(conn.inserts()); got != 0 {
		t.Fatalf("got %d cost inserts for non-terminal events, want 0", got)
	}
}

// TestEventRow_CarriesSeq (T-10 parity): the projection EventRow gains a Seq
// field (additive) so the CostProjector's slow-path point lookup can bound the
// TurnStarted search by `seq < $2`. The in-memory fold ignores it.
func TestEventRow_CarriesSeq(t *testing.T) {
	r := EventRow{GlobalID: 1, Seq: 99, TenantID: "t", SessionID: "s", Type: domain.EventTurnFinished}
	if r.Seq != 99 {
		t.Fatalf("EventRow.Seq = %d, want 99 (additive field for cross-batch model recovery)", r.Seq)
	}
}

// insArgs flattens recorded insert args for error messages.
func insArgs(ins []recordedExec) []any {
	var out []any
	for _, e := range ins {
		out = append(out, e.args...)
	}
	return out
}
