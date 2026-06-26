package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// fakeConn is a hand-built [Conn] for unit-testing the runner's safe-advance loop
// without a database. It models the event_subscriptions cursor row and a fixed,
// already-ordered, already-xmin-bounded slice of events; FetchBatch reads forward
// of the cursor up to the limit, exactly as the real gap-safe query would for the
// rows it admits.
type fakeConn struct {
	mu sync.Mutex
	// cursorTxn/cursorGID is the persisted subscription cursor.
	cursorTxn uint64
	cursorGID int64
	hasRow    bool
	// events is the settled-below-xmin feed in (txn, global) order.
	events []EventRow
	// fetchErr, when set, makes the next FetchBatch fail (transient-error test).
	fetchErr error
}

// fakeRows adapts a slice of EventRow to pgx.Rows for the SELECT statements.
type fakeRows struct {
	cols [][]any
	i    int
}

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return nil }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Values() ([]any, error)                       { return r.cols[r.i-1], nil }
func (r *fakeRows) Next() bool {
	if r.i >= len(r.cols) {
		return false
	}
	r.i++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	row := r.cols[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("fakeRows: scan into %d dest, have %d cols", len(dest), len(row))
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = row[i].(string)
		case *int64:
			*p = row[i].(int64)
		case *[]byte:
			*p = row[i].([]byte)
		case *time.Time:
			*p = row[i].(time.Time)
		default:
			return fmt.Errorf("fakeRows: unsupported dest type %T at %d", d, i)
		}
	}
	return nil
}

// fakeRow adapts a single scalar to pgx.Row for QueryRow.
type fakeRow struct {
	vals []any
	err  error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *uint64:
			*p = r.vals[i].(uint64)
		case *int64:
			*p = r.vals[i].(int64)
		default:
			return fmt.Errorf("fakeRow: unsupported dest type %T", d)
		}
	}
	return nil
}

func (c *fakeConn) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fetchErr != nil {
		err := c.fetchErr
		c.fetchErr = nil
		return nil, err
	}
	// Only the fetch-batch SELECT returns event rows in this fake.
	curTxn := mustParseUint(args[0].(string))
	curGID := args[1].(int64)
	limit := args[2].(int)
	var cols [][]any
	for _, e := range c.events {
		after := e.TransactionID > curTxn || (e.TransactionID == curTxn && e.GlobalID > curGID)
		if !after {
			continue
		}
		cols = append(cols, []any{
			uint64ToText(e.TransactionID), e.GlobalID, e.Seq, e.TenantID, e.SessionID, string(e.Type), e.Payload,
			e.ContentHash, e.ChainHash, e.Actor, e.CreatedAt,
		})
		if len(cols) >= limit {
			break
		}
	}
	_ = sql
	return &fakeRows{cols: cols}, nil
}

func (c *fakeConn) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case contains(sql, "FROM event_subscriptions"):
		if !c.hasRow {
			return fakeRow{err: pgx.ErrNoRows}
		}
		return fakeRow{vals: []any{c.cursorTxn, c.cursorGID}}
	case contains(sql, "COUNT(*)"):
		curTxn := mustParseUint(args[0].(string))
		curGID := args[1].(int64)
		var n int64
		for _, e := range c.events {
			if e.TransactionID > curTxn || (e.TransactionID == curTxn && e.GlobalID > curGID) {
				n++
			}
		}
		return fakeRow{vals: []any{n}}
	default:
		return fakeRow{err: fmt.Errorf("fakeConn: unexpected QueryRow: %s", sql)}
	}
}

func (c *fakeConn) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case contains(sql, "INSERT INTO event_subscriptions"):
		if !c.hasRow {
			c.hasRow = true
		}
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	case contains(sql, "UPDATE event_subscriptions"):
		if !c.hasRow {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		c.cursorTxn = mustParseUint(args[1].(string))
		c.cursorGID = args[2].(int64)
		return pgconn.NewCommandTag("UPDATE 1"), nil
	default:
		return pgconn.CommandTag{}, fmt.Errorf("fakeConn: unexpected Exec: %s", sql)
	}
}

// fakeMetrics records the metric publishes for assertions.
type fakeMetrics struct {
	mu        sync.Mutex
	lastLag   int64
	lagSet    int
	costAdds  []costAdd
	costTotal float64
}

type costAdd struct {
	tenant string
	delta  float64
}

func (m *fakeMetrics) SetProjectionLag(events int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastLag = events
	m.lagSet++
}

func (m *fakeMetrics) AddCost(_ context.Context, tenantID string, deltaUSD float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.costAdds = append(m.costAdds, costAdd{tenant: tenantID, delta: deltaUSD})
	m.costTotal += deltaUSD
}

// TestRunner_CatchUp_FoldsAdvancesAndPublishes drives the runner's single catch-up
// pass over a hand-built feed and asserts: the cursor advances to the last row,
// the cost rollup equals the event sum, the lag gauge ends at zero, and the cost
// counter received the per-batch delta.
func TestRunner_CatchUp_FoldsAdvancesAndPublishes(t *testing.T) {
	const tenant, sess = "tenant-a", "sess-1"
	conn := &fakeConn{
		events: []EventRow{
			rowTF(t, 100, 1, tenant, sess, 0.10, llm.Usage{InputTokens: 10}),
			rowOther(t, 100, 2, tenant, sess),
			rowTF(t, 101, 3, tenant, sess, 0.20, llm.Usage{OutputTokens: 5}),
			rowTA(t, 102, 4, tenant, sess, 0.05, llm.Usage{InputTokens: 3}),
		},
	}
	metrics := &fakeMetrics{}
	r := NewRunner(
		Config{Subscription: "cost-rollup", BatchSize: 2}, // batch < feed to exercise multi-batch drain
		NewSource(conn),
		WithMetrics(metrics),
	)

	if err := r.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce: %v", err)
	}

	// Cursor advanced to the last row (102, 4) and was persisted.
	if conn.cursorTxn != 102 || conn.cursorGID != 4 {
		t.Fatalf("persisted cursor = (%d,%d), want (102,4)", conn.cursorTxn, conn.cursorGID)
	}
	if r.cursor != (Cursor{TransactionID: 102, GlobalID: 4}) {
		t.Fatalf("in-memory cursor = %s, want (txn=102,global=4)", r.cursor)
	}

	// Cost rollup equals the event sum (0.10 + 0.20 + 0.05).
	got := r.Totals()[SessionKey{TenantID: tenant, SessionID: sess}]
	if !floatEq(got.CostUSD, 0.35) || got.Turns != 3 {
		t.Fatalf("rollup = %+v, want cost 0.35 / 3 turns", got)
	}

	// Lag ends at zero (fully caught up) and was published.
	if metrics.lastLag != 0 || metrics.lagSet == 0 {
		t.Fatalf("lag last=%d setCount=%d, want last 0 and >=1 set", metrics.lastLag, metrics.lagSet)
	}
	// Cost counter received the per-batch deltas summing to 0.35 for the tenant.
	if !floatEq(metrics.costTotal, 0.35) {
		t.Fatalf("cost counter total = %v, want 0.35", metrics.costTotal)
	}
	for _, a := range metrics.costAdds {
		if a.tenant != tenant {
			t.Fatalf("cost add attributed to %q, want %q", a.tenant, tenant)
		}
	}
}

// TestRunner_ResumesFromPersistedCursor asserts a runner started against a cursor
// row already advanced past part of the feed only folds the remaining tail
// (resume semantics; architecture §10.4).
func TestRunner_ResumesFromPersistedCursor(t *testing.T) {
	const tenant, sess = "t", "s"
	conn := &fakeConn{
		hasRow:    true,
		cursorTxn: 100, // already processed (100,1)
		cursorGID: 1,
		events: []EventRow{
			rowTF(t, 100, 1, tenant, sess, 9.99, llm.Usage{}), // before cursor: must be skipped
			rowTF(t, 101, 2, tenant, sess, 0.50, llm.Usage{}),
		},
	}
	r := NewRunner(Config{Subscription: "cost-rollup", BatchSize: 10}, NewSource(conn))
	if err := r.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	got, ok := r.Totals()[SessionKey{TenantID: tenant, SessionID: sess}]
	if !ok || !floatEq(got.CostUSD, 0.50) || got.Turns != 1 {
		t.Fatalf("resumed rollup = %+v (present=%v), want only the tail (cost 0.50 / 1 turn)", got, ok)
	}
	if conn.cursorTxn != 101 || conn.cursorGID != 2 {
		t.Fatalf("cursor = (%d,%d), want (101,2)", conn.cursorTxn, conn.cursorGID)
	}
}

// TestRunner_FetchErrorIsNonFatal asserts a transient fetch error during catch-up
// is swallowed (logged, retried next tick) and does not advance the cursor or
// crash the worker.
func TestRunner_FetchErrorIsNonFatal(t *testing.T) {
	conn := &fakeConn{
		fetchErr: errors.New("transient db blip"),
		events:   []EventRow{rowTF(t, 1, 1, "t", "s", 0.1, llm.Usage{})},
	}
	r := NewRunner(Config{Subscription: "cost-rollup", BatchSize: 10}, NewSource(conn))
	// runOnce must return nil (the fetch error is internal to catchUp and retried).
	if err := r.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce returned %v, want nil (transient fetch error is non-fatal)", err)
	}
	if conn.cursorTxn != 0 {
		t.Fatalf("cursor advanced to %d on a failed fetch, want 0 (unchanged)", conn.cursorTxn)
	}
}

// --- row builders for the runner test (mirror the cost_test helpers) ---

func rowTF(t *testing.T, txn uint64, gid int64, tenant, sess string, cost float64, usage llm.Usage) EventRow {
	t.Helper()
	p, _ := json.Marshal(domain.TurnFinished{TurnID: "tf", Reason: domain.Success, Usage: usage, CostUSD: cost, NumTurns: 1})
	return EventRow{TransactionID: txn, GlobalID: gid, TenantID: tenant, SessionID: sess, Type: domain.EventTurnFinished, Payload: p}
}

func rowTA(t *testing.T, txn uint64, gid int64, tenant, sess string, cost float64, usage llm.Usage) EventRow {
	t.Helper()
	p, _ := json.Marshal(domain.TurnAborted{TurnID: "ta", Reason: domain.ErrorDuringExecution, UsageSoFar: usage, CostUSD: cost})
	return EventRow{TransactionID: txn, GlobalID: gid, TenantID: tenant, SessionID: sess, Type: domain.EventTurnAborted, Payload: p}
}

func rowOther(t *testing.T, txn uint64, gid int64, tenant, sess string) EventRow {
	t.Helper()
	p, _ := json.Marshal(domain.TurnStarted{TurnID: "ts", Model: "m"})
	return EventRow{TransactionID: txn, GlobalID: gid, TenantID: tenant, SessionID: sess, Type: domain.EventTurnStarted, Payload: p}
}

func contains(s, sub string) bool { return len(s) >= len(sub) && indexOf(s, sub) >= 0 }

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func mustParseUint(s string) uint64 {
	v, err := textToUint64(s)
	if err != nil {
		panic(err)
	}
	return v
}

// compile-time: fakeConn satisfies Conn and fakeMetrics satisfies MetricSink.
var (
	_ Conn       = (*fakeConn)(nil)
	_ MetricSink = (*fakeMetrics)(nil)
)
