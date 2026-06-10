package projection

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/boltrope/boltrope/internal/orchestrator/domain"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// errConn wraps fakeConn with per-statement error injection so the runner's
// failure branches (ensure / load-cursor / save-cursor / lag) are reachable one
// at a time while every other statement behaves normally.
type errConn struct {
	*fakeConn
	ensureErr error // fails EnsureSubscription's INSERT
	loadErr   error // fails LoadCursor's SELECT (a non-ErrNoRows failure)
	saveErr   error // fails SaveCursor's UPDATE
	lagErr    error // fails Lag's COUNT(*)
}

func (c *errConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if c.ensureErr != nil && contains(sql, "INSERT INTO event_subscriptions") {
		return pgconn.CommandTag{}, c.ensureErr
	}
	if c.saveErr != nil && contains(sql, "UPDATE event_subscriptions") {
		return pgconn.CommandTag{}, c.saveErr
	}
	return c.fakeConn.Exec(ctx, sql, args...)
}

func (c *errConn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if c.loadErr != nil && contains(sql, "FROM event_subscriptions") {
		return fakeRow{err: c.loadErr}
	}
	if c.lagErr != nil && contains(sql, "COUNT(*)") {
		return fakeRow{err: c.lagErr}
	}
	return c.fakeConn.QueryRow(ctx, sql, args...)
}

// persistedCursor reads the fake's saved subscription cursor under the lock
// (the Run goroutine saves it concurrently in lifecycle tests).
func (c *fakeConn) persistedCursor() Cursor {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Cursor{TransactionID: c.cursorTxn, GlobalID: c.cursorGID}
}

// appendEvents appends newly "settled" rows to the fake feed under the lock.
func (c *fakeConn) appendEvents(rows ...EventRow) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, rows...)
}

// waitUntil polls cond until it is true or the deadline passes, fatally failing
// the test on timeout. Run's tickers are real-time (the runner deliberately
// owns its own tickers; only the sweeper cutoff clock is injected), so the
// lifecycle tests wait on observable state instead of sleeping fixed amounts.
func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", d, what)
}

// TestRunner_Run_FullLifecycle drives Run end to end over a fake feed: the
// initial catch-up, a waker hint, the degrade-to-polling path when the waker
// channel closes, and the ctx.Err() return on cancellation. The rollup, the
// persisted cursor, and the published metrics must all reflect every event
// exactly once.
func TestRunner_Run_FullLifecycle(t *testing.T) {
	const tenant, sess = "tenant-a", "sess-1"
	conn := &fakeConn{events: []EventRow{
		rowTF(t, 100, 1, tenant, sess, 0.10, llm.Usage{InputTokens: 10}),
		rowTF(t, 101, 2, tenant, sess, 0.20, llm.Usage{OutputTokens: 5}),
	}}
	metrics := &fakeMetrics{}
	var logBuf bytes.Buffer
	r := NewRunner(
		Config{Subscription: "cost-rollup", BatchSize: 10, PollInterval: 15 * time.Millisecond},
		NewSource(conn),
		WithMetrics(metrics),
		WithLogger(slog.New(slog.NewTextHandler(&logBuf, nil))),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waker := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, waker) }()

	// Initial catch-up before any wakeup: both seeded events fold and the
	// cursor persists at the last row.
	waitUntil(t, 5*time.Second, "initial catch-up", func() bool {
		return conn.persistedCursor() == Cursor{TransactionID: 101, GlobalID: 2}
	})

	// A waker hint picks up a newly settled event (LISTEN/NOTIFY fast path).
	conn.appendEvents(rowTF(t, 102, 3, tenant, sess, 0.30, llm.Usage{}))
	waker <- struct{}{}
	waitUntil(t, 5*time.Second, "waker-driven catch-up", func() bool {
		return conn.persistedCursor() == Cursor{TransactionID: 102, GlobalID: 3}
	})

	// Closing the waker (listener died) must degrade to pure polling, not kill
	// the worker: the next event is picked up by the poll tick alone.
	close(waker)
	conn.appendEvents(rowTF(t, 103, 4, tenant, sess, 0.40, llm.Usage{}))
	waitUntil(t, 5*time.Second, "poll-driven catch-up after waker close", func() bool {
		return conn.persistedCursor() == Cursor{TransactionID: 103, GlobalID: 4}
	})

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	// Every event folded exactly once: 0.10+0.20+0.30+0.40 over 4 turns.
	got := r.Totals()[SessionKey{TenantID: tenant, SessionID: sess}]
	if !floatEq(got.CostUSD, 1.00) || got.Turns != 4 {
		t.Fatalf("rollup = %+v, want cost 1.00 / 4 turns", got)
	}
	if !floatEq(metrics.costTotal, 1.00) {
		t.Fatalf("cost counter total = %v, want 1.00", metrics.costTotal)
	}
	if metrics.lastLag != 0 || metrics.lagSet == 0 {
		t.Fatalf("lag last=%d setCount=%d, want last 0 and >=1 publishes", metrics.lastLag, metrics.lagSet)
	}
	if !strings.Contains(logBuf.String(), "projection worker starting") {
		t.Fatalf("startup log line missing from logger output:\n%s", logBuf.String())
	}
}

// TestRunner_Run_StartupErrors asserts the two fatal start conditions: a failed
// EnsureSubscription and a failed cursor load both end Run with a wrapped error
// (the worker cannot safely begin without its durable cursor).
func TestRunner_Run_StartupErrors(t *testing.T) {
	t.Run("ensure subscription fails", func(t *testing.T) {
		conn := &errConn{fakeConn: &fakeConn{}, ensureErr: errors.New("ensure blew up")}
		r := NewRunner(Config{Subscription: "cost-rollup"}, NewSource(conn))
		err := r.Run(context.Background(), nil)
		if err == nil || !strings.Contains(err.Error(), "ensuring subscription") {
			t.Fatalf("Run = %v, want ensuring-subscription error", err)
		}
	})
	t.Run("cursor load fails", func(t *testing.T) {
		conn := &errConn{fakeConn: &fakeConn{}, loadErr: errors.New("load blew up")}
		r := NewRunner(Config{Subscription: "cost-rollup"}, NewSource(conn))
		err := r.Run(context.Background(), nil)
		if err == nil || !strings.Contains(err.Error(), "loading cursor") {
			t.Fatalf("Run = %v, want loading-cursor error", err)
		}
	})
}

// TestRunner_RunOnce_Errors covers runOnce's own error returns: the ensure and
// load failures propagate, and an already-cancelled context is returned as
// ctx.Err() after the (no-op) catch-up.
func TestRunner_RunOnce_Errors(t *testing.T) {
	t.Run("ensure subscription fails", func(t *testing.T) {
		conn := &errConn{fakeConn: &fakeConn{}, ensureErr: errors.New("ensure blew up")}
		r := NewRunner(Config{Subscription: "cost-rollup"}, NewSource(conn))
		if err := r.runOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "ensuring subscription") {
			t.Fatalf("runOnce = %v, want ensuring-subscription error", err)
		}
	})
	t.Run("cursor load fails", func(t *testing.T) {
		conn := &errConn{fakeConn: &fakeConn{}, loadErr: errors.New("load blew up")}
		r := NewRunner(Config{Subscription: "cost-rollup"}, NewSource(conn))
		if err := r.runOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "loading cursor") {
			t.Fatalf("runOnce = %v, want loading-cursor error", err)
		}
	})
	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r := NewRunner(Config{Subscription: "cost-rollup"}, NewSource(&fakeConn{}))
		if err := r.runOnce(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("runOnce on cancelled ctx = %v, want context.Canceled", err)
		}
	})
}

// TestRunner_FoldErrorDoesNotAdvanceCursor asserts a malformed turn-terminal
// payload (a hard RollupFold error) is swallowed by catchUp for retry and the
// cursor does NOT advance — the batch is re-read next tick rather than skipped.
func TestRunner_FoldErrorDoesNotAdvanceCursor(t *testing.T) {
	conn := &fakeConn{events: []EventRow{
		{TransactionID: 100, GlobalID: 1, TenantID: "t", SessionID: "s", Type: domain.EventTurnFinished, Payload: []byte("{not json")},
	}}
	metrics := &fakeMetrics{}
	r := NewRunner(Config{Subscription: "cost-rollup", BatchSize: 10}, NewSource(conn), WithMetrics(metrics))
	if err := r.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce = %v, want nil (fold error is retried, not fatal)", err)
	}
	if cur := conn.persistedCursor(); cur != (Cursor{}) {
		t.Fatalf("cursor advanced to %s over a failed fold, want unchanged zero cursor", cur)
	}
	if metrics.costTotal != 0 {
		t.Fatalf("cost counter total = %v after failed fold, want 0", metrics.costTotal)
	}
}

// TestRunner_SaveCursorErrorDoesNotAdvanceOrEmitCost asserts foldAndAdvance's
// ordering contract: when the cursor save fails, the in-memory cursor stays put
// and NO cost delta is emitted (the counter add happens strictly after the
// save, so a re-read re-emits at most a delta that was never counted).
func TestRunner_SaveCursorErrorDoesNotAdvanceOrEmitCost(t *testing.T) {
	conn := &errConn{
		fakeConn: &fakeConn{events: []EventRow{rowTF(t, 100, 1, "t", "s", 0.10, llm.Usage{})}},
		saveErr:  errors.New("save blew up"),
	}
	metrics := &fakeMetrics{}
	r := NewRunner(Config{Subscription: "cost-rollup", BatchSize: 10}, NewSource(conn), WithMetrics(metrics))
	if err := r.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce = %v, want nil (save error is retried, not fatal)", err)
	}
	if r.cursor != (Cursor{}) {
		t.Fatalf("in-memory cursor = %s after failed save, want unchanged zero cursor", r.cursor)
	}
	if len(metrics.costAdds) != 0 {
		t.Fatalf("cost adds = %v after failed save, want none (delta only after a saved cursor)", metrics.costAdds)
	}
}

// TestRunner_PublishLagErrorSkipsSink asserts a failed lag read is logged and
// the sink is NOT updated (no stale/zero gauge publish on a read failure).
func TestRunner_PublishLagErrorSkipsSink(t *testing.T) {
	conn := &errConn{fakeConn: &fakeConn{}, lagErr: errors.New("lag blew up")}
	metrics := &fakeMetrics{}
	var buf bytes.Buffer
	r := NewRunner(Config{Subscription: "cost-rollup"}, NewSource(conn),
		WithMetrics(metrics), WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
	if err := r.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce = %v, want nil", err)
	}
	if metrics.lagSet != 0 {
		t.Fatalf("lag gauge was set %d times after a failed lag read, want 0", metrics.lagSet)
	}
	if !strings.Contains(buf.String(), "projection lag read failed") {
		t.Fatalf("lag failure was not logged:\n%s", buf.String())
	}
}

// TestNewRunner_PanicsWithoutSubscription pins the wiring-time contract: an
// empty Config.Subscription is a programming error, not a runtime condition.
func TestNewRunner_PanicsWithoutSubscription(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewRunner accepted an empty Subscription, want panic")
		}
	}()
	_ = NewRunner(Config{}, NewSource(&fakeConn{}))
}

// TestConfig_WithDefaults covers both directions of the default fill: zero
// fields get the documented defaults, set fields are kept.
func TestConfig_WithDefaults(t *testing.T) {
	got := Config{Subscription: "s"}.withDefaults()
	if got.BatchSize != 500 || got.PollInterval != time.Second {
		t.Fatalf("zero-config defaults = (batch=%d, poll=%s), want (500, 1s)", got.BatchSize, got.PollInterval)
	}
	kept := Config{Subscription: "s", BatchSize: 7, PollInterval: 3 * time.Second}.withDefaults()
	if kept.BatchSize != 7 || kept.PollInterval != 3*time.Second {
		t.Fatalf("explicit config was overwritten: %+v", kept)
	}
}

// TestRunnerOptions covers the option setters' guard clauses: nil logger/now
// keep the defaults, a real logger is actually used, and a custom now feeds the
// runner's clock.
func TestRunnerOptions(t *testing.T) {
	t.Run("nil logger and nil now keep defaults", func(t *testing.T) {
		r := NewRunner(Config{Subscription: "s"}, NewSource(&fakeConn{}), WithLogger(nil), WithNow(nil))
		if _, ok := r.log.Handler().(discardHandler); !ok {
			t.Fatalf("WithLogger(nil) replaced the discard handler with %T", r.log.Handler())
		}
		if r.now == nil || r.now().IsZero() {
			t.Fatal("WithNow(nil) clobbered the default clock")
		}
	})
	t.Run("custom now is used", func(t *testing.T) {
		fixed := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
		r := NewRunner(Config{Subscription: "s"}, NewSource(&fakeConn{}), WithNow(func() time.Time { return fixed }))
		if !r.now().Equal(fixed) {
			t.Fatalf("r.now() = %s, want the injected fixed time %s", r.now(), fixed)
		}
	})
	t.Run("custom logger receives the fetch warning", func(t *testing.T) {
		var buf bytes.Buffer
		conn := &fakeConn{fetchErr: errors.New("transient blip")}
		r := NewRunner(Config{Subscription: "s"}, NewSource(conn),
			WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
		if err := r.runOnce(context.Background()); err != nil {
			t.Fatalf("runOnce = %v, want nil", err)
		}
		if !strings.Contains(buf.String(), "projection fetch failed") {
			t.Fatalf("fetch warning missing from logger output:\n%s", buf.String())
		}
	})
}

// TestSessionKey_String pins the tenant/session log rendering.
func TestSessionKey_String(t *testing.T) {
	k := SessionKey{TenantID: "tenant-a", SessionID: "sess-1"}
	if got, want := k.String(), "tenant-a/sess-1"; got != want {
		t.Fatalf("SessionKey.String() = %q, want %q", got, want)
	}
}

// TestRollupFold_MalformedTurnAbortedIsError mirrors the TurnFinished malformed
// test for the OTHER cost-bearing type: a corrupt TurnAborted payload is a hard
// error too (partial turns must be billed, never silently dropped).
func TestRollupFold_MalformedTurnAbortedIsError(t *testing.T) {
	rows := []EventRow{
		{TransactionID: 1, GlobalID: 1, TenantID: "t", SessionID: "s", Type: domain.EventTurnAborted, Payload: []byte("{not json")},
	}
	if _, err := RollupFold(nil, rows); err == nil {
		t.Fatal("RollupFold accepted a malformed TurnAborted payload, want error")
	}
}

// TestDiscardHandler_NoOps pins the silent-by-default logger contract: the
// fallback handler never errors and returns itself for derived handlers.
func TestDiscardHandler_NoOps(t *testing.T) {
	d := discardHandler{}
	if err := d.Handle(context.Background(), slog.Record{}); err != nil {
		t.Fatalf("Handle = %v, want nil", err)
	}
	if _, ok := d.WithAttrs(nil).(discardHandler); !ok {
		t.Fatal("WithAttrs did not return the discard handler")
	}
	if _, ok := d.WithGroup("g").(discardHandler); !ok {
		t.Fatal("WithGroup did not return the discard handler")
	}
}
