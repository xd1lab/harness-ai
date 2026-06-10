package dedup

// This file unit-tests the dedup ledger's error-classification and fail-closed
// branches that the Docker integration suite only crosses on its happy paths:
// the duplicate-key race tolerance in Begin, the lost-record path in Complete,
// the SQLSTATE 23505 classifier, the fail-closed tenant transaction setup, and
// the result_ref decode failures. No database is used — the [Pool],
// [PooledConn] and pgx.Tx surfaces are faked in-memory so every branch is
// provoked deterministically.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// ---------------------------------------------------------------------------
// In-memory fakes for Pool / PooledConn / pgx.Tx / pgx.Row (mirroring the
// fakes in the eventstore adapter's unit tests; the two stores share idioms).
// ---------------------------------------------------------------------------

// stmtStub is one scripted response of [fakeTx]: the first stub whose match is
// a substring of the statement SQL answers it. Unmatched Exec succeeds benignly
// (so tests only script what they assert on); an unmatched QueryRow fails
// loudly, since readRecord depends on its scanned values.
type stmtStub struct {
	match string                  // substring of the SQL this stub answers
	tag   pgconn.CommandTag       // Exec result tag (zero tag => 0 rows affected)
	err   error                   // error for Exec / QueryRow's Scan
	scan  func(dest ...any) error // QueryRow scan behavior
}

// fakeTx implements pgx.Tx over a stub script, recording routed SQL and
// commit/rollback counts for the fail-closed assertions.
type fakeTx struct {
	stubs     []stmtStub
	commitErr error
	commits   int
	rollbacks int
	executed  []string
}

func (tx *fakeTx) find(sql string) *stmtStub {
	for i := range tx.stubs {
		if strings.Contains(sql, tx.stubs[i].match) {
			return &tx.stubs[i]
		}
	}
	return nil
}

func (tx *fakeTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	tx.executed = append(tx.executed, sql)
	if st := tx.find(sql); st != nil {
		return st.tag, st.err
	}
	return pgconn.NewCommandTag("OK 1"), nil
}

func (tx *fakeTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	tx.executed = append(tx.executed, sql)
	if st := tx.find(sql); st != nil {
		if st.err != nil {
			return fakeRow{err: st.err}
		}
		return fakeRow{scan: st.scan}
	}
	return fakeRow{err: fmt.Errorf("fakeTx: no stub for QueryRow %q", sql)}
}

func (tx *fakeTx) Commit(context.Context) error   { tx.commits++; return tx.commitErr }
func (tx *fakeTx) Rollback(context.Context) error { tx.rollbacks++; return nil }

// The store never uses the remaining pgx.Tx surface; fail loudly if that drifts.
func (tx *fakeTx) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("fakeTx: nested Begin not supported")
}

func (tx *fakeTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("fakeTx: Query not supported")
}

func (tx *fakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("fakeTx: CopyFrom not supported")
}
func (tx *fakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (tx *fakeTx) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (tx *fakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("fakeTx: Prepare not supported")
}
func (tx *fakeTx) Conn() *pgx.Conn { return nil }

// fakeRow implements pgx.Row: a fixed Scan error (e.g. pgx.ErrNoRows) or a
// scripted scan that fills the dests.
type fakeRow struct {
	err  error
	scan func(dest ...any) error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.scan != nil {
		return r.scan(dest...)
	}
	return nil
}

// fakeConn / fakePool implement PooledConn / Pool around one fakeTx, recording
// acquire/release counts so fail-closed paths can assert no connection leaked.
type fakeConn struct {
	tx       pgx.Tx
	beginErr error
	released int
}

func (c *fakeConn) Begin(context.Context) (pgx.Tx, error) {
	if c.beginErr != nil {
		return nil, c.beginErr
	}
	return c.tx, nil
}
func (c *fakeConn) Release() { c.released++ }

type fakePool struct {
	conn       *fakeConn
	acquireErr error
	acquires   int
}

func (p *fakePool) Acquire(context.Context) (PooledConn, error) {
	p.acquires++
	if p.acquireErr != nil {
		return nil, p.acquireErr
	}
	return p.conn, nil
}
func (p *fakePool) Close() {}

// newFakeStore wires a Store over a single scripted transaction.
func newFakeStore(tx *fakeTx) (*Store, *fakePool, *fakeConn) {
	conn := &fakeConn{tx: tx}
	pool := &fakePool{conn: conn}
	return New(pool), pool, conn
}

// pgErrWithCode builds a *pgconn.PgError with the given SQLSTATE, the shape pgx
// surfaces server errors in.
func pgErrWithCode(code string) *pgconn.PgError {
	return &pgconn.PgError{Code: code, Message: "stubbed server error"}
}

// readRecordStub scripts the readRecord SELECT to return (status, result_ref);
// pass resultRef == nil for a NULL column.
func readRecordStub(status string, resultRef *string) stmtStub {
	return stmtStub{
		match: "SELECT status, result_ref",
		scan: func(dest ...any) error {
			*(dest[0].(*string)) = status
			*(dest[1].(**string)) = resultRef
			return nil
		},
	}
}

// strPtr returns a pointer to s (for result_ref values).
func strPtr(s string) *string { return &s }

// startedRec is a minimal valid Begin/Complete input for the fixed test key.
func startedRec() app.ExecutionRecord {
	return app.ExecutionRecord{
		TenantID:       "tenant-1",
		SessionID:      "sess-1",
		IdempotencyKey: "key-1",
	}
}

// ---------------------------------------------------------------------------
// isUniqueViolation: the SQLSTATE classifier (0% in the integration runs —
// the happy-path suites never trip it).
// ---------------------------------------------------------------------------

// TestIsUniqueViolation_Classification asserts only SQLSTATE 23505 matches,
// through wrapping, and that nil / non-pg errors are safely non-matches.
func TestIsUniqueViolation_Classification(t *testing.T) {
	t.Parallel()
	if isUniqueViolation(nil) {
		t.Error("isUniqueViolation(nil) must be false")
	}
	if isUniqueViolation(errors.New("plain")) {
		t.Error("isUniqueViolation(plain error) must be false")
	}
	if !isUniqueViolation(pgErrWithCode("23505")) {
		t.Error("bare 23505 PgError must classify as unique violation")
	}
	if !isUniqueViolation(fmt.Errorf("insert: %w", pgErrWithCode("23505"))) {
		t.Error("wrapped 23505 PgError must classify via errors.As")
	}
	if isUniqueViolation(pgErrWithCode("40001")) {
		t.Error("a serialization failure (40001) must NOT classify as unique violation")
	}
}

// ---------------------------------------------------------------------------
// beginTenantTx: fail-closed acquisition.
// ---------------------------------------------------------------------------

// TestBeginTenantTx_FailClosed walks every acquisition failure: an empty
// tenant must fail BEFORE any connection is acquired (the public methods guard
// this too, but beginTenantTx is the defense-in-depth check), and each later
// failure must release exactly what it acquired.
func TestBeginTenantTx_FailClosed(t *testing.T) {
	t.Parallel()
	bg := context.Background()

	t.Run("empty tenant", func(t *testing.T) {
		t.Parallel()
		store, pool, _ := newFakeStore(&fakeTx{})
		_, _, err := store.beginTenantTx(bg, "")
		if err == nil || !strings.Contains(err.Error(), "fail-closed") {
			t.Fatalf("err = %v, want fail-closed empty-tenant rejection", err)
		}
		if pool.acquires != 0 {
			t.Errorf("acquired %d connections before the tenant check; fail-closed must acquire none", pool.acquires)
		}
	})

	t.Run("acquire error", func(t *testing.T) {
		t.Parallel()
		store, pool, _ := newFakeStore(&fakeTx{})
		pool.acquireErr = errors.New("pool exhausted")
		if _, _, err := store.beginTenantTx(bg, "tenant-1"); err == nil || !strings.Contains(err.Error(), "pool exhausted") {
			t.Fatalf("err = %v, want acquire error surfaced", err)
		}
	})

	t.Run("begin error releases the connection", func(t *testing.T) {
		t.Parallel()
		store, _, conn := newFakeStore(&fakeTx{})
		conn.beginErr = errors.New("backend down")
		if _, _, err := store.beginTenantTx(bg, "tenant-1"); err == nil || !strings.Contains(err.Error(), "begin tx") {
			t.Fatalf("err = %v, want wrapped begin error", err)
		}
		if conn.released != 1 {
			t.Errorf("released = %d, want 1 (no leaked connection on Begin failure)", conn.released)
		}
	})

	// A canceled context surfaces as the SET LOCAL Exec failing — the same
	// branch a server-side error takes; both must roll back AND release.
	t.Run("set_config error rolls back and releases", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{{match: "set_config", err: context.Canceled}}}
		store, _, conn := newFakeStore(tx)
		if _, _, err := store.beginTenantTx(bg, "tenant-1"); err == nil || !strings.Contains(err.Error(), "setting tenant GUC") {
			t.Fatalf("err = %v, want wrapped set_config error", err)
		}
		if tx.rollbacks != 1 || conn.released != 1 {
			t.Errorf("rollbacks = %d, released = %d; want 1 and 1", tx.rollbacks, conn.released)
		}
	})

	t.Run("success: cleanup rolls back and releases once", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{}
		store, _, conn := newFakeStore(tx)
		gotTx, cleanup, err := store.beginTenantTx(bg, "tenant-1")
		if err != nil {
			t.Fatalf("beginTenantTx: %v", err)
		}
		if gotTx != pgx.Tx(tx) {
			t.Fatal("beginTenantTx returned a different tx than the connection began")
		}
		cleanup()
		if tx.rollbacks != 1 || conn.released != 1 {
			t.Errorf("after cleanup: rollbacks = %d, released = %d; want 1 and 1", tx.rollbacks, conn.released)
		}
	})
}

// TestPublicMethods_AcquireFailureSurfaces asserts each public method aborts
// (with no statements run) when the pool cannot supply a connection — the
// beginTenantTx error return inside Begin / Complete / Lookup.
func TestPublicMethods_AcquireFailureSurfaces(t *testing.T) {
	t.Parallel()

	calls := map[string]func(*Store) error{
		"Begin": func(s *Store) error {
			_, err := s.Begin(context.Background(), startedRec())
			return err
		},
		"Complete": func(s *Store) error {
			rec := startedRec()
			rec.Status = app.ExecFailed
			return s.Complete(context.Background(), rec)
		},
		"Lookup": func(s *Store) error {
			_, err := s.Lookup(context.Background(), "tenant-1", "sess-1", "key-1")
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tx := &fakeTx{}
			store, pool, _ := newFakeStore(tx)
			pool.acquireErr = errors.New("pool exhausted")
			if err := call(store); err == nil || !strings.Contains(err.Error(), "pool exhausted") {
				t.Fatalf("%s: err = %v, want acquire error surfaced", name, err)
			}
			if len(tx.executed) != 0 {
				t.Errorf("%s ran statements after a failed acquire: %v", name, tx.executed)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Begin: the INSERT … ON CONFLICT race and its read-back.
// ---------------------------------------------------------------------------

// TestBegin_DuplicateKeyRaceTolerated pins the FR-TOOL-04 AC-2 race contract at
// the unit level: when the INSERT surfaces a unique violation (a writer that
// beat us to the PRIMARY KEY outside ON CONFLICT's reach, e.g. under a stricter
// isolation level), Begin must NOT error — it falls through to the read-back
// and returns the winner's record.
func TestBegin_DuplicateKeyRaceTolerated(t *testing.T) {
	t.Parallel()

	winner, err := json.Marshal(domain.Observation{Content: "winner output"})
	if err != nil {
		t.Fatalf("marshal observation: %v", err)
	}
	tx := &fakeTx{stubs: []stmtStub{
		{match: "INSERT INTO tool_executions", err: pgErrWithCode("23505")},
		readRecordStub(string(app.ExecCompleted), strPtr(string(winner))),
	}}
	store, _, _ := newFakeStore(tx)

	got, err := store.Begin(context.Background(), startedRec())
	if err != nil {
		t.Fatalf("Begin must tolerate the duplicate-key race, got %v", err)
	}
	if got.Status != app.ExecCompleted || got.Result.Content != "winner output" {
		t.Fatalf("Begin returned %+v, want the racing winner's completed record", got)
	}
	if tx.commits != 1 {
		t.Errorf("commits = %d, want 1 (the read-back still commits)", tx.commits)
	}
}

// TestBegin_ErrorPaths covers the remaining Begin failures: the empty-tenant
// guard, a non-23505 insert failure, a missing read-back row, and a commit
// failure.
func TestBegin_ErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("empty tenant rejected before any tx", func(t *testing.T) {
		t.Parallel()
		store, pool, _ := newFakeStore(&fakeTx{})
		rec := startedRec()
		rec.TenantID = ""
		if _, err := store.Begin(context.Background(), rec); err == nil || !strings.Contains(err.Error(), "TenantID must not be empty") {
			t.Fatalf("err = %v, want empty-tenant rejection", err)
		}
		if pool.acquires != 0 {
			t.Errorf("acquires = %d, want 0", pool.acquires)
		}
	})

	t.Run("non-unique insert error fails", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{{match: "INSERT INTO tool_executions", err: errors.New("conn reset")}}}
		store, _, _ := newFakeStore(tx)
		if _, err := store.Begin(context.Background(), startedRec()); err == nil || !strings.Contains(err.Error(), "Begin insert") {
			t.Fatalf("err = %v, want wrapped insert error", err)
		}
		if tx.commits != 0 {
			t.Errorf("commits = %d, want 0", tx.commits)
		}
	})

	t.Run("read-back missing row surfaces ErrNotFound", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{{match: "SELECT status, result_ref", err: pgx.ErrNoRows}}}
		store, _, _ := newFakeStore(tx)
		_, err := store.Begin(context.Background(), startedRec())
		if err == nil || !strings.Contains(err.Error(), "Begin read-back") {
			t.Fatalf("err = %v, want wrapped read-back error", err)
		}
		// The sentinel must survive the wrap so recovery can branch on it.
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("err = %v must wrap ErrNotFound", err)
		}
	})

	t.Run("commit failure wraps", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{
			stubs:     []stmtStub{readRecordStub(string(app.ExecStarted), nil)},
			commitErr: errors.New("conn lost"),
		}
		store, _, _ := newFakeStore(tx)
		if _, err := store.Begin(context.Background(), startedRec()); err == nil || !strings.Contains(err.Error(), "Begin commit") {
			t.Fatalf("err = %v, want wrapped commit error", err)
		}
	})
}

// TestBegin_SuccessReturnsReadBack asserts the normal path returns the row the
// read-back observed (here: started, no result yet) and commits exactly once.
func TestBegin_SuccessReturnsReadBack(t *testing.T) {
	t.Parallel()
	tx := &fakeTx{stubs: []stmtStub{readRecordStub(string(app.ExecStarted), nil)}}
	store, _, conn := newFakeStore(tx)

	got, err := store.Begin(context.Background(), startedRec())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	want := startedRec()
	if got.TenantID != want.TenantID || got.SessionID != want.SessionID || got.IdempotencyKey != want.IdempotencyKey {
		t.Errorf("Begin key = (%s,%s,%s), want the input key echoed", got.TenantID, got.SessionID, got.IdempotencyKey)
	}
	if got.Status != app.ExecStarted || got.Result != (domain.Observation{}) {
		t.Errorf("Begin record = %+v, want fresh started record with zero Result", got)
	}
	if tx.commits != 1 || conn.released != 1 {
		t.Errorf("commits = %d, released = %d; want 1 and 1", tx.commits, conn.released)
	}
}

// ---------------------------------------------------------------------------
// Complete: terminal-status recording, including the lost-record path.
// ---------------------------------------------------------------------------

// TestComplete covers the empty-tenant guard, the update/commit failures, the
// 0-rows lost-record path (the ledger row vanished between Begin and Complete —
// a bug or manual intervention that must fail loudly, never silently succeed),
// and the success path.
func TestComplete(t *testing.T) {
	t.Parallel()

	completed := func() app.ExecutionRecord {
		rec := startedRec()
		rec.Status = app.ExecCompleted
		rec.Result = domain.Observation{Content: "ok"}
		return rec
	}

	t.Run("empty tenant rejected before any tx", func(t *testing.T) {
		t.Parallel()
		store, pool, _ := newFakeStore(&fakeTx{})
		rec := completed()
		rec.TenantID = ""
		if err := store.Complete(context.Background(), rec); err == nil || !strings.Contains(err.Error(), "TenantID must not be empty") {
			t.Fatalf("err = %v, want empty-tenant rejection", err)
		}
		if pool.acquires != 0 {
			t.Errorf("acquires = %d, want 0", pool.acquires)
		}
	})

	t.Run("update failure wraps", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{{match: "UPDATE tool_executions", err: errors.New("conn reset")}}}
		store, _, _ := newFakeStore(tx)
		if err := store.Complete(context.Background(), completed()); err == nil || !strings.Contains(err.Error(), "Complete update") {
			t.Fatalf("err = %v, want wrapped update error", err)
		}
	})

	t.Run("lost record fails loudly with the key", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{{match: "UPDATE tool_executions", tag: pgconn.NewCommandTag("UPDATE 0")}}}
		store, _, _ := newFakeStore(tx)
		err := store.Complete(context.Background(), completed())
		if err == nil || !strings.Contains(err.Error(), "no row found") {
			t.Fatalf("err = %v, want lost-record error", err)
		}
		// The error must carry the full key so the UNKNOWN-outcome adjudication
		// has something to act on.
		for _, part := range []string{"tenant-1", "sess-1", "key-1"} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("lost-record error %q is missing key part %q", err, part)
			}
		}
		if tx.commits != 0 {
			t.Errorf("commits = %d, want 0 (nothing to persist for a lost record)", tx.commits)
		}
	})

	t.Run("commit failure wraps", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{
			stubs:     []stmtStub{{match: "UPDATE tool_executions", tag: pgconn.NewCommandTag("UPDATE 1")}},
			commitErr: errors.New("conn lost"),
		}
		store, _, _ := newFakeStore(tx)
		if err := store.Complete(context.Background(), completed()); err == nil || !strings.Contains(err.Error(), "Complete commit") {
			t.Fatalf("err = %v, want wrapped commit error", err)
		}
	})

	t.Run("success commits once", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{{match: "UPDATE tool_executions", tag: pgconn.NewCommandTag("UPDATE 1")}}}
		store, _, conn := newFakeStore(tx)
		if err := store.Complete(context.Background(), completed()); err != nil {
			t.Fatalf("Complete: %v", err)
		}
		if tx.commits != 1 || conn.released != 1 {
			t.Errorf("commits = %d, released = %d; want 1 and 1", tx.commits, conn.released)
		}
	})
}

// ---------------------------------------------------------------------------
// Lookup / readRecord: not-found, decode failures, observation round-trip.
// ---------------------------------------------------------------------------

// TestLookup_ReadRecordBranches covers each readRecord outcome through the
// public Lookup: the ErrNotFound sentinel, a scan failure, a corrupted
// result_ref, NULL/empty result_ref (no observation), and a full observation
// round-trip.
func TestLookup_ReadRecordBranches(t *testing.T) {
	t.Parallel()
	bg := context.Background()

	t.Run("empty tenant rejected", func(t *testing.T) {
		t.Parallel()
		store, _, _ := newFakeStore(&fakeTx{})
		if _, err := store.Lookup(bg, "", "sess-1", "key-1"); err == nil || !strings.Contains(err.Error(), "tenantID must not be empty") {
			t.Fatalf("err = %v, want empty-tenant rejection", err)
		}
	})

	t.Run("no row is ErrNotFound", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{{match: "SELECT status, result_ref", err: pgx.ErrNoRows}}}
		store, _, _ := newFakeStore(tx)
		_, err := store.Lookup(bg, "tenant-1", "sess-1", "key-1")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want errors.Is(ErrNotFound)", err)
		}
	})

	t.Run("scan failure wraps", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{{match: "SELECT status, result_ref", err: errors.New("conn reset")}}}
		store, _, _ := newFakeStore(tx)
		_, err := store.Lookup(bg, "tenant-1", "sess-1", "key-1")
		if err == nil || !strings.Contains(err.Error(), "reading record") {
			t.Fatalf("err = %v, want wrapped read error", err)
		}
		if errors.Is(err, ErrNotFound) {
			t.Error("a transport failure must not classify as not-found")
		}
	})

	t.Run("corrupted result_ref fails loudly", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{readRecordStub(string(app.ExecCompleted), strPtr("{not json"))}}
		store, _, _ := newFakeStore(tx)
		if _, err := store.Lookup(bg, "tenant-1", "sess-1", "key-1"); err == nil || !strings.Contains(err.Error(), "unmarshaling result_ref") {
			t.Fatalf("err = %v, want result_ref decode error", err)
		}
	})

	t.Run("NULL and empty result_ref mean no observation", func(t *testing.T) {
		t.Parallel()
		for name, ref := range map[string]*string{"NULL": nil, "empty string": strPtr("")} {
			tx := &fakeTx{stubs: []stmtStub{readRecordStub(string(app.ExecStarted), ref)}}
			store, _, _ := newFakeStore(tx)
			rec, err := store.Lookup(bg, "tenant-1", "sess-1", "key-1")
			if err != nil {
				t.Fatalf("%s result_ref: Lookup: %v", name, err)
			}
			if rec.Result != (domain.Observation{}) {
				t.Errorf("%s result_ref: Result = %+v, want zero observation", name, rec.Result)
			}
			if rec.Status != app.ExecStarted {
				t.Errorf("%s result_ref: Status = %q, want started", name, rec.Status)
			}
		}
	})

	t.Run("observation round-trips all fields", func(t *testing.T) {
		t.Parallel()
		want := domain.Observation{Content: "out", IsError: true, Truncated: true, BlobRef: "sha256:abc"}
		raw, err := json.Marshal(want)
		if err != nil {
			t.Fatalf("marshal observation: %v", err)
		}
		tx := &fakeTx{stubs: []stmtStub{readRecordStub(string(app.ExecUnknown), strPtr(string(raw)))}}
		store, _, _ := newFakeStore(tx)
		rec, err := store.Lookup(bg, "tenant-1", "sess-1", "key-1")
		if err != nil {
			t.Fatalf("Lookup: %v", err)
		}
		if rec.Result != want {
			t.Errorf("Result = %+v, want %+v", rec.Result, want)
		}
		if rec.Status != app.ExecUnknown {
			t.Errorf("Status = %q, want unknown", rec.Status)
		}
		if tx.commits != 0 {
			t.Errorf("commits = %d; Lookup is read-only and relies on the deferred rollback", tx.commits)
		}
	})
}

// ---------------------------------------------------------------------------
// SimplePool construction and lifecycle.
// ---------------------------------------------------------------------------

// TestNewSimplePool_BadDSN asserts a malformed DSN is rejected at construction
// (no connection is opened here, so this is the only constructor-time failure).
func TestNewSimplePool_BadDSN(t *testing.T) {
	t.Parallel()
	if _, err := NewSimplePool("://not a dsn"); err == nil {
		t.Fatal("NewSimplePool should reject a malformed DSN")
	}
}

// TestSimplePoolCloseIsNoOp pins the documented contract that SimplePool.Close
// is a no-op (it holds no long-lived connections), so callers may defer it
// unconditionally.
func TestSimplePoolCloseIsNoOp(t *testing.T) {
	t.Parallel()
	pool, err := NewSimplePool("postgres://app@localhost:5432/boltrope")
	if err != nil {
		t.Fatalf("NewSimplePool: %v", err)
	}
	pool.Close()
	pool.Close() // idempotent by construction
}
