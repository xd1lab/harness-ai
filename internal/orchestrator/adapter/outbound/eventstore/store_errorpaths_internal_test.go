package eventstore

// This file unit-tests the error-classification and fail-closed branches the
// Docker integration suite exercises only on its happy paths: the append
// transaction's gate/fencing/idempotency classification, the COMMIT-time
// conflict reclassification, tenant fail-closed acquisition, and the row-scan
// decode failures. A real database is deliberately NOT used — the [Pool],
// [PooledConn] and pgx.Tx surfaces are faked in-memory so each branch is
// provoked deterministically (a serialization failure at COMMIT, for example,
// is injected rather than raced).

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/boltrope/boltrope/internal/orchestrator/app"
	"github.com/boltrope/boltrope/internal/orchestrator/domain"
	infradb "github.com/boltrope/boltrope/internal/orchestrator/infra/db"
)

// ---------------------------------------------------------------------------
// In-memory fakes for Pool / PooledConn / pgx.Tx / pgx.Rows / pgx.Row.
// ---------------------------------------------------------------------------

// stmtStub is one scripted response of [fakeTx]: the first stub whose match is
// a substring of the statement SQL answers it. Unmatched Exec/Query succeed
// benignly (so tests only script the statements they care about); an unmatched
// QueryRow fails loudly, since every QueryRow in this package scans a value the
// production code depends on.
type stmtStub struct {
	match string                  // substring of the SQL this stub answers
	tag   pgconn.CommandTag       // Exec result tag (zero tag => 0 rows affected)
	err   error                   // error for Exec / Query / QueryRow's Scan
	rows  *fakeRows               // Query result set
	scan  func(dest ...any) error // QueryRow scan behavior
}

// fakeTx implements pgx.Tx over a stub script. It records every routed SQL (in
// order) plus commit/rollback counts so tests can assert what the store did —
// e.g. that the idempotency short-circuit never reaches the optimistic UPDATE.
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

func (tx *fakeTx) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	tx.executed = append(tx.executed, sql)
	if st := tx.find(sql); st != nil {
		if st.err != nil {
			return nil, st.err
		}
		return st.rows, nil
	}
	return &fakeRows{}, nil
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

func (tx *fakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("fakeTx: CopyFrom not supported")
}
func (tx *fakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (tx *fakeTx) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (tx *fakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("fakeTx: Prepare not supported")
}
func (tx *fakeTx) Conn() *pgx.Conn { return nil }

// sawSQL reports whether any routed statement contains substr.
func (tx *fakeTx) sawSQL(substr string) bool {
	for _, sql := range tx.executed {
		if strings.Contains(sql, substr) {
			return true
		}
	}
	return false
}

// fakeRow implements pgx.Row: either a fixed Scan error (e.g. pgx.ErrNoRows for
// the 0-row optimistic gate) or a scripted scan that fills the dests.
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

// fakeRows implements pgx.Rows over literal row values in [eventColumns] order.
// scanErr forces the Scan-failure branch; rowsErr surfaces after iteration (the
// rows.Err() path a network fault mid-result-set takes).
type fakeRows struct {
	rows    [][]any
	idx     int
	scanErr error
	rowsErr error
}

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return r.rowsErr }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Next() bool {
	if r.idx < len(r.rows) {
		r.idx++
		return true
	}
	return false
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.rows[r.idx-1]
	if len(dest) != len(row) {
		return fmt.Errorf("fakeRows: %d dests for %d values", len(dest), len(row))
	}
	for i, d := range dest {
		if err := assignDest(d, row[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *fakeRows) Values() ([]any, error) { return nil, errors.New("fakeRows: Values not supported") }
func (r *fakeRows) RawValues() [][]byte    { return nil }
func (r *fakeRows) Conn() *pgx.Conn        { return nil }

// assignDest copies a stored row value into a Scan destination, covering
// exactly the dest types [scanEnvelopes] and the session reads use.
func assignDest(dest, v any) error {
	switch d := dest.(type) {
	case *string:
		*d = v.(string)
	case *int64:
		*d = v.(int64)
	case *int:
		*d = v.(int)
	case *[]byte:
		*d = v.([]byte)
	case **string:
		if v == nil {
			*d = nil
		} else {
			s := v.(string)
			*d = &s
		}
	case *time.Time:
		*d = v.(time.Time)
	default:
		return fmt.Errorf("fakeRows: unsupported dest type %T", dest)
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

// fakeTenantCtx carries a fixed verified tenant (no DB, so any id works).
func fakeTenantCtx() context.Context {
	return infradb.WithTenant(context.Background(), "11111111-1111-1111-1111-111111111111")
}

// pgErrWithCode builds a *pgconn.PgError with the given SQLSTATE, the shape pgx
// surfaces server errors in (e.g. 23505 unique violation at COMMIT).
func pgErrWithCode(code string) *pgconn.PgError {
	return &pgconn.PgError{Code: code, Message: "stubbed server error"}
}

// gateStub returns the optimistic-UPDATE stub. err != nil overrides the scan
// (pgx.ErrNoRows simulates the 0-row gate miss).
func gateStub(newHead int64, err error) stmtStub {
	return stmtStub{
		match: "RETURNING head_seq",
		err:   err,
		scan: func(dest ...any) error {
			*(dest[0].(*int64)) = newHead
			return nil
		},
	}
}

// insertEventStub returns the INSERT INTO events stub scanning a fixed
// created_at (deterministic; the clock is the database's in production).
func insertEventStub(err error) stmtStub {
	return stmtStub{
		match: "INSERT INTO events",
		err:   err,
		scan: func(dest ...any) error {
			*(dest[0].(*time.Time)) = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			return nil
		},
	}
}

// classifyStub returns the re-read stub classifyAppendFailure runs after a
// 0-row gate, yielding the given session control state.
func classifyStub(status string, headSeq, epoch int64) stmtStub {
	return stmtStub{
		match: "SELECT status, head_seq, lease_epoch",
		scan: func(dest ...any) error {
			*(dest[0].(*string)) = status
			*(dest[1].(*int64)) = headSeq
			*(dest[2].(*int64)) = epoch
			return nil
		},
	}
}

// turnStartedInput is a minimal valid append input.
func turnStartedInput(turnID string) app.AppendInput {
	return app.AppendInput{Event: domain.TurnStarted{TurnID: turnID, Model: "test-model"}, Actor: domain.ActorSystem}
}

// ---------------------------------------------------------------------------
// beginTenantTx / setLocalTenant: fail-closed acquisition.
// ---------------------------------------------------------------------------

// TestBeginTenantTx_FailClosed walks every acquisition failure: a missing
// tenant must fail BEFORE any connection is acquired (fail closed; §8.2), and
// each later failure must release exactly what it acquired so a failed append
// can never leak a connection or leave a transaction open.
func TestBeginTenantTx_FailClosed(t *testing.T) {
	t.Parallel()

	t.Run("no tenant in context", func(t *testing.T) {
		t.Parallel()
		store, pool, _ := newFakeStore(&fakeTx{})
		_, _, err := store.beginTenantTx(context.Background())
		if !errors.Is(err, infradb.ErrNoTenant) {
			t.Fatalf("err = %v, want ErrNoTenant", err)
		}
		if pool.acquires != 0 {
			t.Errorf("acquired %d connections before tenant check; fail-closed must acquire none", pool.acquires)
		}
	})

	t.Run("acquire error", func(t *testing.T) {
		t.Parallel()
		store, pool, _ := newFakeStore(&fakeTx{})
		pool.acquireErr = errors.New("pool exhausted")
		_, _, err := store.beginTenantTx(fakeTenantCtx())
		if err == nil || !strings.Contains(err.Error(), "pool exhausted") {
			t.Fatalf("err = %v, want acquire error surfaced", err)
		}
	})

	t.Run("begin error releases the connection", func(t *testing.T) {
		t.Parallel()
		store, _, conn := newFakeStore(&fakeTx{})
		conn.beginErr = errors.New("backend down")
		_, _, err := store.beginTenantTx(fakeTenantCtx())
		if err == nil || !strings.Contains(err.Error(), "begin tx") {
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
		_, _, err := store.beginTenantTx(fakeTenantCtx())
		if err == nil || !strings.Contains(err.Error(), "setting tenant GUC") {
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
		gotTx, cleanup, err := store.beginTenantTx(fakeTenantCtx())
		if err != nil {
			t.Fatalf("beginTenantTx: %v", err)
		}
		if gotTx != pgx.Tx(tx) {
			t.Fatal("beginTenantTx returned a different tx than the connection began")
		}
		if !tx.sawSQL("set_config") {
			t.Error("tenant GUC was not set on the new transaction")
		}
		cleanup()
		if tx.rollbacks != 1 || conn.released != 1 {
			t.Errorf("after cleanup: rollbacks = %d, released = %d; want 1 and 1", tx.rollbacks, conn.released)
		}
	})
}

// ---------------------------------------------------------------------------
// classifyAppendFailure: the three sentinels behind a 0-row optimistic gate.
// ---------------------------------------------------------------------------

// TestAppend_GateMissClassification drives Append into the 0-row gate path and
// asserts the re-read maps each session state to its precise sentinel
// (ADR-0011 §6.3) — including the AC-3 precedence rule that a stale-lease
// writer is FENCED even when its expected head happens to be current, and the
// fail-closed mapping of an invisible (RLS-hidden or absent) session.
func TestAppend_GateMissClassification(t *testing.T) {
	t.Parallel()

	const writerEpoch = int64(3)
	cases := []struct {
		name     string
		classify stmtStub // re-read result (or error)
		wantIs   error    // sentinel via errors.Is; nil => non-sentinel wrapped error
		wantSub  string   // substring of the error text
	}{
		{
			name:     "session missing or RLS-hidden",
			classify: stmtStub{match: "SELECT status, head_seq, lease_epoch", err: pgx.ErrNoRows},
			wantIs:   app.SessionNotActiveError,
			wantSub:  "not found or not visible",
		},
		{
			name:     "session finished",
			classify: classifyStub(string(domain.StatusFinished), 5, writerEpoch),
			wantIs:   app.SessionNotActiveError,
			wantSub:  "status=finished",
		},
		{
			name: "stale lease with current head is fenced (AC-3 precedence)",
			// head matches expected (5) — only the epoch is stale, so the head
			// re-check alone would NOT explain the miss; fencing must win.
			classify: classifyStub(string(domain.StatusActive), 5, writerEpoch+1),
			wantIs:   app.FencedError,
		},
		{
			name:     "stale lease and stale head is still fenced first",
			classify: classifyStub(string(domain.StatusActive), 9, writerEpoch+1),
			wantIs:   app.FencedError,
		},
		{
			name:     "stale head with current lease conflicts",
			classify: classifyStub(string(domain.StatusActive), 9, writerEpoch),
			wantIs:   app.ConflictError,
			wantSub:  "head_seq=9, expected=5",
		},
		{
			name:     "re-read itself fails",
			classify: stmtStub{match: "SELECT status, head_seq, lease_epoch", err: errors.New("conn reset")},
			wantIs:   nil,
			wantSub:  "classifying append failure",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx := &fakeTx{stubs: []stmtStub{gateStub(0, pgx.ErrNoRows), tc.classify}}
			store, _, _ := newFakeStore(tx)

			_, err := store.Append(fakeTenantCtx(), "sess-1", 5, writerEpoch, "req-1", turnStartedInput("t1"))
			if err == nil {
				t.Fatal("Append on a 0-row gate must fail")
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("err = %v, want errors.Is(%v)", err, tc.wantIs)
			}
			if tc.wantIs == nil {
				// Must NOT leak any sentinel for an unclassifiable failure.
				for _, sentinel := range []error{app.ConflictError, app.FencedError, app.SessionNotActiveError} {
					if errors.Is(err, sentinel) {
						t.Fatalf("err = %v wrongly matches sentinel %v", err, sentinel)
					}
				}
			}
			if tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("err = %q, want substring %q", err, tc.wantSub)
			}
			if tx.commits != 0 {
				t.Errorf("commits = %d; a classified gate miss must never commit", tx.commits)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// reclassifyCommitError: conflicts surfacing at COMMIT.
// ---------------------------------------------------------------------------

// TestAppend_CommitErrorReclassification drives a fully-staged append whose
// COMMIT fails, the deferred-constraint shape a racing writer produces when it
// wins between our gate and our commit. Only SQLSTATE 23505 maps to
// ConflictError; everything else (including a 40001 serialization failure) is
// wrapped verbatim so callers can apply their own retry policy.
func TestAppend_CommitErrorReclassification(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		commitErr    error
		wantConflict bool
	}{
		{"unique violation at commit is a conflict", pgErrWithCode("23505"), true},
		{
			"wrapped unique violation still classifies",
			fmt.Errorf("commit: %w", pgErrWithCode("23505")),
			true,
		},
		{"serialization failure is not silently a conflict", pgErrWithCode("40001"), false},
		{"plain commit error wraps verbatim", errors.New("connection lost"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx := &fakeTx{
				stubs:     []stmtStub{gateStub(6, nil), insertEventStub(nil)},
				commitErr: tc.commitErr,
			}
			store, _, _ := newFakeStore(tx)

			_, err := store.Append(fakeTenantCtx(), "sess-1", 5, 0, "req-1", turnStartedInput("t1"))
			if err == nil {
				t.Fatal("Append must surface the commit failure")
			}
			if got := errors.Is(err, app.ConflictError); got != tc.wantConflict {
				t.Fatalf("errors.Is(err, ConflictError) = %v, want %v (err = %v)", got, tc.wantConflict, err)
			}
			if !tc.wantConflict && !strings.Contains(err.Error(), "commit append") {
				t.Errorf("non-conflict commit error %q should wrap with 'commit append'", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// appendTx: argument validation, idempotency, per-statement failures, success.
// ---------------------------------------------------------------------------

// TestAppend_ValidationFailsBeforeAcquire asserts the no-event and no-tenant
// rejections happen before any connection is touched.
func TestAppend_ValidationFailsBeforeAcquire(t *testing.T) {
	t.Parallel()

	store, pool, _ := newFakeStore(&fakeTx{})
	if _, err := store.Append(fakeTenantCtx(), "sess-1", 0, 0, "req-1"); err == nil {
		t.Fatal("Append with zero events must fail")
	}
	if _, err := store.Append(context.Background(), "sess-1", 0, 0, "req-1", turnStartedInput("t1")); !errors.Is(err, infradb.ErrNoTenant) {
		t.Fatalf("Append without tenant: err = %v, want ErrNoTenant", err)
	}
	if pool.acquires != 0 {
		t.Errorf("acquires = %d; validation failures must not acquire a connection", pool.acquires)
	}
}

// TestAppend_AcquireFailureSurfaces asserts a pool acquisition failure aborts
// the append before any statement runs (the beginTenantTx error return inside
// appendTx).
func TestAppend_AcquireFailureSurfaces(t *testing.T) {
	t.Parallel()
	tx := &fakeTx{}
	store, pool, _ := newFakeStore(tx)
	pool.acquireErr = errors.New("pool exhausted")
	_, err := store.Append(fakeTenantCtx(), "sess-1", 0, 0, "req-1", turnStartedInput("t1"))
	if err == nil || !strings.Contains(err.Error(), "pool exhausted") {
		t.Fatalf("err = %v, want acquire error surfaced", err)
	}
	if len(tx.executed) != 0 {
		t.Errorf("statements ran after a failed acquire: %v", tx.executed)
	}
}

// TestAppend_IdempotentReplayShortCircuits scripts a prior committed append
// under the same request_id and asserts the replay returns those envelopes as
// SUCCESS without ever reaching the optimistic UPDATE or committing anything
// (ADR-0011 §6.3: a lost-ACK retry is not a conflict).
func TestAppend_IdempotentReplayShortCircuits(t *testing.T) {
	t.Parallel()

	priorPayload, err := marshalPayload(domain.TurnStarted{TurnID: "t-prior", Model: "m"})
	if err != nil {
		t.Fatalf("marshalPayload: %v", err)
	}
	createdAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tx := &fakeTx{stubs: []stmtStub{{
		match: "request_id = $2",
		rows: &fakeRows{rows: [][]any{
			// eventColumns order: session_id, seq, request_id, event_type,
			// schema_version, payload, blob_ref, actor, created_at.
			{"sess-1", int64(7), "req-1", string(domain.EventTurnStarted), 1, priorPayload, nil, string(domain.ActorSystem), createdAt},
		}},
	}}}
	store, _, _ := newFakeStore(tx)

	envs, err := store.Append(fakeTenantCtx(), "sess-1", 0, 0, "req-1", turnStartedInput("t-any"))
	if err != nil {
		t.Fatalf("idempotent replay must succeed, got %v", err)
	}
	if len(envs) != 1 || envs[0].Seq != 7 || envs[0].RequestID != "req-1" {
		t.Fatalf("replay envelopes = %+v, want the single prior envelope seq=7 req-1", envs)
	}
	if got, ok := envs[0].Event.(domain.TurnStarted); !ok || got.TurnID != "t-prior" {
		t.Errorf("replay payload = %#v, want decoded prior TurnStarted{t-prior}", envs[0].Event)
	}
	if tx.sawSQL("UPDATE sessions") {
		t.Error("idempotency short-circuit must not reach the optimistic gate UPDATE")
	}
	if tx.commits != 0 {
		t.Errorf("commits = %d; the read-only replay path relies on the deferred rollback", tx.commits)
	}
}

// TestAppend_PerStatementFailures provokes each statement-level failure inside
// the append transaction and asserts the wrapped error names the failing step
// and that nothing was committed.
func TestAppend_PerStatementFailures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		stubs   []stmtStub
		events  []app.AppendInput
		wantSub string
	}{
		{
			name:    "idempotency lookup query fails",
			stubs:   []stmtStub{{match: "request_id = $2", err: errors.New("conn reset")}},
			events:  []app.AppendInput{turnStartedInput("t1")},
			wantSub: "idempotency lookup",
		},
		{
			name: "idempotency lookup returns a malformed payload row",
			stubs: []stmtStub{{
				match: "request_id = $2",
				rows: &fakeRows{rows: [][]any{
					{"sess-1", int64(1), "req-1", string(domain.EventTurnStarted), 1, []byte("{not json"), nil, "system", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
				}},
			}},
			events:  []app.AppendInput{turnStartedInput("t1")},
			wantSub: "decoding TurnStarted payload",
		},
		{
			name:    "optimistic gate fails with a non-ErrNoRows error",
			stubs:   []stmtStub{gateStub(0, errors.New("conn reset"))},
			events:  []app.AppendInput{turnStartedInput("t1")},
			wantSub: "optimistic gate update",
		},
		{
			// json.Marshal cannot encode NaN — the one way a domain payload can
			// fail to marshal, exercising marshalPayload's error branch.
			name:    "payload marshal failure",
			stubs:   []stmtStub{gateStub(6, nil)},
			events:  []app.AppendInput{{Event: domain.AssistantMessage{TurnID: "t1", CostUSD: math.NaN()}}},
			wantSub: "marshaling AssistantMessage payload",
		},
		{
			name:    "event insert fails",
			stubs:   []stmtStub{gateStub(6, nil), insertEventStub(errors.New("disk full"))},
			events:  []app.AppendInput{turnStartedInput("t1")},
			wantSub: "inserting event seq=6",
		},
		{
			name:    "notify fails",
			stubs:   []stmtStub{gateStub(6, nil), insertEventStub(nil), {match: "pg_notify", err: errors.New("conn reset")}},
			events:  []app.AppendInput{turnStartedInput("t1")},
			wantSub: "eventstore: notify",
		},
		{
			// A plain Append carrying a blob-referencing payload must be refused:
			// only AppendWithBlob inserts the metadata row that the composite FK
			// needs in the same tx, so this is the dangling-reference guard.
			name:    "blob-referencing event without a BlobUpload",
			stubs:   []stmtStub{gateStub(6, nil)},
			events:  []app.AppendInput{{Event: domain.ToolResult{CallID: "c1", BlobRef: "sha256:abc"}}},
			wantSub: "use AppendWithBlob",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx := &fakeTx{stubs: tc.stubs}
			store, _, _ := newFakeStore(tx)
			_, err := store.Append(fakeTenantCtx(), "sess-1", 5, 0, "req-1", tc.events...)
			if err == nil {
				t.Fatal("Append must fail")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("err = %q, want substring %q", err, tc.wantSub)
			}
			if tx.commits != 0 {
				t.Errorf("commits = %d, want 0 on failure", tx.commits)
			}
		})
	}
}

// TestAppend_SuccessAssignsSeqsAndDefaults asserts the happy path through the
// fakes: contiguous seqs from expectedHeadSeq+1, the schema-version and actor
// defaults, the NOTIFY hint inside the tx, and exactly one commit.
func TestAppend_SuccessAssignsSeqsAndDefaults(t *testing.T) {
	t.Parallel()

	tx := &fakeTx{stubs: []stmtStub{gateStub(7, nil), insertEventStub(nil)}}
	store, _, _ := newFakeStore(tx)

	envs, err := store.Append(fakeTenantCtx(), "sess-1", 5, 2, "req-1",
		app.AppendInput{Event: domain.TurnStarted{TurnID: "t1", Model: "m"}}, // Actor + SchemaVersion unset
		app.AppendInput{Event: domain.TurnFinished{TurnID: "t1", Reason: domain.Success}, SchemaVersion: 3, Actor: domain.ActorAssistant},
	)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(envs) != 2 || envs[0].Seq != 6 || envs[1].Seq != 7 {
		t.Fatalf("envelope seqs = %+v, want contiguous 6,7", envs)
	}
	if envs[0].SchemaVersion != defaultSchemaVersion || envs[0].Actor != domain.ActorSystem {
		t.Errorf("defaults: schema=%d actor=%q, want %d/%q", envs[0].SchemaVersion, envs[0].Actor, defaultSchemaVersion, domain.ActorSystem)
	}
	if envs[1].SchemaVersion != 3 || envs[1].Actor != domain.ActorAssistant {
		t.Errorf("explicit values overwritten: schema=%d actor=%q", envs[1].SchemaVersion, envs[1].Actor)
	}
	if !tx.sawSQL("pg_notify") {
		t.Error("the Subscribe wakeup NOTIFY must be issued inside the append tx")
	}
	if tx.commits != 1 {
		t.Errorf("commits = %d, want exactly 1", tx.commits)
	}
}

// TestAppendWithBlob covers the blob-in-same-tx variants: the metadata row must
// precede the event insert (write-before-reference inside the tx), an empty Ref
// is rejected, a mismatched Ref trips the dangling-reference guard, and an
// insert failure aborts the append.
func TestAppendWithBlob(t *testing.T) {
	t.Parallel()

	blob := BlobUpload{Ref: "sha256:abc", MediaType: "text/plain", SizeBytes: 10, StorageURI: "t1/sha256:abc"}
	blobEvent := app.AppendInput{Event: domain.ToolResult{CallID: "c1", Result: "descriptor", Truncated: true, BlobRef: "sha256:abc"}}

	t.Run("success inserts the blob row before the event", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{gateStub(6, nil), insertEventStub(nil)}}
		store, _, _ := newFakeStore(tx)
		envs, err := store.AppendWithBlob(fakeTenantCtx(), "sess-1", 5, 0, "req-1", blob, blobEvent)
		if err != nil {
			t.Fatalf("AppendWithBlob: %v", err)
		}
		if len(envs) != 1 || envs[0].Seq != 6 {
			t.Fatalf("envelopes = %+v, want one at seq 6", envs)
		}
		blobIdx, eventIdx := -1, -1
		for i, sql := range tx.executed {
			if strings.Contains(sql, "INSERT INTO blobs") {
				blobIdx = i
			}
			if strings.Contains(sql, "INSERT INTO events") && eventIdx == -1 {
				eventIdx = i
			}
		}
		if blobIdx == -1 || eventIdx == -1 || blobIdx > eventIdx {
			t.Errorf("blob insert at %d, event insert at %d; blob row must precede the referencing event", blobIdx, eventIdx)
		}
	})

	t.Run("empty blob Ref is rejected", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{gateStub(6, nil)}}
		store, _, _ := newFakeStore(tx)
		_, err := store.AppendWithBlob(fakeTenantCtx(), "sess-1", 5, 0, "req-1", BlobUpload{}, turnStartedInput("t1"))
		if err == nil || !strings.Contains(err.Error(), "non-empty Ref") {
			t.Fatalf("err = %v, want non-empty Ref rejection", err)
		}
	})

	t.Run("mismatched blob Ref trips the dangling-reference guard", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{gateStub(6, nil)}}
		store, _, _ := newFakeStore(tx)
		other := BlobUpload{Ref: "sha256:OTHER", MediaType: "text/plain", SizeBytes: 1, StorageURI: "x"}
		_, err := store.AppendWithBlob(fakeTenantCtx(), "sess-1", 5, 0, "req-1", other, blobEvent)
		if err == nil || !strings.Contains(err.Error(), "no matching BlobUpload") {
			t.Fatalf("err = %v, want mismatched-ref guard", err)
		}
	})

	t.Run("blob insert failure aborts the append", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{gateStub(6, nil), {match: "INSERT INTO blobs", err: errors.New("disk full")}}}
		store, _, _ := newFakeStore(tx)
		_, err := store.AppendWithBlob(fakeTenantCtx(), "sess-1", 5, 0, "req-1", blob, blobEvent)
		if err == nil || !strings.Contains(err.Error(), "inserting blob row") {
			t.Fatalf("err = %v, want wrapped blob-insert error", err)
		}
		if tx.commits != 0 {
			t.Errorf("commits = %d, want 0", tx.commits)
		}
	})
}

// ---------------------------------------------------------------------------
// scanEnvelopes: malformed rows and mid-stream faults.
// ---------------------------------------------------------------------------

// TestScanEnvelopes covers the row-decode branches: a healthy mixed result set
// (with and without blob_ref), a Scan failure, a malformed payload, an unknown
// event type, and a rows.Err() surfacing after iteration (the shape a network
// fault mid-result-set takes — Next() just stops early).
func TestScanEnvelopes(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	goodPayload, err := marshalPayload(domain.TurnStarted{TurnID: "t1", Model: "m"})
	if err != nil {
		t.Fatalf("marshalPayload: %v", err)
	}
	blobPayload, err := marshalPayload(domain.ToolResult{CallID: "c1", BlobRef: "sha256:abc", Truncated: true})
	if err != nil {
		t.Fatalf("marshalPayload: %v", err)
	}

	t.Run("decodes rows and stamps the caller tenant", func(t *testing.T) {
		t.Parallel()
		rows := &fakeRows{rows: [][]any{
			{"sess-1", int64(1), "req-1", string(domain.EventTurnStarted), 1, goodPayload, nil, string(domain.ActorSystem), createdAt},
			{"sess-1", int64(2), "req-1", string(domain.EventToolResult), 2, blobPayload, "sha256:abc", string(domain.ActorTool), createdAt},
		}}
		envs, err := scanEnvelopes(rows, "tenant-A")
		if err != nil {
			t.Fatalf("scanEnvelopes: %v", err)
		}
		if len(envs) != 2 {
			t.Fatalf("got %d envelopes, want 2", len(envs))
		}
		for _, env := range envs {
			if env.TenantID != "tenant-A" {
				t.Errorf("envelope seq=%d tenant = %q, want caller tenant stamped", env.Seq, env.TenantID)
			}
		}
		if tr, ok := envs[1].Event.(domain.ToolResult); !ok || tr.BlobRef != "sha256:abc" {
			t.Errorf("second envelope payload = %#v, want ToolResult with BlobRef", envs[1].Event)
		}
		if envs[1].SchemaVersion != 2 || envs[1].Actor != domain.ActorTool || !envs[1].CreatedAt.Equal(createdAt) {
			t.Errorf("envelope coordinates drifted: %+v", envs[1])
		}
	})

	t.Run("scan failure", func(t *testing.T) {
		t.Parallel()
		rows := &fakeRows{
			rows:    [][]any{{"sess-1", int64(1), "req-1", "TurnStarted", 1, goodPayload, nil, "system", createdAt}},
			scanErr: errors.New("type mismatch"),
		}
		if _, err := scanEnvelopes(rows, "t"); err == nil || !strings.Contains(err.Error(), "scanning event row") {
			t.Fatalf("err = %v, want wrapped scan error", err)
		}
	})

	t.Run("malformed payload", func(t *testing.T) {
		t.Parallel()
		rows := &fakeRows{rows: [][]any{
			{"sess-1", int64(1), "req-1", string(domain.EventTurnStarted), 1, []byte("{truncated"), nil, "system", createdAt},
		}}
		if _, err := scanEnvelopes(rows, "t"); err == nil || !strings.Contains(err.Error(), "decoding TurnStarted payload") {
			t.Fatalf("err = %v, want decode error", err)
		}
	})

	t.Run("unknown event type fails loudly", func(t *testing.T) {
		t.Parallel()
		rows := &fakeRows{rows: [][]any{
			{"sess-1", int64(1), "req-1", "EventFromTheFuture", 1, []byte("{}"), nil, "system", createdAt},
		}}
		if _, err := scanEnvelopes(rows, "t"); err == nil || !strings.Contains(err.Error(), "unknown event_type") {
			t.Fatalf("err = %v, want unknown event_type error", err)
		}
	})

	t.Run("rows.Err after iteration", func(t *testing.T) {
		t.Parallel()
		rows := &fakeRows{rowsErr: errors.New("conn reset mid-stream")}
		if _, err := scanEnvelopes(rows, "t"); err == nil || !strings.Contains(err.Error(), "iterating event rows") {
			t.Fatalf("err = %v, want wrapped rows.Err", err)
		}
	})
}

// TestMarshalPayloadError pins the one realistic marshal failure (NaN in a
// float column) and that the error names the offending event type — the detail
// an operator needs when a payload is rejected.
func TestMarshalPayloadError(t *testing.T) {
	t.Parallel()
	_, err := marshalPayload(domain.AssistantMessage{TurnID: "t1", CostUSD: math.NaN()})
	if err == nil || !strings.Contains(err.Error(), "AssistantMessage") {
		t.Fatalf("err = %v, want marshal error naming AssistantMessage", err)
	}
}

// TestIsUniqueViolation_PgCodes complements the nil/plain cases in
// store_internal_test.go with the positive 23505 match (bare and wrapped) and
// a near-miss SQLSTATE.
func TestIsUniqueViolation_PgCodes(t *testing.T) {
	t.Parallel()
	if !isUniqueViolation(pgErrWithCode("23505")) {
		t.Error("bare 23505 PgError must classify as unique violation")
	}
	if !isUniqueViolation(fmt.Errorf("commit: %w", pgErrWithCode("23505"))) {
		t.Error("wrapped 23505 PgError must classify via errors.As")
	}
	if isUniqueViolation(pgErrWithCode("40001")) {
		t.Error("a serialization failure (40001) must NOT classify as unique violation")
	}
}

// ---------------------------------------------------------------------------
// sessions.go: SetSessionStatus and CreateTenant control paths.
// ---------------------------------------------------------------------------

// TestSetSessionStatus covers the illegal active transition, the 0-row
// classification (fenced / not-visible), and the exec/commit failure wraps.
func TestSetSessionStatus(t *testing.T) {
	t.Parallel()

	t.Run("transition to active is rejected before any tx", func(t *testing.T) {
		t.Parallel()
		store, pool, _ := newFakeStore(&fakeTx{})
		err := store.SetSessionStatus(fakeTenantCtx(), "sess-1", 0, domain.StatusActive)
		if err == nil || !strings.Contains(err.Error(), "not allowed") {
			t.Fatalf("err = %v, want active-transition rejection", err)
		}
		if pool.acquires != 0 {
			t.Errorf("acquires = %d; the caller error must not open a transaction", pool.acquires)
		}
	})

	t.Run("no tenant in context fails closed", func(t *testing.T) {
		t.Parallel()
		store, _, _ := newFakeStore(&fakeTx{})
		if err := store.SetSessionStatus(context.Background(), "sess-1", 0, domain.StatusFinished); !errors.Is(err, infradb.ErrNoTenant) {
			t.Fatalf("err = %v, want ErrNoTenant", err)
		}
	})

	t.Run("zero rows from a stale epoch classifies as fenced", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{
			{match: "UPDATE sessions SET status", tag: pgconn.NewCommandTag("UPDATE 0")},
			classifyStub(string(domain.StatusActive), 4, 9), // current epoch 9 != writer 3
		}}
		store, _, _ := newFakeStore(tx)
		err := store.SetSessionStatus(fakeTenantCtx(), "sess-1", 3, domain.StatusFinished)
		if !errors.Is(err, app.FencedError) {
			t.Fatalf("err = %v, want FencedError", err)
		}
		if tx.commits != 0 {
			t.Errorf("commits = %d, want 0 on a fenced close", tx.commits)
		}
	})

	t.Run("zero rows on an invisible session is not-active", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{
			{match: "UPDATE sessions SET status", tag: pgconn.NewCommandTag("UPDATE 0")},
			{match: "SELECT status, head_seq, lease_epoch", err: pgx.ErrNoRows},
		}}
		store, _, _ := newFakeStore(tx)
		if err := store.SetSessionStatus(fakeTenantCtx(), "ghost", 0, domain.StatusFailed); !errors.Is(err, app.SessionNotActiveError) {
			t.Fatalf("err = %v, want SessionNotActiveError", err)
		}
	})

	t.Run("update exec failure wraps", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{{match: "UPDATE sessions SET status", err: errors.New("conn reset")}}}
		store, _, _ := newFakeStore(tx)
		if err := store.SetSessionStatus(fakeTenantCtx(), "sess-1", 0, domain.StatusFinished); err == nil || !strings.Contains(err.Error(), "setting session status") {
			t.Fatalf("err = %v, want wrapped exec error", err)
		}
	})

	t.Run("commit failure wraps", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{
			stubs:     []stmtStub{{match: "UPDATE sessions SET status", tag: pgconn.NewCommandTag("UPDATE 1")}},
			commitErr: errors.New("conn lost"),
		}
		store, _, _ := newFakeStore(tx)
		if err := store.SetSessionStatus(fakeTenantCtx(), "sess-1", 0, domain.StatusFinished); err == nil || !strings.Contains(err.Error(), "commit set-status") {
			t.Fatalf("err = %v, want wrapped commit error", err)
		}
	})

	t.Run("success commits once", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{{match: "UPDATE sessions SET status", tag: pgconn.NewCommandTag("UPDATE 1")}}}
		store, _, _ := newFakeStore(tx)
		if err := store.SetSessionStatus(fakeTenantCtx(), "sess-1", 0, domain.StatusFinished); err != nil {
			t.Fatalf("SetSessionStatus: %v", err)
		}
		if tx.commits != 1 {
			t.Errorf("commits = %d, want 1", tx.commits)
		}
	})
}

// TestCreateTenant covers the fail-closed missing-tenant path and the
// exec/commit failure wraps of the bootstrap helper.
func TestCreateTenant(t *testing.T) {
	t.Parallel()

	t.Run("no tenant in context fails closed", func(t *testing.T) {
		t.Parallel()
		store, pool, _ := newFakeStore(&fakeTx{})
		if err := store.CreateTenant(context.Background(), "t1", "Tenant One"); !errors.Is(err, infradb.ErrNoTenant) {
			t.Fatalf("err = %v, want ErrNoTenant", err)
		}
		if pool.acquires != 0 {
			t.Errorf("acquires = %d, want 0", pool.acquires)
		}
	})

	t.Run("insert failure wraps", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{stubs: []stmtStub{{match: "INSERT INTO tenants", err: errors.New("conn reset")}}}
		store, _, _ := newFakeStore(tx)
		if err := store.CreateTenant(fakeTenantCtx(), "t1", "Tenant One"); err == nil || !strings.Contains(err.Error(), "creating tenant") {
			t.Fatalf("err = %v, want wrapped insert error", err)
		}
	})

	t.Run("commit failure wraps", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{commitErr: errors.New("conn lost")}
		store, _, _ := newFakeStore(tx)
		if err := store.CreateTenant(fakeTenantCtx(), "t1", "Tenant One"); err == nil || !strings.Contains(err.Error(), "commit create-tenant") {
			t.Fatalf("err = %v, want wrapped commit error", err)
		}
	})

	t.Run("success commits once", func(t *testing.T) {
		t.Parallel()
		tx := &fakeTx{}
		store, _, conn := newFakeStore(tx)
		if err := store.CreateTenant(fakeTenantCtx(), "t1", "Tenant One"); err != nil {
			t.Fatalf("CreateTenant: %v", err)
		}
		if tx.commits != 1 || conn.released != 1 {
			t.Errorf("commits = %d, released = %d; want 1 and 1", tx.commits, conn.released)
		}
	})
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
