package sessionstatus

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/runtime"
)

// ctx is a convenience for a plain background context (the Lookup carries no
// tenant: that is the point — the reaper has no tenant principal).
var ctx = context.Background()

// fakeRow satisfies pgx.Row, scanning a preset nullable status (or failing).
type fakeRow struct {
	status *string
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	p, ok := dest[0].(**string)
	if !ok {
		return errors.New("fakeRow: unexpected scan destination type")
	}
	*p = r.status
	return nil
}

// fakeConn is a PooledConn that records the query it served and whether it was
// released.
type fakeConn struct {
	row      fakeRow
	gotSQL   string
	gotArgs  []any
	released bool
}

func (c *fakeConn) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	c.gotSQL = sql
	c.gotArgs = args
	return c.row
}

func (c *fakeConn) Release() { c.released = true }

// fakePool hands out a single fakeConn (or fails to acquire).
type fakePool struct {
	conn       *fakeConn
	acquireErr error
}

func (p *fakePool) Acquire(context.Context) (PooledConn, error) {
	if p.acquireErr != nil {
		return nil, p.acquireErr
	}
	return p.conn, nil
}

func (p *fakePool) Close() {}

// strPtr returns a pointer to s.
func strPtr(s string) *string { return &s }

// TestStatus_MapsSessionStatusColumn verifies the status-text → SessionStatus
// mapping for every value the sessions_status_chk constraint admits
// (architecture §10.6: finished/failed sandboxes are reaped immediately).
func TestStatus_MapsSessionStatusColumn(t *testing.T) {
	cases := []struct {
		column string
		want   runtime.SessionStatus
	}{
		{column: "active", want: runtime.SessionActive},
		{column: "finished", want: runtime.SessionFinished},
		{column: "failed", want: runtime.SessionFailed},
	}
	for _, tc := range cases {
		t.Run(tc.column, func(t *testing.T) {
			conn := &fakeConn{row: fakeRow{status: strPtr(tc.column)}}
			l := New(&fakePool{conn: conn})

			got, err := l.Status(ctx, "3d0f8e0a-58a6-4d3f-9f6f-0f4d3f1c2b1a")
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if got != tc.want {
				t.Errorf("Status = %v, want %v", got, tc.want)
			}
			if !conn.released {
				t.Error("connection was not released")
			}
		})
	}
}

// TestStatus_PassesSessionIDToQuery verifies the lookup queries the definer
// function with the session id as the only parameter.
func TestStatus_PassesSessionIDToQuery(t *testing.T) {
	conn := &fakeConn{row: fakeRow{status: strPtr("active")}}
	l := New(&fakePool{conn: conn})

	const id = "3d0f8e0a-58a6-4d3f-9f6f-0f4d3f1c2b1a"
	if _, err := l.Status(ctx, id); err != nil {
		t.Fatalf("Status: %v", err)
	}
	if !strings.Contains(conn.gotSQL, "session_status_for_reaper") {
		t.Errorf("query %q does not call session_status_for_reaper", conn.gotSQL)
	}
	if len(conn.gotArgs) != 1 || conn.gotArgs[0] != id {
		t.Errorf("query args = %v, want [%s]", conn.gotArgs, id)
	}
}

// TestStatus_NullStatusIsUnknownWithError verifies the ambiguity contract: a
// NULL from the definer function means the session is missing OR hidden by RLS
// (a misprivileged migration role) — indistinguishable, so the lookup must
// return SessionUnknown plus an error so the reaper retains the sandbox and
// falls back to TTLs (fail-safe; architecture §10.6).
func TestStatus_NullStatusIsUnknownWithError(t *testing.T) {
	conn := &fakeConn{row: fakeRow{status: nil}}
	l := New(&fakePool{conn: conn})

	got, err := l.Status(ctx, "3d0f8e0a-58a6-4d3f-9f6f-0f4d3f1c2b1a")
	if err == nil {
		t.Fatal("Status returned nil error for a NULL status; the reaper would lose its fail-safe signal")
	}
	if got != runtime.SessionUnknown {
		t.Errorf("Status = %v, want SessionUnknown", got)
	}
	if !conn.released {
		t.Error("connection was not released on the error path")
	}
}

// TestStatus_UnrecognizedStatusIsUnknownWithError verifies a status text this
// adapter does not understand (e.g. a future schema value) maps to
// SessionUnknown + error rather than guessing a reapable state.
func TestStatus_UnrecognizedStatusIsUnknownWithError(t *testing.T) {
	conn := &fakeConn{row: fakeRow{status: strPtr("hibernating")}}
	l := New(&fakePool{conn: conn})

	got, err := l.Status(ctx, "3d0f8e0a-58a6-4d3f-9f6f-0f4d3f1c2b1a")
	if err == nil {
		t.Fatal("Status returned nil error for an unrecognized status value")
	}
	if got != runtime.SessionUnknown {
		t.Errorf("Status = %v, want SessionUnknown", got)
	}
}

// TestStatus_AcquireErrorIsUnknownWithError verifies a connection failure
// (Postgres down) surfaces as SessionUnknown + error — retain, never reap.
func TestStatus_AcquireErrorIsUnknownWithError(t *testing.T) {
	l := New(&fakePool{acquireErr: errors.New("connection refused")})

	got, err := l.Status(ctx, "3d0f8e0a-58a6-4d3f-9f6f-0f4d3f1c2b1a")
	if err == nil {
		t.Fatal("Status returned nil error when the pool could not acquire")
	}
	if got != runtime.SessionUnknown {
		t.Errorf("Status = %v, want SessionUnknown", got)
	}
}

// TestStatus_QueryErrorIsUnknownWithError verifies a scan/query failure maps
// to SessionUnknown + error.
func TestStatus_QueryErrorIsUnknownWithError(t *testing.T) {
	conn := &fakeConn{row: fakeRow{err: errors.New("malformed uuid")}}
	l := New(&fakePool{conn: conn})

	got, err := l.Status(ctx, "not-a-uuid")
	if err == nil {
		t.Fatal("Status returned nil error for a query failure")
	}
	if got != runtime.SessionUnknown {
		t.Errorf("Status = %v, want SessionUnknown", got)
	}
	if !conn.released {
		t.Error("connection was not released on the query-error path")
	}
}

// TestStatus_EmptySessionIDFailsWithoutQuery verifies the guard: an empty id
// is a caller bug and must not reach the database.
func TestStatus_EmptySessionIDFailsWithoutQuery(t *testing.T) {
	conn := &fakeConn{row: fakeRow{status: strPtr("active")}}
	l := New(&fakePool{conn: conn})

	got, err := l.Status(ctx, "")
	if err == nil {
		t.Fatal("Status returned nil error for an empty session id")
	}
	if got != runtime.SessionUnknown {
		t.Errorf("Status = %v, want SessionUnknown", got)
	}
	if conn.gotSQL != "" {
		t.Errorf("empty session id still queried the database: %q", conn.gotSQL)
	}
}
