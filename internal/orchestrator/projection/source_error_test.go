package projection

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// stubConn is a canned-response [Conn] for the source's defensive branches: it
// returns exactly what it is configured with, regardless of the statement.
type stubConn struct {
	rows     pgx.Rows
	queryErr error
	row      pgx.Row
	execTag  pgconn.CommandTag
	execErr  error
}

func (c *stubConn) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return c.rows, c.queryErr
}
func (c *stubConn) QueryRow(context.Context, string, ...any) pgx.Row { return c.row }
func (c *stubConn) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return c.execTag, c.execErr
}

// errRows wraps fakeRows with an injectable Scan or post-iteration error so the
// row-decode failure branches are reachable.
type errRows struct {
	*fakeRows
	scanErr error
	iterErr error
}

func (r *errRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	return r.fakeRows.Scan(dest...)
}
func (r *errRows) Err() error { return r.iterErr }

var _ Conn = (*stubConn)(nil)

// TestSource_ErrorPaths covers each statement's failure wrapping plus the two
// non-error edge cases the runner relies on: a missing cursor row reads as the
// zero cursor, and a cursor save that matches no row is a hard error (the
// subscription row must exist).
func TestSource_ErrorPaths(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")

	t.Run("EnsureSubscription wraps exec error", func(t *testing.T) {
		s := NewSource(&stubConn{execErr: boom})
		if err := s.EnsureSubscription(ctx, "sub"); err == nil || !strings.Contains(err.Error(), `ensuring subscription "sub"`) {
			t.Fatalf("EnsureSubscription = %v, want wrapped ensuring error", err)
		}
	})

	t.Run("LoadCursor absent row is the zero cursor", func(t *testing.T) {
		s := NewSource(&stubConn{row: fakeRow{err: pgx.ErrNoRows}})
		cur, err := s.LoadCursor(ctx, "sub")
		if err != nil || cur != (Cursor{}) {
			t.Fatalf("LoadCursor on absent row = (%s, %v), want zero cursor and nil", cur, err)
		}
	})

	t.Run("LoadCursor wraps scan error", func(t *testing.T) {
		s := NewSource(&stubConn{row: fakeRow{err: boom}})
		if _, err := s.LoadCursor(ctx, "sub"); err == nil || !strings.Contains(err.Error(), "loading cursor") {
			t.Fatalf("LoadCursor = %v, want wrapped loading error", err)
		}
	})

	t.Run("SaveCursor wraps exec error", func(t *testing.T) {
		s := NewSource(&stubConn{execErr: boom})
		if err := s.SaveCursor(ctx, "sub", Cursor{TransactionID: 1, GlobalID: 1}); err == nil || !strings.Contains(err.Error(), "saving cursor") {
			t.Fatalf("SaveCursor = %v, want wrapped saving error", err)
		}
	})

	t.Run("SaveCursor with no matched row is an error", func(t *testing.T) {
		s := NewSource(&stubConn{execTag: pgconn.NewCommandTag("UPDATE 0")})
		if err := s.SaveCursor(ctx, "sub", Cursor{TransactionID: 1, GlobalID: 1}); err == nil || !strings.Contains(err.Error(), "subscription row missing") {
			t.Fatalf("SaveCursor = %v, want subscription-row-missing error", err)
		}
	})

	t.Run("FetchBatch wraps scan error", func(t *testing.T) {
		rows := &errRows{
			fakeRows: &fakeRows{cols: [][]any{{"1", int64(1), int64(1), "t", "s", "x", []byte("{}")}}},
			scanErr:  boom,
		}
		s := NewSource(&stubConn{rows: rows})
		if _, err := s.FetchBatch(ctx, Cursor{}, 10); err == nil || !strings.Contains(err.Error(), "scanning event row") {
			t.Fatalf("FetchBatch = %v, want wrapped scanning error", err)
		}
	})

	t.Run("FetchBatch rejects a non-numeric transaction id", func(t *testing.T) {
		rows := &fakeRows{cols: [][]any{{"not-a-number", int64(1), int64(1), "t", "s", "x", []byte("{}")}}}
		s := NewSource(&stubConn{rows: rows})
		if _, err := s.FetchBatch(ctx, Cursor{}, 10); err == nil || !strings.Contains(err.Error(), "parsing transaction_id") {
			t.Fatalf("FetchBatch = %v, want transaction_id parse error", err)
		}
	})

	t.Run("FetchBatch wraps rows iteration error", func(t *testing.T) {
		s := NewSource(&stubConn{rows: &errRows{fakeRows: &fakeRows{}, iterErr: boom}})
		if _, err := s.FetchBatch(ctx, Cursor{}, 10); err == nil || !strings.Contains(err.Error(), "iterating event rows") {
			t.Fatalf("FetchBatch = %v, want wrapped iterating error", err)
		}
	})

	t.Run("Lag wraps scan error", func(t *testing.T) {
		s := NewSource(&stubConn{row: fakeRow{err: boom}})
		if _, err := s.Lag(ctx, Cursor{}); err == nil || !strings.Contains(err.Error(), "reading lag") {
			t.Fatalf("Lag = %v, want wrapped reading-lag error", err)
		}
	})
}
