package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// fakeVersionRow is a pgx.Row stub serving a canned server_version_num text
// (SHOW returns text, which is exactly what the checker scans and parses).
type fakeVersionRow struct {
	val string
	err error
}

func (r fakeVersionRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	p, ok := dest[0].(*string)
	if !ok {
		return fmt.Errorf("fakeVersionRow: want *string dest, got %T", dest[0])
	}
	*p = r.val
	return nil
}

// fakeQuerier is a [RowQuerier] over a single canned row, proving the version
// gate is testable through the consumer-defined interface without a server.
type fakeQuerier struct{ row fakeVersionRow }

func (q fakeQuerier) QueryRow(context.Context, string, ...any) pgx.Row { return q.row }

// TestCheckPostgresVersion covers the version gate against canned
// server_version_num values: the floor boundary, a too-old rejection via the
// errors.Is sentinel, whitespace tolerance, and the parse/query failure wraps
// (NFR-PORT-03). A real server can never be made "too old", so the rejection
// branch is proven here through the RowQuerier seam; the live-server pass is
// exercised by the integration tests.
func TestCheckPostgresVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cases := []struct {
		name        string
		row         fakeVersionRow
		wantTooOld  bool
		wantErrPart string // "" means want nil
	}{
		{name: "12.9 below floor", row: fakeVersionRow{val: "120009"}, wantTooOld: true, wantErrPart: "need >= 13"},
		{name: "13.0 exactly at floor", row: fakeVersionRow{val: "130000"}},
		{name: "16.4 modern server", row: fakeVersionRow{val: "160004"}},
		{name: "whitespace is trimmed", row: fakeVersionRow{val: " 170001\n"}},
		{name: "non-numeric value", row: fakeVersionRow{val: "banana"}, wantErrPart: "parsing server_version_num"},
		{name: "query failure is wrapped", row: fakeVersionRow{err: errors.New("conn reset")}, wantErrPart: "reading server_version_num"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := CheckPostgresVersion(ctx, fakeQuerier{row: tc.row})
			if got := errors.Is(err, ErrPostgresVersionTooOld); got != tc.wantTooOld {
				t.Fatalf("errors.Is(err, ErrPostgresVersionTooOld) = %v, want %v (err=%v)", got, tc.wantTooOld, err)
			}
			if tc.wantErrPart == "" {
				if err != nil {
					t.Fatalf("CheckPostgresVersion = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErrPart) {
				t.Fatalf("CheckPostgresVersion = %v, want error containing %q", err, tc.wantErrPart)
			}
		})
	}
}

// TestCheckPostgresVersion_TooOldErrorIsNotMisparsed asserts the parse-failure
// path does NOT claim the sentinel (a garbled value is a read problem, not a
// proven too-old server), so a startup gate cannot misreport the condition.
func TestCheckPostgresVersion_TooOldErrorIsNotMisparsed(t *testing.T) {
	t.Parallel()
	err := CheckPostgresVersion(context.Background(), fakeQuerier{row: fakeVersionRow{val: "banana"}})
	if err == nil || errors.Is(err, ErrPostgresVersionTooOld) {
		t.Fatalf("parse failure = %v, must be an error but NOT ErrPostgresVersionTooOld", err)
	}
}

// TestMigrate_RejectsUnparsableDSN covers Migrate's DSN-parse failure: it must
// fail before any connection attempt (the forward-only check and the parse are
// both offline), with the parse wrap and no version sentinel.
func TestMigrate_RejectsUnparsableDSN(t *testing.T) {
	t.Parallel()
	err := Migrate(context.Background(), "://not-a-dsn")
	if err == nil || !strings.Contains(err.Error(), "parsing DSN") {
		t.Fatalf("Migrate(bad dsn) = %v, want wrapped parsing-DSN error", err)
	}
	if errors.Is(err, ErrPostgresVersionTooOld) {
		t.Fatalf("Migrate(bad dsn) claimed ErrPostgresVersionTooOld: %v", err)
	}
}
