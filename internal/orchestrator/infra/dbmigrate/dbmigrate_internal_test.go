package dbmigrate

import (
	"errors"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/golang-migrate/migrate/v4/database"
)

// TestMajorFromVersionNum covers the pure version-gate arithmetic against the
// PostgreSQL >= 10 scheme (major*10000 + minor), including the boundary at the
// pinned floor (NFR-PORT-03).
func TestMajorFromVersionNum(t *testing.T) {
	t.Parallel()
	cases := []struct {
		versionNum int
		wantMajor  int
	}{
		{120009, 12},   // 12.9 — below floor
		{130000, 13},   // 13.0 — exactly at floor
		{130012, 13},   // 13.12
		{160004, 16},   // 16.4
		{170001, 17},   // 17.1
		{1000000, 100}, // hypothetical future
	}
	for _, tc := range cases {
		if got := majorFromVersionNum(tc.versionNum); got != tc.wantMajor {
			t.Errorf("majorFromVersionNum(%d) = %d, want %d", tc.versionNum, got, tc.wantMajor)
		}
	}
}

// TestMinPostgresMajorPinnedTo13 guards the pin itself: xid8 /
// pg_current_xact_id() require 13, so a careless edit lowering the floor must
// fail this test (NFR-PORT-03; ADR-0011).
func TestMinPostgresMajorPinnedTo13(t *testing.T) {
	t.Parallel()
	if minPostgresMajor != 13 {
		t.Fatalf("minPostgresMajor = %d, want 13 (xid8/pg_current_xact_id floor)", minPostgresMajor)
	}
}

// TestDriverUnsupportedOps asserts the instance-only/forward-only contract: Open
// (URL scheme) and Drop (schema teardown) are both refused, so the runner can
// never be used to drop the event log (ADR-0011 §6.1).
func TestDriverUnsupportedOps(t *testing.T) {
	t.Parallel()
	d := &pgxDriver{}

	if _, err := d.Open("pgx5://x"); err == nil {
		t.Error("pgxDriver.Open should be unsupported (instance-only)")
	}
	if err := d.Drop(); err == nil {
		t.Error("pgxDriver.Drop should be unsupported (forward-only event log)")
	}
	if err := d.Close(); err != nil {
		t.Errorf("pgxDriver.Close should be a no-op nil, got %v", err)
	}
}

// TestDriverRun_BodyEdgeCases covers Run's pre-statement branches, which never
// touch the connection: a failed body read is wrapped, and an empty body is a
// no-op success (so neither needs a live server).
func TestDriverRun_BodyEdgeCases(t *testing.T) {
	t.Parallel()
	d := &pgxDriver{}

	err := d.Run(iotest.ErrReader(errors.New("body read blew up")))
	if err == nil || !strings.Contains(err.Error(), "reading migration body") {
		t.Errorf("Run(failing reader) = %v, want wrapped reading-migration-body error", err)
	}
	if err := d.Run(strings.NewReader("")); err != nil {
		t.Errorf("Run(empty body) = %v, want nil (no statement to execute)", err)
	}
}

// TestNilVersionConstant documents the dependency on golang-migrate's NilVersion
// sentinel that Version returns for an un-migrated database.
func TestNilVersionConstant(t *testing.T) {
	t.Parallel()
	if database.NilVersion != -1 {
		t.Fatalf("database.NilVersion = %d, want -1", database.NilVersion)
	}
}

// TestErrPostgresVersionTooOldIsSentinel asserts the version-gate error is an
// errors.Is-matchable sentinel so callers can branch on it.
func TestErrPostgresVersionTooOldIsSentinel(t *testing.T) {
	t.Parallel()
	wrapped := errors.Join(errors.New("ctx"), ErrPostgresVersionTooOld)
	if !errors.Is(wrapped, ErrPostgresVersionTooOld) {
		t.Fatal("ErrPostgresVersionTooOld must be matchable via errors.Is when wrapped")
	}
}
