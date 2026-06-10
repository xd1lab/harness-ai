//go:build integration

// Package db integration tests (dual-mode, mirroring the eventstore/projection
// harnesses): they run the migration runner against a live PostgreSQL — an
// external server via BOLTROPE_TEST_DATABASE_URL, or a disposable
// testcontainers Postgres (image from BOLTROPE_TEST_PG_IMAGE, default
// postgres:16). When no DSN is set and Docker is unreachable the tests skip.
//
// The driver-level tests (SetVersion/Version, Run) replace the single
// schema_migrations row, so they run on a per-test scratch DATABASE created on
// the provisioned server — never on the provisioned database itself, which in
// external-DSN mode may be a shared dev instance.
package db

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	envTestDatabaseURL = "BOLTROPE_TEST_DATABASE_URL"

	// envTestPGImage overrides the Postgres image for the testcontainer mode.
	// NFR-PORT-03 pins the floor at PostgreSQL 13; set
	// BOLTROPE_TEST_PG_IMAGE=postgres:13 to prove the floor without edits.
	envTestPGImage        = "BOLTROPE_TEST_PG_IMAGE"
	defaultContainerImage = "postgres:16"

	containerDB       = "boltrope"
	containerUser     = "boltrope_owner"
	containerPassword = "owner_pw"
)

// provisionDSN returns an owner DSN for a live server: the external DSN when
// configured, otherwise a fresh container. It skips (not fails) when Docker is
// unreachable in container mode.
func provisionDSN(ctx context.Context, t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv(envTestDatabaseURL); dsn != "" {
		t.Logf("db integration: using external DSN from %s", envTestDatabaseURL)
		return dsn
	}
	container, err := tcpostgres.Run(ctx, containerImage(),
		tcpostgres.WithDatabase(containerDB),
		tcpostgres.WithUsername(containerUser),
		tcpostgres.WithPassword(containerPassword),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Skipf("db integration: no %s set and Docker unreachable (testcontainers: %v)", envTestDatabaseURL, err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("container ConnectionString: %v", err)
	}
	return dsn
}

// containerImage returns the Postgres image for the testcontainer mode,
// honoring BOLTROPE_TEST_PG_IMAGE.
func containerImage() string {
	if img := os.Getenv(envTestPGImage); img != "" {
		return img
	}
	return defaultContainerImage
}

// connectOwner opens a connection over dsn, closed on test cleanup.
func connectOwner(ctx context.Context, t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect %s-mode owner: %v", dsn, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// scratchConn creates a uniquely named scratch database on the server behind
// adminDSN and returns a SIMPLE-protocol connection to it (the same query mode
// the migration runner configures, so multi-statement Run bodies behave
// identically). The database is dropped on cleanup.
func scratchConn(ctx context.Context, t *testing.T, adminDSN string) *pgx.Conn {
	t.Helper()
	admin := connectOwner(ctx, t, adminDSN)

	// The name is built from a constant prefix plus an integer — a safe
	// identifier; CREATE/DROP DATABASE cannot take bind parameters.
	name := "boltrope_dbtest_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create scratch database %s: %v", name, err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop scratch database %s: %v", name, err)
		}
	})

	cfg, err := pgx.ParseConfig(adminDSN)
	if err != nil {
		t.Fatalf("parse admin DSN: %v", err)
	}
	cfg.Database = name
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect scratch database %s: %v", name, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// waitForCondition polls cond until true or the deadline passes, fatally
// failing on timeout. Used to observe lock-wait state deterministically via
// pg_locks instead of sleeping fixed amounts.
func waitForCondition(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", d, what)
}

// TestMigrate_AppliesAndIsIdempotent applies the embedded migrations to a live
// server and proves: the event-store schema exists afterwards, the
// schema_migrations bookkeeping row records a clean (non-dirty) version, a
// second Migrate is a no-op success (the ErrNoChange path; architecture
// §10.2), and the live server passes the version gate (NFR-PORT-03).
func TestMigrate_AppliesAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	dsn := provisionDSN(ctx, t)

	if err := Migrate(ctx, dsn); err != nil {
		t.Fatalf("Migrate (first run): %v", err)
	}

	conn := connectOwner(ctx, t, dsn)

	// The event-store schema the migrations declare must exist.
	for _, tbl := range []string{"events", "event_subscriptions", "blobs", "sessions", "tenants", "schema_migrations"} {
		var present bool
		if err := conn.QueryRow(ctx, "SELECT to_regclass('public.'||$1::text) IS NOT NULL", tbl).Scan(&present); err != nil {
			t.Fatalf("to_regclass(%s): %v", tbl, err)
		}
		if !present {
			t.Fatalf("table %q missing after Migrate", tbl)
		}
	}

	// Bookkeeping: one clean row at a positive version.
	var (
		version int
		dirty   bool
	)
	if err := conn.QueryRow(ctx, "SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty); err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	if version <= 0 || dirty {
		t.Fatalf("schema_migrations = (version=%d, dirty=%v), want a positive clean version", version, dirty)
	}

	// Idempotence: a second run is the ErrNoChange no-op and changes nothing.
	if err := Migrate(ctx, dsn); err != nil {
		t.Fatalf("Migrate (second run) = %v, want nil (already-applied is success)", err)
	}
	var versionAfter int
	if err := conn.QueryRow(ctx, "SELECT version FROM schema_migrations").Scan(&versionAfter); err != nil {
		t.Fatalf("re-read schema_migrations: %v", err)
	}
	if versionAfter != version {
		t.Fatalf("version changed across an idempotent re-run: %d -> %d", version, versionAfter)
	}

	// The live server passes the version gate through both entry points.
	if err := CheckPostgresVersion(ctx, conn); err != nil {
		t.Fatalf("CheckPostgresVersion (live): %v", err)
	}
	if err := checkVersionConn(ctx, conn); err != nil {
		t.Fatalf("checkVersionConn (live): %v", err)
	}
}

// TestMigrate_ConnectError asserts an unreachable server is a wrapped
// connection error (and not, e.g., a version-gate misreport).
func TestMigrate_ConnectError(t *testing.T) {
	// Port 1 on loopback: refused immediately, nothing listens there.
	err := Migrate(context.Background(), "postgres://nobody:wrong@127.0.0.1:1/none?sslmode=disable&connect_timeout=2")
	if err == nil || !strings.Contains(err.Error(), "db: connecting") {
		t.Fatalf("Migrate(unreachable) = %v, want wrapped connecting error", err)
	}
	if errors.Is(err, ErrPostgresVersionTooOld) {
		t.Fatalf("Migrate(unreachable) claimed ErrPostgresVersionTooOld: %v", err)
	}
}

// TestPgxDriver_VersionLifecycle drives the hand-rolled migrate driver's
// bookkeeping surface against a scratch database: ensureVersionTable creates
// the table on first use, SetVersion replaces the single row (including the
// dirty flag), NilVersion clears it, and Run executes a multi-statement body
// under the simple protocol.
func TestPgxDriver_VersionLifecycle(t *testing.T) {
	ctx := context.Background()
	dsn := provisionDSN(ctx, t)
	conn := scratchConn(ctx, t, dsn)
	d := &pgxDriver{ctx: ctx, conn: conn}

	// A fresh database has no schema_migrations: Version creates it and
	// reports NilVersion (nothing applied).
	v, dirty, err := d.Version()
	if err != nil || v != database.NilVersion || dirty {
		t.Fatalf("Version (fresh) = (%d, %v, %v), want (NilVersion, false, nil)", v, dirty, err)
	}

	// SetVersion records the version and the dirty flag.
	if err := d.SetVersion(7, true); err != nil {
		t.Fatalf("SetVersion(7, true): %v", err)
	}
	if v, dirty, err = d.Version(); err != nil || v != 7 || !dirty {
		t.Fatalf("Version = (%d, %v, %v), want (7, true, nil)", v, dirty, err)
	}

	// A later SetVersion REPLACES the single row (never accumulates rows).
	if err := d.SetVersion(9, false); err != nil {
		t.Fatalf("SetVersion(9, false): %v", err)
	}
	if v, dirty, err = d.Version(); err != nil || v != 9 || dirty {
		t.Fatalf("Version = (%d, %v, %v), want (9, false, nil)", v, dirty, err)
	}
	var rows int
	if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM "+migrationsTable).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("schema_migrations row count = (%d, %v), want exactly 1", rows, err)
	}

	// NilVersion clears the row: back to "no migration applied".
	if err := d.SetVersion(database.NilVersion, false); err != nil {
		t.Fatalf("SetVersion(NilVersion): %v", err)
	}
	if v, dirty, err = d.Version(); err != nil || v != database.NilVersion || dirty {
		t.Fatalf("Version after NilVersion = (%d, %v, %v), want (NilVersion, false, nil)", v, dirty, err)
	}

	// Run executes a multi-statement migration body as one simple-protocol
	// batch (the reason the runner pins the simple query mode).
	body := "CREATE TABLE run_batch_probe (x int);\nINSERT INTO run_batch_probe VALUES (1);\nINSERT INTO run_batch_probe VALUES (2);"
	if err := d.Run(strings.NewReader(body)); err != nil {
		t.Fatalf("Run(multi-statement): %v", err)
	}
	var n int
	if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM run_batch_probe").Scan(&n); err != nil || n != 2 {
		t.Fatalf("run_batch_probe count = (%d, %v), want 2", n, err)
	}

	// An empty body is a no-op; a broken body surfaces the exec error.
	if err := d.Run(strings.NewReader("")); err != nil {
		t.Fatalf("Run(empty) = %v, want nil", err)
	}
	if err := d.Run(strings.NewReader("THIS IS NOT SQL")); err == nil || !strings.Contains(err.Error(), "executing migration") {
		t.Fatalf("Run(invalid SQL) = %v, want wrapped executing error", err)
	}
}

// TestPgxDriver_DeadConnectionErrors asserts the driver surfaces wrapped
// errors (never panics) when its single connection has died mid-migration: the
// advisory lock acquire/release, the bookkeeping bootstrap behind
// Version/SetVersion, and a statement Run each report their own wrap, so a
// failed migrator is diagnosable from the message alone.
func TestPgxDriver_DeadConnectionErrors(t *testing.T) {
	ctx := context.Background()
	dsn := provisionDSN(ctx, t)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	d := &pgxDriver{ctx: ctx, conn: conn}

	if err := d.Lock(); err == nil || !strings.Contains(err.Error(), "acquiring advisory lock") {
		t.Errorf("Lock on dead conn = %v, want wrapped acquiring error", err)
	}
	if err := d.Unlock(); err == nil || !strings.Contains(err.Error(), "releasing advisory lock") {
		t.Errorf("Unlock on dead conn = %v, want wrapped releasing error", err)
	}
	if _, _, err := d.Version(); err == nil || !strings.Contains(err.Error(), "ensuring schema_migrations") {
		t.Errorf("Version on dead conn = %v, want wrapped ensuring error", err)
	}
	if err := d.SetVersion(1, false); err == nil || !strings.Contains(err.Error(), "ensuring schema_migrations") {
		t.Errorf("SetVersion on dead conn = %v, want wrapped ensuring error", err)
	}
	if err := d.Run(strings.NewReader("SELECT 1")); err == nil || !strings.Contains(err.Error(), "executing migration") {
		t.Errorf("Run on dead conn = %v, want wrapped executing error", err)
	}
}

// TestPgxDriver_AdvisoryLockSerializesMigrators proves NFR-OPS-02 end to end:
// a second migrator's Lock BLOCKS (observed via pg_locks as an ungranted
// advisory waiter on the fleet-wide key) until the first Unlocks — contention
// serializes, it never errors — and Unlock fully releases the lock.
func TestPgxDriver_AdvisoryLockSerializesMigrators(t *testing.T) {
	ctx := context.Background()
	dsn := provisionDSN(ctx, t)
	conn1 := connectOwner(ctx, t, dsn)
	conn2 := connectOwner(ctx, t, dsn)
	probe := connectOwner(ctx, t, dsn)

	// pg_locks splits a bigint advisory key into (classid, objid).
	classid := advisoryLockKey >> 32
	objid := advisoryLockKey & 0xFFFFFFFF
	const waiterSQL = `
		SELECT COUNT(*) FROM pg_locks
		 WHERE locktype = 'advisory' AND classid::bigint = $1 AND objid::bigint = $2 AND NOT granted`
	const holderSQL = `
		SELECT COUNT(*) FROM pg_locks
		 WHERE locktype = 'advisory' AND classid::bigint = $1 AND objid::bigint = $2`

	d1 := &pgxDriver{ctx: ctx, conn: conn1}
	d2 := &pgxDriver{ctx: ctx, conn: conn2}

	if err := d1.Lock(); err != nil {
		t.Fatalf("first Lock: %v", err)
	}

	acquired := make(chan error, 1)
	go func() { acquired <- d2.Lock() }()

	// Deterministically observe the second migrator waiting on the key.
	waitForCondition(t, 10*time.Second, "second migrator to block on the advisory lock", func() bool {
		var n int
		if err := probe.QueryRow(ctx, waiterSQL, classid, objid).Scan(&n); err != nil {
			return false
		}
		return n >= 1
	})
	select {
	case err := <-acquired:
		t.Fatalf("second Lock returned (%v) while the first migrator held the lock", err)
	default: // still blocked, as required
	}

	if err := d1.Unlock(); err != nil {
		t.Fatalf("first Unlock: %v", err)
	}
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("second Lock after release = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("second Lock did not acquire after the first Unlock")
	}

	if err := d2.Unlock(); err != nil {
		t.Fatalf("second Unlock: %v", err)
	}
	// Fully released: no holder or waiter remains on the key.
	waitForCondition(t, 10*time.Second, "advisory lock to be fully released", func() bool {
		var n int
		if err := probe.QueryRow(ctx, holderSQL, classid, objid).Scan(&n); err != nil {
			return false
		}
		return n == 0
	})
}
