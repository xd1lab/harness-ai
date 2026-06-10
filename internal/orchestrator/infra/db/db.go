// Package db is the orchestrator's migration-runner library half: it applies the
// embedded event-store migrations ([github.com/boltrope/boltrope/migrations]) to
// a PostgreSQL DSN and gates on the pinned minimum server version. It is the
// library that cmd/boltrope-migrate (written later) wraps, and it backs the
// startup migration-gate check the orchestrator runs before accepting traffic
// (NFR-OPS-01, NFR-PORT-03; architecture §10.2).
//
// # Why a hand-rolled migrate database driver
//
// golang-migrate's bundled pgx/postgres database drivers pull dependencies
// (pgerrcode, lib/pq) that are not in this module's dependency set. Rather than
// add a dependency, this package implements golang-migrate's small
// [github.com/golang-migrate/migrate/v4/database.Driver] interface directly over
// [github.com/jackc/pgx/v5] (already a dependency) and plugs it into
// [github.com/golang-migrate/migrate/v4.NewWithInstance] alongside the iofs
// source driver. The driver uses the SIMPLE query protocol so a multi-statement
// migration file runs as one batch, a session-level advisory lock on its single
// connection to serialize concurrent migrators (NFR-OPS-02), and golang-migrate's
// conventional schema_migrations bookkeeping table.
//
// A migration is a short-lived, single-connection operation, so the runner uses
// one [github.com/jackc/pgx/v5.Conn] rather than a pool — keeping the
// session-level advisory lock on one predictable session and avoiding any pool
// dependency.
//
// # Forward-only
//
// Before applying, [Migrate] re-checks the forward-only convention via
// [github.com/boltrope/boltrope/migrations.CheckForwardOnly] so a destructive
// down on the log fails loudly even outside CI (ADR-0011 §6.1). Only Up
// migrations are applied here; this library never runs Down on the event log.
package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"

	"github.com/boltrope/boltrope/migrations"
)

const (
	// minPostgresMajor is the pinned PostgreSQL floor: xid8 and
	// pg_current_xact_id() (the event store's transaction_id column and the
	// projector's xmin-bounded cursor) require PostgreSQL >= 13 (NFR-PORT-03;
	// ADR-0011). It mirrors the config package's pin and is re-checked here at
	// connect time so a misconfigured server fails fast before any DDL runs.
	minPostgresMajor = 13

	// migrationsTable is golang-migrate's bookkeeping table recording the applied
	// version and dirty state. It is created on first use.
	migrationsTable = "schema_migrations"

	// advisoryLockKey is the session-level advisory lock id the runner holds for
	// the duration of a migration so two concurrent migrators cannot apply the
	// same step (NFR-OPS-02). The constant is arbitrary but fixed across the
	// fleet so all migrators contend on the same lock.
	advisoryLockKey int64 = 0x626f6c74 // "bolt"
)

// ErrPostgresVersionTooOld is returned by [Migrate] and [CheckPostgresVersion]
// when the connected server's major version is below the pinned minimum
// ([minPostgresMajor]). It is a sentinel for [errors.Is] so callers (the migrate
// command, the orchestrator startup gate) can distinguish a version-gate failure
// from a connection or DDL error (NFR-PORT-03).
var ErrPostgresVersionTooOld = errors.New("db: connected PostgreSQL server version is below the supported minimum (13)")

// Migrate connects to dsn, verifies the server satisfies the pinned minimum
// PostgreSQL version, and applies all embedded Up migrations to completion. It
// is idempotent: a database already at the latest version is a no-op and returns
// nil (golang-migrate's ErrNoChange is treated as success, the clean "already
// applied" case; architecture §10.2). It returns [ErrPostgresVersionTooOld] when
// the server is too old (NFR-PORT-03), or a wrapped error on a connection or DDL
// failure.
//
// The caller owns dsn's credentials; Migrate connects as the role the DSN names
// (the migration runner connects as the schema owner, distinct from the
// non-owner application role RLS binds; ADR-0011 §6.7). The pool it opens is
// closed before returning.
func Migrate(ctx context.Context, dsn string) error {
	// Fail fast on a destructive down sneaking into the embedded set, even
	// outside CI (ADR-0011 §6.1).
	if err := migrations.CheckForwardOnly(migrations.Source()); err != nil {
		return err
	}

	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("db: parsing DSN: %w", err)
	}
	// Multi-statement migration files run as one batch under the simple protocol;
	// the extended protocol pgx defaults to permits only a single statement.
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("db: connecting: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if err := checkVersionConn(ctx, conn); err != nil {
		return err
	}

	src, err := iofs.New(migrations.Source(), ".")
	if err != nil {
		return fmt.Errorf("db: opening embedded migration source: %w", err)
	}

	drv := &pgxDriver{ctx: ctx, conn: conn}
	m, err := migrate.NewWithInstance("iofs", src, "pgx-boltrope", drv)
	if err != nil {
		return fmt.Errorf("db: building migrator: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("db: applying migrations: %w", err)
	}
	return nil
}

// RowQuerier is the minimal query surface [CheckPostgresVersion] needs: a single
// QueryRow. Both [github.com/jackc/pgx/v5.Conn] and a pgx pool satisfy it, so the
// version gate is reusable by the migrate runner (single conn) and by the
// orchestrator's pooled startup/readiness check without coupling to either.
type RowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// CheckPostgresVersion reads the connected server's numeric version via
// SHOW server_version_num and returns [ErrPostgresVersionTooOld] if its major
// component is below [minPostgresMajor] (NFR-PORT-03). server_version_num is an
// integer like 130012 (13.12) or 160004 (16.4); the major version is the value
// divided by 10000. It is exported so the orchestrator readiness/startup gate can
// reuse the exact check over its pool.
//
// SHOW returns its value as text, and under the simple query protocol (which the
// runner's connection uses) pgx surfaces all values as text, so the result is
// scanned into a string and parsed — robust under either query protocol.
func CheckPostgresVersion(ctx context.Context, q RowQuerier) error {
	var versionStr string
	if err := q.QueryRow(ctx, "SHOW server_version_num").Scan(&versionStr); err != nil {
		return fmt.Errorf("db: reading server_version_num: %w", err)
	}
	versionNum, err := strconv.Atoi(strings.TrimSpace(versionStr))
	if err != nil {
		return fmt.Errorf("db: parsing server_version_num %q: %w", versionStr, err)
	}
	if major := majorFromVersionNum(versionNum); major < minPostgresMajor {
		return fmt.Errorf("%w: server major version %d (server_version_num=%d), need >= %d",
			ErrPostgresVersionTooOld, major, versionNum, minPostgresMajor)
	}
	return nil
}

// checkVersionConn is the internal version gate over the runner's single
// connection (a [github.com/jackc/pgx/v5.Conn] satisfies [RowQuerier]).
func checkVersionConn(ctx context.Context, conn *pgx.Conn) error {
	return CheckPostgresVersion(ctx, conn)
}

// majorFromVersionNum extracts the PostgreSQL major version from the integer
// server_version_num reports (e.g. 130012 -> 13, 160004 -> 16). Since
// PostgreSQL 10 the scheme is major*10000 + minor, so integer division by 10000
// yields the major. It is a pure helper so the version-gate arithmetic is
// unit-tested without a live server.
func majorFromVersionNum(versionNum int) int { return versionNum / 10000 }

// pgxDriver implements golang-migrate's [database.Driver] over a single pgx
// connection. It is used only as an already-constructed instance via
// [github.com/golang-migrate/migrate/v4.NewWithInstance]; it does not register a
// URL scheme, so [pgxDriver.Open] is unsupported. The connection is configured
// for the simple query mode so multi-statement migration files run as a single
// batch.
type pgxDriver struct {
	ctx  context.Context //nolint:containedctx // migrate.Driver has no ctx params; the runner's ctx is carried here.
	conn *pgx.Conn
}

// Open is not supported: this driver is constructed as an instance via
// NewWithInstance, not opened from a URL scheme.
func (d *pgxDriver) Open(string) (database.Driver, error) {
	return nil, errors.New("db: pgxDriver is instance-only; use migrate.NewWithInstance")
}

// Close releases the driver. The connection is owned and closed by [Migrate], so
// Close is a no-op here to avoid a double close.
func (d *pgxDriver) Close() error { return nil }

// Lock acquires a session-level advisory lock so only one migrator runs at a
// time (NFR-OPS-02). pg_advisory_lock blocks until granted, so it never returns
// [database.ErrLocked]; contention serializes rather than errors.
func (d *pgxDriver) Lock() error {
	if _, err := d.conn.Exec(d.ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		return fmt.Errorf("db: acquiring advisory lock: %w", err)
	}
	return nil
}

// Unlock releases the advisory lock taken by [pgxDriver.Lock].
func (d *pgxDriver) Unlock() error {
	if _, err := d.conn.Exec(d.ctx, "SELECT pg_advisory_unlock($1)", advisoryLockKey); err != nil {
		return fmt.Errorf("db: releasing advisory lock: %w", err)
	}
	return nil
}

// Run applies one migration: it reads the full SQL body and executes it as a
// single simple-protocol batch (so a file with multiple statements runs
// atomically per Postgres' implicit transaction for a simple query string).
func (d *pgxDriver) Run(migration io.Reader) error {
	body, err := io.ReadAll(migration)
	if err != nil {
		return fmt.Errorf("db: reading migration body: %w", err)
	}
	if len(body) == 0 {
		return nil
	}
	if _, err := d.conn.Exec(d.ctx, string(body)); err != nil {
		return fmt.Errorf("db: executing migration: %w", err)
	}
	return nil
}

// SetVersion records the applied version and dirty flag in the schema_migrations
// table, creating the table on first use. golang-migrate calls it before and
// after each Run; a single-row table is the documented convention. A version of
// [database.NilVersion] (-1) clears the row (no migration applied).
func (d *pgxDriver) SetVersion(version int, dirty bool) error {
	if err := d.ensureVersionTable(); err != nil {
		return err
	}
	// Replace the single row transactionally so a reader never sees an empty
	// table mid-update.
	tx, err := d.conn.Begin(d.ctx)
	if err != nil {
		return fmt.Errorf("db: begin set-version tx: %w", err)
	}
	defer func() { _ = tx.Rollback(d.ctx) }()

	if _, err := tx.Exec(d.ctx, "TRUNCATE "+migrationsTable); err != nil {
		return fmt.Errorf("db: clearing version row: %w", err)
	}
	// Only record a concrete version; NilVersion means "no migration applied",
	// represented by the now-empty table.
	if version >= 0 {
		if _, err := tx.Exec(d.ctx,
			"INSERT INTO "+migrationsTable+" (version, dirty) VALUES ($1, $2)",
			version, dirty); err != nil {
			return fmt.Errorf("db: recording version: %w", err)
		}
	}
	if err := tx.Commit(d.ctx); err != nil {
		return fmt.Errorf("db: commit set-version tx: %w", err)
	}
	return nil
}

// Version returns the currently recorded version and dirty flag, or
// [database.NilVersion] when no migration has been applied (an absent or empty
// schema_migrations table).
func (d *pgxDriver) Version() (int, bool, error) {
	if err := d.ensureVersionTable(); err != nil {
		return database.NilVersion, false, err
	}
	var (
		version int
		dirty   bool
	)
	err := d.conn.QueryRow(d.ctx,
		"SELECT version, dirty FROM "+migrationsTable+" LIMIT 1").Scan(&version, &dirty)
	if errors.Is(err, pgx.ErrNoRows) {
		return database.NilVersion, false, nil
	}
	if err != nil {
		return database.NilVersion, false, fmt.Errorf("db: reading version: %w", err)
	}
	return version, dirty, nil
}

// Drop is intentionally unsupported: this runner is FORWARD-ONLY for the event
// log (ADR-0011 §6.1) and never drops the schema. golang-migrate only calls Drop
// for the `migrate drop` command, which Boltrope does not expose.
func (d *pgxDriver) Drop() error {
	return errors.New("db: Drop is not supported (event store is forward-only; ADR-0011 §6.1)")
}

// ensureVersionTable creates the schema_migrations bookkeeping table if absent.
func (d *pgxDriver) ensureVersionTable() error {
	const ddl = "CREATE TABLE IF NOT EXISTS " + migrationsTable +
		" (version BIGINT NOT NULL PRIMARY KEY, dirty BOOLEAN NOT NULL)"
	if _, err := d.conn.Exec(d.ctx, ddl); err != nil {
		return fmt.Errorf("db: ensuring %s: %w", migrationsTable, err)
	}
	return nil
}

// Compile-time assertion that pgxDriver satisfies the migrate database.Driver.
var _ database.Driver = (*pgxDriver)(nil)
