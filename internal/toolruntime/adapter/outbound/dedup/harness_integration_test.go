//go:build integration

// Integration-test harness for the dedup [Store]. Dual-mode provisioning:
//   - if BOLTROPE_TEST_DATABASE_URL is set, it is used as the owner DSN;
//   - otherwise a Postgres testcontainer is started (image from
//     BOLTROPE_TEST_PG_IMAGE, default postgres:16).
//
// If neither is available the tests are skipped (not failed) with a clear
// reason, so the suite compiles and is deliverable without Docker.
//
// Note: this harness does NOT import internal/orchestrator/infra/db because
// cross-service imports are forbidden for toolruntime packages (ADR-0015,
// depguard). Migrations are applied via a minimal inline pgx runner that uses
// the same migrations.Source() the orchestrator's runner uses.
package dedup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/boltrope/boltrope/migrations"
)

const (
	envTestDatabaseURL = "BOLTROPE_TEST_DATABASE_URL"

	// appRole / appPassword — same values as the event-store harness so both
	// harnesses can share a single test database when BOLTROPE_TEST_DATABASE_URL
	// is set (the migration is idempotent).
	appRole     = "boltrope_app"
	appPassword = "boltrope_app_test_pw" //nolint:gosec // ephemeral test-only DB credential, not a secret

	// envTestPGImage overrides the Postgres image for the testcontainer mode.
	// NFR-PORT-03 pins the supported floor at PostgreSQL 13 (xid8 /
	// pg_current_xact_id), so set BOLTROPE_TEST_PG_IMAGE=postgres:13 to run
	// the floor proof without editing the harness.
	envTestPGImage        = "BOLTROPE_TEST_PG_IMAGE"
	defaultContainerImage = "postgres:16"

	containerDB       = "boltrope"
	containerUser     = "boltrope_owner"
	containerPassword = "owner_pw"

	// advisoryLockKey serializes concurrent migrators (matches db.advisoryLockKey).
	advisoryLockKey int64 = 0x626f6c74 // "bolt"

	// migrationsTable is golang-migrate's bookkeeping table name.
	migrationsTable = "schema_migrations"
)

// dedupHarness is a provisioned dedup-store test environment.
type dedupHarness struct {
	ownerDSN string
	appDSN   string
	store    *Store
	pool     *SimplePool
	mode     string // "external-dsn" or "testcontainer"
}

// newDedupHarness provisions Postgres, applies migrations, grants the app role
// LOGIN, and returns a ready [dedupHarness]. It registers cleanup with t. It
// skips (not fails) when no DSN is set and Docker is unreachable.
func newDedupHarness(t *testing.T) *dedupHarness {
	t.Helper()
	ctx := context.Background()

	ownerDSN, mode := provisionOwnerDSN(ctx, t)

	// Apply all embedded migrations (idempotent; creates tool_executions).
	if err := runMigrations(ctx, ownerDSN); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	grantAppLogin(ctx, t, ownerDSN)

	appDSN := deriveAppDSN(t, ownerDSN)
	pool, err := NewSimplePool(appDSN)
	if err != nil {
		t.Fatalf("NewSimplePool(app): %v", err)
	}
	t.Cleanup(pool.Close)

	return &dedupHarness{
		ownerDSN: ownerDSN,
		appDSN:   appDSN,
		store:    New(pool),
		pool:     pool,
		mode:     mode,
	}
}

// provisionOwnerDSN returns an owner DSN + mode label. Starts a container when
// no external DSN is configured. Skips (not fails) when Docker is unreachable.
func provisionOwnerDSN(ctx context.Context, t *testing.T) (string, string) {
	t.Helper()
	if dsn := os.Getenv(envTestDatabaseURL); dsn != "" {
		t.Logf("dedup integration: using external DSN from %s", envTestDatabaseURL)
		return dsn, "external-dsn"
	}

	container, err := tcpostgres.Run(ctx, containerImage(),
		tcpostgres.WithDatabase(containerDB),
		tcpostgres.WithUsername(containerUser),
		tcpostgres.WithPassword(containerPassword),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Skipf("dedup integration: no %s set and Docker unreachable (testcontainers: %v)", envTestDatabaseURL, err)
	}
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(container)
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("container ConnectionString: %v", err)
	}
	return dsn, "testcontainer"
}

// containerImage returns the Postgres image for the testcontainer mode,
// honoring BOLTROPE_TEST_PG_IMAGE so the NFR-PORT-03 floor (postgres:13) can
// be proven against the same suite that runs on the default image.
func containerImage() string {
	if img := os.Getenv(envTestPGImage); img != "" {
		return img
	}
	return defaultContainerImage
}

// grantAppLogin gives the boltrope_app role a LOGIN credential so the Store
// can connect as the non-owner, RLS-enforcing role.
func grantAppLogin(ctx context.Context, t *testing.T, ownerDSN string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, ownerDSN)
	if err != nil {
		t.Fatalf("owner connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx,
		fmt.Sprintf("ALTER ROLE %s WITH LOGIN PASSWORD '%s'", appRole, appPassword),
	); err != nil {
		t.Fatalf("grant app login: %v", err)
	}
}

// deriveAppDSN rewrites ownerDSN's credentials to the app role.
func deriveAppDSN(t *testing.T, ownerDSN string) string {
	t.Helper()
	u, err := url.Parse(ownerDSN)
	if err != nil {
		t.Fatalf("parse owner DSN: %v", err)
	}
	u.User = url.UserPassword(appRole, appPassword)
	return u.String()
}

// ---------------------------------------------------------------------------
// Inline migration runner (avoids importing internal/orchestrator/infra/db).
// Mirrors the essential logic of infra/db.Migrate: simple-protocol pgx
// connection, session-level advisory lock, golang-migrate Up over iofs source.
// ---------------------------------------------------------------------------

// runMigrations applies the embedded migrations to dsn (forward-only, Up).
// Mirrors infra/db.Migrate without importing the orchestrator package.
func runMigrations(ctx context.Context, dsn string) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("dedup harness: parse DSN: %w", err)
	}
	// Simple protocol so multi-statement migration files run as one batch.
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("dedup harness: connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	src, err := iofs.New(migrations.Source(), ".")
	if err != nil {
		return fmt.Errorf("dedup harness: iofs source: %w", err)
	}

	drv := &minimalPgxDriver{ctx: ctx, conn: conn}
	m, err := migrate.NewWithInstance("iofs", src, "pgx-dedup-harness", drv)
	if err != nil {
		return fmt.Errorf("dedup harness: build migrator: %w", err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("dedup harness: apply migrations: %w", err)
	}
	return nil
}

// minimalPgxDriver satisfies golang-migrate's database.Driver over a single
// pgx connection — identical in structure to infra/db.pgxDriver but local to
// this test file so the cross-service import rule is not violated.
type minimalPgxDriver struct {
	ctx  context.Context //nolint:containedctx // migrate.Driver has no ctx params; runner ctx carried here.
	conn *pgx.Conn
}

func (d *minimalPgxDriver) Open(string) (database.Driver, error) {
	return nil, errors.New("dedup harness: pgxDriver is instance-only")
}

func (d *minimalPgxDriver) Close() error { return nil }

func (d *minimalPgxDriver) Lock() error {
	_, err := d.conn.Exec(d.ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey)
	return err
}

func (d *minimalPgxDriver) Unlock() error {
	_, err := d.conn.Exec(d.ctx, "SELECT pg_advisory_unlock($1)", advisoryLockKey)
	return err
}

func (d *minimalPgxDriver) Run(r io.Reader) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	_, err = d.conn.Exec(d.ctx, string(body))
	return err
}

func (d *minimalPgxDriver) SetVersion(version int, dirty bool) error {
	if err := d.ensureVersionTable(); err != nil {
		return err
	}
	tx, err := d.conn.Begin(d.ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(d.ctx) }()
	if _, err := tx.Exec(d.ctx, "TRUNCATE "+migrationsTable); err != nil {
		return err
	}
	if version >= 0 {
		if _, err := tx.Exec(d.ctx,
			"INSERT INTO "+migrationsTable+" (version, dirty) VALUES ($1, $2)",
			version, dirty); err != nil {
			return err
		}
	}
	return tx.Commit(d.ctx)
}

func (d *minimalPgxDriver) Version() (int, bool, error) {
	if err := d.ensureVersionTable(); err != nil {
		return database.NilVersion, false, err
	}
	var (
		ver   int
		dirty bool
	)
	err := d.conn.QueryRow(d.ctx,
		"SELECT version, dirty FROM "+migrationsTable+" LIMIT 1").Scan(&ver, &dirty)
	if errors.Is(err, pgx.ErrNoRows) {
		return database.NilVersion, false, nil
	}
	if err != nil {
		return database.NilVersion, false, err
	}
	return ver, dirty, nil
}

func (d *minimalPgxDriver) Drop() error {
	return errors.New("dedup harness: Drop not supported (forward-only)")
}

func (d *minimalPgxDriver) ensureVersionTable() error {
	const ddl = "CREATE TABLE IF NOT EXISTS " + migrationsTable +
		" (version BIGINT NOT NULL PRIMARY KEY, dirty BOOLEAN NOT NULL)"
	_, err := d.conn.Exec(d.ctx, ddl)
	return err
}

var _ database.Driver = (*minimalPgxDriver)(nil)

// ---------------------------------------------------------------------------
// Per-test helpers
// ---------------------------------------------------------------------------

// newUUID mints a fresh UUIDv7. Tests are not under the determinism rule
// (forbidigo is disabled in _test.go), so minting directly is fine.
func newUUID(t *testing.T) string {
	t.Helper()
	u, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid.NewV7: %v", err)
	}
	return u.String()
}

// seedTenantAndSession inserts a tenant row and an active session row via the
// owner connection (bypasses RLS) so the FK constraints are satisfied when the
// app-role Store inserts into tool_executions.
func (h *dedupHarness) seedTenantAndSession(t *testing.T) (tenantID, sessionID string) {
	t.Helper()
	tenantID = newUUID(t)
	sessionID = newUUID(t)
	conn, err := pgx.Connect(context.Background(), h.ownerDSN)
	if err != nil {
		t.Fatalf("owner connect for seed: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(context.Background(),
		"INSERT INTO tenants (id, name) VALUES ($1, 'test-tenant')", tenantID); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if _, err := conn.Exec(context.Background(),
		"INSERT INTO sessions (id, tenant_id) VALUES ($1, $2)", sessionID, tenantID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	return tenantID, sessionID
}
