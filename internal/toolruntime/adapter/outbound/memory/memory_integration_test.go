//go:build integration

// RED (TDD) — ADR-0030 AC-7 / AC-13. Integration tests for the pgx-backed
// app.MemoryStore against a real Postgres, proving tenant isolation under RLS:
//   - a value Put as tenant A is invisible to Get/Search performed under tenant B's
//     GUC (RLS hides the row — no cross-tenant read);
//   - a Delete under tenant B does not affect tenant A's row;
//   - a transaction with NO app.current_tenant GUC fails closed (the store's
//     empty-tenant guard / RLS raises rather than returning rows).
//
// The store reads the tenant from the toolruntime tenant-context helper (AC-4), so
// the tests scope each call with tenantctx.WithTenant(...). The harness mirrors the
// dedup integration harness (testcontainer OR external DSN; applies the embedded
// migrations including 0008; grants boltrope_app LOGIN; connects as the non-owner
// RLS-enforcing role).
//
// The memory package, its SimplePool/New constructors, the app.MemoryStore port,
// the tenant helper, and migration 0008 do not exist yet, so this file is expected
// to FAIL to compile (and, once it compiles, to FAIL at runtime) until the feature
// lands (feature absent).
package memory_test

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

	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/memory"
	tenantctx "github.com/xd1lab/harness-ai/internal/toolruntime/infra/tenant"
	"github.com/xd1lab/harness-ai/migrations"
)

const (
	envTestDatabaseURL = "BOLTROPE_TEST_DATABASE_URL"

	appRole     = "boltrope_app"
	appPassword = "boltrope_app_test_pw" //nolint:gosec // ephemeral test-only DB credential, not a secret

	envTestPGImage        = "BOLTROPE_TEST_PG_IMAGE"
	defaultContainerImage = "postgres:16"

	containerDB       = "boltrope"
	containerUser     = "boltrope_owner"
	containerPassword = "owner_pw"

	advisoryLockKey int64 = 0x626f6c74 // "bolt"
	migrationsTable       = "schema_migrations"
)

// memHarness is a provisioned memory-store test environment.
type memHarness struct {
	ownerDSN string
	appDSN   string
	store    *memory.Store
	pool     *memory.SimplePool
	mode     string
}

// newMemHarness provisions Postgres, applies migrations (creates agent_memory via
// 0008), grants the app role LOGIN, and returns a ready harness. Skips (not fails)
// when no DSN is set and Docker is unreachable.
func newMemHarness(t *testing.T) *memHarness {
	t.Helper()
	ctx := context.Background()

	ownerDSN, mode := provisionOwnerDSN(ctx, t)
	if err := runMigrations(ctx, ownerDSN); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	grantAppLogin(ctx, t, ownerDSN)

	appDSN := deriveAppDSN(t, ownerDSN)
	pool, err := memory.NewSimplePool(appDSN)
	if err != nil {
		t.Fatalf("NewSimplePool(app): %v", err)
	}
	t.Cleanup(pool.Close)

	return &memHarness{
		ownerDSN: ownerDSN,
		appDSN:   appDSN,
		store:    memory.New(pool),
		pool:     pool,
		mode:     mode,
	}
}

func provisionOwnerDSN(ctx context.Context, t *testing.T) (string, string) {
	t.Helper()
	if dsn := os.Getenv(envTestDatabaseURL); dsn != "" {
		t.Logf("memory integration: using external DSN from %s", envTestDatabaseURL)
		return dsn, "external-dsn"
	}
	container, err := tcpostgres.Run(ctx, containerImage(),
		tcpostgres.WithDatabase(containerDB),
		tcpostgres.WithUsername(containerUser),
		tcpostgres.WithPassword(containerPassword),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Skipf("memory integration: no %s set and Docker unreachable (testcontainers: %v)", envTestDatabaseURL, err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("container ConnectionString: %v", err)
	}
	return dsn, "testcontainer"
}

func containerImage() string {
	if img := os.Getenv(envTestPGImage); img != "" {
		return img
	}
	return defaultContainerImage
}

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

func deriveAppDSN(t *testing.T, ownerDSN string) string {
	t.Helper()
	u, err := url.Parse(ownerDSN)
	if err != nil {
		t.Fatalf("parse owner DSN: %v", err)
	}
	u.User = url.UserPassword(appRole, appPassword)
	return u.String()
}

// seedTenant inserts a tenant row via the owner connection (bypasses RLS) so the
// agent_memory FK to tenants(id) is satisfied when the app-role Store inserts.
func (h *memHarness) seedTenant(t *testing.T) string {
	t.Helper()
	tenantID := newUUID(t)
	conn, err := pgx.Connect(context.Background(), h.ownerDSN)
	if err != nil {
		t.Fatalf("owner connect for seed: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(context.Background(),
		"INSERT INTO tenants (id, name) VALUES ($1, 'mem-test-tenant')", tenantID); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	return tenantID
}

func newUUID(t *testing.T) string {
	t.Helper()
	u, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid.NewV7: %v", err)
	}
	return u.String()
}

// ---------------------------------------------------------------------------
// Inline migration runner (avoids importing internal/orchestrator/infra/db).
// ---------------------------------------------------------------------------

func runMigrations(ctx context.Context, dsn string) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("memory harness: parse DSN: %w", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("memory harness: connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	src, err := iofs.New(migrations.Source(), ".")
	if err != nil {
		return fmt.Errorf("memory harness: iofs source: %w", err)
	}
	drv := &minimalPgxDriver{ctx: ctx, conn: conn}
	m, err := migrate.NewWithInstance("iofs", src, "pgx-memory-harness", drv)
	if err != nil {
		return fmt.Errorf("memory harness: build migrator: %w", err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("memory harness: apply migrations: %w", err)
	}
	return nil
}

type minimalPgxDriver struct {
	ctx  context.Context //nolint:containedctx // migrate.Driver has no ctx params; runner ctx carried here.
	conn *pgx.Conn
}

func (d *minimalPgxDriver) Open(string) (database.Driver, error) {
	return nil, errors.New("memory harness: pgxDriver is instance-only")
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
	return errors.New("memory harness: Drop not supported (forward-only)")
}
func (d *minimalPgxDriver) ensureVersionTable() error {
	const ddl = "CREATE TABLE IF NOT EXISTS " + migrationsTable +
		" (version BIGINT NOT NULL PRIMARY KEY, dirty BOOLEAN NOT NULL)"
	_, err := d.conn.Exec(d.ctx, ddl)
	return err
}

var _ database.Driver = (*minimalPgxDriver)(nil)

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestPutGetWithinTenant is the baseline: a value Put under a tenant is read back
// within that tenant's scope.
func TestPutGetWithinTenant(t *testing.T) {
	h := newMemHarness(t)
	t.Logf("mode: %s", h.mode)

	tenantA := h.seedTenant(t)
	ctxA := tenantctx.WithTenant(context.Background(), tenantA)

	if err := h.store.Put(ctxA, "default", "k", "value-A", []string{"t1"}); err != nil {
		t.Fatalf("Put as A: %v", err)
	}
	got, ok, err := h.store.Get(ctxA, "default", "k")
	if err != nil {
		t.Fatalf("Get as A: %v", err)
	}
	if !ok {
		t.Fatal("Get as A: ok=false, want the written row")
	}
	if got.Value != "value-A" {
		t.Errorf("Get value = %q, want %q", got.Value, "value-A")
	}
}

// TestTenantIsolationGetUnderRLS proves a Get under tenant B never sees tenant A's
// row — RLS (FORCE ROW LEVEL SECURITY) hides it.
func TestTenantIsolationGetUnderRLS(t *testing.T) {
	h := newMemHarness(t)

	tenantA := h.seedTenant(t)
	tenantB := h.seedTenant(t)
	ctxA := tenantctx.WithTenant(context.Background(), tenantA)
	ctxB := tenantctx.WithTenant(context.Background(), tenantB)

	if err := h.store.Put(ctxA, "default", "secret", "A-only", nil); err != nil {
		t.Fatalf("Put as A: %v", err)
	}
	_, ok, err := h.store.Get(ctxB, "default", "secret")
	if err != nil {
		t.Fatalf("Get as B: unexpected error %v", err)
	}
	if ok {
		t.Fatal("tenant B read tenant A's memory under RLS — cross-tenant isolation FAILED")
	}
}

// TestTenantIsolationSearchUnderRLS proves Search under tenant B returns zero of
// tenant A's rows.
func TestTenantIsolationSearchUnderRLS(t *testing.T) {
	h := newMemHarness(t)

	tenantA := h.seedTenant(t)
	tenantB := h.seedTenant(t)
	ctxA := tenantctx.WithTenant(context.Background(), tenantA)
	ctxB := tenantctx.WithTenant(context.Background(), tenantB)

	if err := h.store.Put(ctxA, "default", "k", "find-me-substring", []string{"shared"}); err != nil {
		t.Fatalf("Put as A: %v", err)
	}
	hits, err := h.store.Search(ctxB, "find-me-substring", nil, 0)
	if err != nil {
		t.Fatalf("Search as B: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("tenant B Search saw %d of tenant A's rows — isolation FAILED", len(hits))
	}
}

// TestTenantIsolationDeleteUnderRLS proves a Delete under tenant B does not remove
// tenant A's row (RLS scopes the DELETE).
func TestTenantIsolationDeleteUnderRLS(t *testing.T) {
	h := newMemHarness(t)

	tenantA := h.seedTenant(t)
	tenantB := h.seedTenant(t)
	ctxA := tenantctx.WithTenant(context.Background(), tenantA)
	ctxB := tenantctx.WithTenant(context.Background(), tenantB)

	if err := h.store.Put(ctxA, "default", "k", "A-value", nil); err != nil {
		t.Fatalf("Put as A: %v", err)
	}
	if err := h.store.Delete(ctxB, "default", "k"); err != nil {
		t.Fatalf("Delete as B: %v", err)
	}
	got, ok, err := h.store.Get(ctxA, "default", "k")
	if err != nil {
		t.Fatalf("Get as A after B delete: %v", err)
	}
	if !ok {
		t.Fatal("tenant A's row was removed by tenant B's delete — isolation FAILED")
	}
	if got.Value != "A-value" {
		t.Errorf("A's value = %q after B delete, want %q", got.Value, "A-value")
	}
}

// TestSearchTagAndUnderRLS proves the tag-AND filter (tags @> $) works within a
// tenant: requiring two tags matches only the row carrying both.
func TestSearchTagAndUnderRLS(t *testing.T) {
	h := newMemHarness(t)

	tenantA := h.seedTenant(t)
	ctxA := tenantctx.WithTenant(context.Background(), tenantA)

	if err := h.store.Put(ctxA, "default", "k1", "v1", []string{"red", "blue"}); err != nil {
		t.Fatalf("Put k1: %v", err)
	}
	if err := h.store.Put(ctxA, "default", "k2", "v2", []string{"red"}); err != nil {
		t.Fatalf("Put k2: %v", err)
	}
	hits, err := h.store.Search(ctxA, "", []string{"red", "blue"}, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 || hits[0].Key != "k1" {
		t.Fatalf("Search tags [red,blue] = %d hits (%v), want exactly k1", len(hits), hits)
	}
}

// TestMissingTenantGUCFailsClosed proves a call with NO tenant in the context fails
// closed: the store's empty-tenant guard (or the RLS current_setting without
// missing_ok) raises rather than returning rows or silently succeeding.
func TestMissingTenantGUCFailsClosed(t *testing.T) {
	h := newMemHarness(t)

	bg := context.Background() // no WithTenant — no tenant in context.

	if err := h.store.Put(bg, "default", "k", "v", nil); err == nil {
		t.Error("Put with no tenant in ctx: expected fail-closed error, got nil")
	}
	if _, _, err := h.store.Get(bg, "default", "k"); err == nil {
		t.Error("Get with no tenant in ctx: expected fail-closed error, got nil")
	}
	if _, err := h.store.Search(bg, "x", nil, 0); err == nil {
		t.Error("Search with no tenant in ctx: expected fail-closed error, got nil")
	}
	if err := h.store.Delete(bg, "default", "k"); err == nil {
		t.Error("Delete with no tenant in ctx: expected fail-closed error, got nil")
	}
}

// keep pgx import live even if a future refactor drops a direct use.
var _ = pgx.ErrNoRows
