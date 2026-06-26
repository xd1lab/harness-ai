//go:build integration

// Package eventstore integration-test harness (dual-mode). It provisions a live
// PostgreSQL, applies the embedded migrations, and returns an OWNER connection
// (for setup/assertions) plus the non-owner app-role pool the [Store] uses so
// RLS is actually enforced (a superuser/owner bypasses RLS even under FORCE; the
// app role does not).
//
// Mode selection (per the task spec):
//   - if BOLTROPE_TEST_DATABASE_URL is set, it is used as the OWNER DSN (the
//     coordinator runs integration centrally against a managed Postgres);
//   - otherwise a Postgres container is started via testcontainers-go (image
//     from BOLTROPE_TEST_PG_IMAGE, default postgres:16).
//
// If neither a DSN is set nor Docker is reachable, the harness calls t.Skip with
// a clear reason (the tests still compile and are delivered; the coordinator runs
// them where Docker/DSN is available).
package eventstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	infradb "github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
	"github.com/xd1lab/harness-ai/internal/orchestrator/infra/dbmigrate"
)

const (
	// envTestDatabaseURL, when set, is the OWNER DSN the harness uses instead of
	// starting a container (dual-mode).
	envTestDatabaseURL = "BOLTROPE_TEST_DATABASE_URL"

	// appRole / appPassword are the non-owner application role the harness grants
	// LOGIN to so the [Store] connects as it and RLS is enforced. The migration
	// creates the role (NOBYPASSRLS); the harness only adds a login credential.
	// The password is a fixed, throwaway value for an ephemeral test database
	// (a container or a coordinator-managed test DSN), never a real credential.
	appRole     = "boltrope_app"
	appPassword = "boltrope_app_test_pw" //nolint:gosec // ephemeral test-only DB credential, not a secret

	// envTestPGImage overrides the Postgres image for the testcontainer mode.
	// NFR-PORT-03 pins the supported floor at PostgreSQL 13 (xid8 /
	// pg_current_xact_id), so the floor must be testable without editing the
	// harness: set BOLTROPE_TEST_PG_IMAGE=postgres:13 to run the floor proof.
	envTestPGImage = "BOLTROPE_TEST_PG_IMAGE"
	// defaultContainerImage is the image used when no override is set.
	defaultContainerImage = "postgres:16"
	// containerDB / containerUser / containerPassword are the superuser/owner
	// credentials for the started container.
	containerDB       = "boltrope"
	containerUser     = "boltrope_owner"
	containerPassword = "owner_pw"
)

// harness is a provisioned event-store test environment.
type harness struct {
	ownerDSN string
	appDSN   string
	// store is the Store over the non-owner app-role pool (RLS-enforced).
	store *Store
	pool  *SimplePool
	mode  string // "external-dsn" or "testcontainer"
}

// newHarness provisions Postgres (dual-mode), applies migrations, grants the app
// role LOGIN, and returns a ready [harness]. It registers cleanup with t. It
// skips the test (t.Skip) when no DSN is set and Docker is unreachable.
func newHarness(t *testing.T) *harness {
	t.Helper()
	ctx := context.Background()

	ownerDSN, mode := provisionOwnerDSN(ctx, t)

	// Apply the embedded migrations as the owner. This also creates the
	// boltrope_app role (NOBYPASSRLS).
	if err := dbmigrate.Migrate(ctx, ownerDSN); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Grant the app role a login credential so the Store can connect as it
	// (the migration intentionally leaves it NOLOGIN/credential-less).
	grantAppLogin(ctx, t, ownerDSN)

	appDSN := deriveAppDSN(t, ownerDSN)
	pool, err := NewSimplePool(appDSN)
	if err != nil {
		t.Fatalf("NewSimplePool(app): %v", err)
	}
	t.Cleanup(pool.Close)

	// Operator pool over the OWNER DSN: the operator-tier, RLS-exempt reads
	// (VerifyAuditCheckpoints) need a connection that bypasses events' RLS to read
	// the GLOBAL audit_checkpoints + events.content_hash across tenants. The owner
	// connection (a superuser in container mode; the table owner under FORCE RLS in
	// a managed DSN) is that connection. The application pool above stays the
	// NOBYPASSRLS role so every tenant-scoped path is still RLS-enforced.
	operatorPool, err := NewSimplePool(ownerDSN)
	if err != nil {
		t.Fatalf("NewSimplePool(operator): %v", err)
	}
	t.Cleanup(operatorPool.Close)

	return &harness{
		ownerDSN: ownerDSN,
		appDSN:   appDSN,
		store:    NewWithOperator(pool, operatorPool),
		pool:     pool,
		mode:     mode,
	}
}

// provisionOwnerDSN returns an owner DSN and the mode label, starting a container
// when no external DSN is configured. It skips (not fails) when Docker is
// unreachable in container mode so the suite is deliverable without Docker.
func provisionOwnerDSN(ctx context.Context, t *testing.T) (string, string) {
	t.Helper()
	if dsn := os.Getenv(envTestDatabaseURL); dsn != "" {
		t.Logf("eventstore integration: using external DSN from %s", envTestDatabaseURL)
		return dsn, "external-dsn"
	}

	container, err := tcpostgres.Run(ctx, containerImage(),
		tcpostgres.WithDatabase(containerDB),
		tcpostgres.WithUsername(containerUser),
		tcpostgres.WithPassword(containerPassword),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Skipf("eventstore integration: no %s set and Docker unreachable (testcontainers: %v)", envTestDatabaseURL, err)
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

// grantAppLogin gives the boltrope_app role a login credential as the owner so
// the Store's app pool can connect as the non-owner, RLS-bound role.
func grantAppLogin(ctx context.Context, t *testing.T, ownerDSN string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, ownerDSN)
	if err != nil {
		t.Fatalf("owner connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER ROLE %s WITH LOGIN PASSWORD '%s'", appRole, appPassword)); err != nil {
		t.Fatalf("grant app login: %v", err)
	}
}

// deriveAppDSN rewrites ownerDSN's user/password to the app role so the Store
// connects as boltrope_app. It assumes a URL-form DSN (the form both
// testcontainers and a typical BOLTROPE_TEST_DATABASE_URL use).
func deriveAppDSN(t *testing.T, ownerDSN string) string {
	t.Helper()
	u, err := url.Parse(ownerDSN)
	if err != nil {
		t.Fatalf("parse owner DSN as URL (app DSN derivation needs URL form): %v", err)
	}
	u.User = url.UserPassword(appRole, appPassword)
	return u.String()
}

// ---------------------------------------------------------------------------
// Per-test fixtures
// ---------------------------------------------------------------------------

// newUUID returns a fresh UUID string for test ids. Tests are not under the
// determinism rule (forbidigo is disabled in _test.go), so minting here is fine.
func newUUID(t *testing.T) string {
	t.Helper()
	// Use the same UUIDv7 source the platform ids.System uses, via pgx-free path:
	// crypto-quality uniqueness is all the tests need.
	return mustUUIDv7(t)
}

// tenantCtx returns a context scoped to tenantID (the verified-tenant carrier the
// Store reads). Backed by infra/db.WithTenant so it exercises the real path.
func tenantCtx(tenantID string) context.Context {
	return infradb.WithTenant(context.Background(), tenantID)
}

// seedTenantAndSession creates a tenant and one active session under it, both via
// the Store (so RLS WITH CHECK is exercised). It returns (tenantID, sessionID).
func (h *harness) seedTenantAndSession(t *testing.T) (string, string) {
	t.Helper()
	tenantID := newUUID(t)
	sessionID := newUUID(t)
	ctx := tenantCtx(tenantID)
	if err := h.store.CreateTenant(ctx, tenantID, "test-tenant"); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	if _, err := h.store.CreateSession(ctx, sessionID, domain.ModeDefault); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	return tenantID, sessionID
}

// ownerConn opens a fresh owner connection for setup/assertions that must bypass
// RLS (e.g. seeding two tenants' rows, or the predicate-removed proof's
// cross-tenant insert). The caller closes it.
func (h *harness) ownerConn(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), h.ownerDSN)
	if err != nil {
		t.Fatalf("owner connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// waitFor polls cond up to timeout, returning true if it became true. It avoids
// a fixed sleep when waiting for an async subscription delivery.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// errIsAny reports whether err matches any of the given sentinels via errors.Is.
func errIsAny(err error, sentinels ...error) bool {
	for _, s := range sentinels {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
}
