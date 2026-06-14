//go:build integration

// Package projection integration-test harness (dual-mode), mirroring the
// eventstore harness. It provisions a live PostgreSQL, applies the embedded
// migrations, and returns an OWNER (superuser) connection the projection
// [Source]/[Sweeper] use — the projector is operator-tier and reads the GLOBAL
// feed across tenants, so it connects as a role that bypasses RLS (a superuser
// does, even under FORCE ROW LEVEL SECURITY).
//
// Mode selection (per the task spec):
//   - if BOLTROPE_TEST_DATABASE_URL is set, it is used as the OWNER DSN;
//   - otherwise a Postgres container is started via testcontainers-go (image
//     from BOLTROPE_TEST_PG_IMAGE, default postgres:16).
//
// If neither a DSN is set nor Docker is reachable, the harness calls t.Skip.
package projection

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/xd1lab/harness-ai/internal/orchestrator/infra/dbmigrate"
)

const (
	envTestDatabaseURL = "BOLTROPE_TEST_DATABASE_URL"

	// envTestPGImage overrides the Postgres image for the testcontainer mode.
	// NFR-PORT-03 pins the supported floor at PostgreSQL 13 (xid8 /
	// pg_current_xact_id), so set BOLTROPE_TEST_PG_IMAGE=postgres:13 to run
	// the floor proof without editing the harness.
	envTestPGImage        = "BOLTROPE_TEST_PG_IMAGE"
	defaultContainerImage = "postgres:16"

	containerDB       = "boltrope"
	containerUser     = "boltrope_owner"
	containerPassword = "owner_pw"
)

// pharness is a provisioned projection test environment over an owner connection.
type pharness struct {
	ownerDSN string
	conn     *pgx.Conn // owner/superuser connection (bypasses RLS; operator-tier)
	mode     string
}

// newHarness provisions Postgres (dual-mode), applies migrations, and opens an
// owner connection. It registers cleanup with t and skips when no DSN is set and
// Docker is unreachable.
func newHarness(t *testing.T) *pharness {
	t.Helper()
	ctx := context.Background()
	ownerDSN, mode := provisionOwnerDSN(ctx, t)

	if err := dbmigrate.Migrate(ctx, ownerDSN); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	conn, err := pgx.Connect(ctx, ownerDSN)
	if err != nil {
		t.Fatalf("owner connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	return &pharness{ownerDSN: ownerDSN, conn: conn, mode: mode}
}

// provisionOwnerDSN returns an owner DSN and a mode label, starting a container
// when no external DSN is configured. It skips (not fails) when Docker is
// unreachable in container mode.
func provisionOwnerDSN(ctx context.Context, t *testing.T) (string, string) {
	t.Helper()
	if dsn := os.Getenv(envTestDatabaseURL); dsn != "" {
		t.Logf("projection integration: using external DSN from %s", envTestDatabaseURL)
		return dsn, "external-dsn"
	}
	container, err := tcpostgres.Run(ctx, containerImage(),
		tcpostgres.WithDatabase(containerDB),
		tcpostgres.WithUsername(containerUser),
		tcpostgres.WithPassword(containerPassword),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Skipf("projection integration: no %s set and Docker unreachable (testcontainers: %v)", envTestDatabaseURL, err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

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

// newConn opens a fresh owner connection (for a second/long-running transaction
// that controls the snapshot xmin). The caller closes it via t.Cleanup.
func (h *pharness) newConn(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), h.ownerDSN)
	if err != nil {
		t.Fatalf("owner connect (second conn): %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// newUUID returns a fresh UUIDv7 string for test ids.
func newUUID(t *testing.T) string {
	t.Helper()
	u, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid.NewV7: %v", err)
	}
	return u.String()
}

// seedTenantSession inserts a tenant and an active session via the owner
// connection (RLS-bypassing) and returns their ids.
func (h *pharness) seedTenantSession(t *testing.T, tenantID, sessionID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := h.conn.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING", tenantID, "proj-test"); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if _, err := h.conn.Exec(ctx,
		"INSERT INTO sessions (id, tenant_id, status, head_seq, lease_epoch) VALUES ($1, $2, 'active', 0, 0)",
		sessionID, tenantID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}

// insertEventTx inserts one event row on the given querier (a conn or a tx),
// letting transaction_id default to the querier's current transaction id. seq is
// the per-session sequence; payload is the JSONB body. It returns nothing — the
// projector reads the row back by its own query.
func insertEventTx(ctx context.Context, t *testing.T, q querier, tenantID, sessionID string, seq int64, eventType string, payload []byte) {
	t.Helper()
	_, err := q.Exec(ctx, `
		INSERT INTO events (tenant_id, session_id, seq, request_id, event_type, schema_version, payload)
		VALUES ($1, $2, $3, $4, $5, 1, $6)`,
		tenantID, sessionID, seq, newUUID(t), eventType, payload)
	if err != nil {
		t.Fatalf("insert event seq=%d: %v", seq, err)
	}
}

// querier is the minimal Exec surface shared by *pgx.Conn and pgx.Tx for the
// event-insert helper.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// waitFor polls cond up to timeout, returning true if it became true.
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
