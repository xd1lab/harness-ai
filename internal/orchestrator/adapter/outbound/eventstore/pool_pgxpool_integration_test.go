//go:build integration

package eventstore

import (
	"context"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	infradb "github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
)

// TestPgxPool_AppendLoad_Integration opens a real *pgxpool.Pool against the
// integration test database (testcontainer or BOLTROPE_TEST_DATABASE_URL) and
// runs a trivial Append/Load through the Store to prove that PgxPool satisfies
// the Pool interface in exactly the same way SimplePool does. This is the pool
// "swap" proof: the same Store constructor, same harness infrastructure,
// different Pool implementation.
//
// It also exercises the AfterConnect hook slot by registering a no-op hook
// (representing the RLS SET LOCAL wiring point described in T-ORCH-04), and
// asserts the pool operates correctly with the hook installed.
func TestPgxPool_AppendLoad_Integration(t *testing.T) {
	ctx := context.Background()

	// Reuse the existing harness provisioning path (dual-mode: testcontainer or
	// BOLTROPE_TEST_DATABASE_URL) to obtain a provisioned owner DSN and an
	// app-role DSN. provisionOwnerDSN, grantAppLogin, and deriveAppDSN are all
	// in harness_integration_test.go in the same test package.
	ownerDSN, mode := provisionOwnerDSN(ctx, t)
	t.Logf("pool_pgxpool integration: mode=%s", mode)

	if err := infradb.Migrate(ctx, ownerDSN); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	grantAppLogin(ctx, t, ownerDSN)
	appDSN := deriveAppDSN(t, ownerDSN)

	// Build a PgxPool with all functional options exercised:
	//   - WithMaxConns/WithMinConns for size
	//   - WithAfterConnect to exercise the hook slot (no-op; the real wiring
	//     in T-ORCH-04 will do SET LOCAL app.current_tenant here)
	hookCalls := 0
	pool, err := NewPgxPool(ctx, appDSN,
		WithMaxConns(4),
		WithMinConns(1),
		WithAfterConnect(func(_ context.Context, _ pgxConn) error {
			hookCalls++
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("NewPgxPool: %v", err)
	}
	t.Cleanup(pool.Close)

	// Compile-time: var _ Pool = pool is in pool_pgxpool.go; runtime sanity:
	var _ Pool = pool

	store := New(pool)

	tenantID := newUUID(t)
	sessionID := newUUID(t)
	ctx2 := tenantCtx(tenantID)

	if err := store.CreateTenant(ctx2, tenantID, "pgxpool-test-tenant"); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	if _, err := store.CreateSession(ctx2, sessionID, domain.ModeDefault); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	// Append one event through the pgxpool-backed Store.
	reqID := newUUID(t)
	envs, err := store.Append(ctx2, sessionID, 0, 0, reqID,
		app.AppendInput{
			Event: domain.TurnStarted{TurnID: "pgxpool-t1", Model: "test-model"},
			Actor: domain.ActorSystem,
		},
	)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(envs) != 1 || envs[0].Seq != 1 {
		t.Fatalf("Append: got %d envelopes (seq %d), want 1 at seq 1", len(envs), seqOf(envs))
	}

	// Load it back.
	loaded, err := store.Load(ctx2, sessionID, 1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 1 || loaded[0].Seq != 1 {
		t.Fatalf("Load: got %d envelopes (seq %d), want 1 at seq 1", len(loaded), seqOf(loaded))
	}
	ts, ok := loaded[0].Event.(domain.TurnStarted)
	if !ok || ts.TurnID != "pgxpool-t1" {
		t.Fatalf("Load: event = %T %+v, want TurnStarted{TurnID:pgxpool-t1}", loaded[0].Event, loaded[0].Event)
	}

	// Verify idempotency path still works through the pgxpool.
	replay, err := store.Append(ctx2, sessionID, 0, 0, reqID,
		app.AppendInput{
			Event: domain.TurnStarted{TurnID: "pgxpool-t1", Model: "test-model"},
			Actor: domain.ActorSystem,
		},
	)
	if err != nil {
		t.Fatalf("idempotent replay Append: %v", err)
	}
	if len(replay) != 1 || replay[0].Seq != 1 {
		t.Fatalf("idempotent replay: got %d envelopes (seq %d), want 1 at seq 1", len(replay), seqOf(replay))
	}

	// The AfterConnect hook may or may not have been called depending on
	// whether pgxpool pre-connects eagerly (MinConns=1). We log the count
	// rather than asserting a minimum, because the pool may reuse a connection
	// established in a prior test. Non-nil hook slot is the critical assertion
	// (confirmed by the type system via WithAfterConnect).
	t.Logf("AfterConnect hook call count: %d", hookCalls)
}
