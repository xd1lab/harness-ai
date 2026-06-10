package db_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
)

// TestTenantContextRoundTrip asserts a tenant placed with WithTenant is read back
// by TenantFromContext, and that a missing or empty tenant fails closed with
// ErrNoTenant (the RLS pre-condition; architecture §6.7, §8.2).
func TestTenantContextRoundTrip(t *testing.T) {
	t.Parallel()

	const tenant = "11111111-1111-1111-1111-111111111111"
	ctx := db.WithTenant(context.Background(), tenant)
	got, err := db.TenantFromContext(ctx)
	if err != nil {
		t.Fatalf("TenantFromContext after WithTenant: unexpected error %v", err)
	}
	if got != tenant {
		t.Fatalf("TenantFromContext = %q, want %q", got, tenant)
	}

	if _, err := db.TenantFromContext(context.Background()); !errors.Is(err, db.ErrNoTenant) {
		t.Fatalf("TenantFromContext on bare context: got %v, want ErrNoTenant", err)
	}

	if _, err := db.TenantFromContext(db.WithTenant(context.Background(), "")); !errors.Is(err, db.ErrNoTenant) {
		t.Fatalf("TenantFromContext on empty tenant: got %v, want ErrNoTenant", err)
	}
}
