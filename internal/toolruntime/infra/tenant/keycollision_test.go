package tenant_test

// ADR-0030 AC-4 — key non-collision. Proves the helper's unexported
// context-key type cannot be read back from (or clobbered by) a same-shaped
// key defined in an unrelated package (the standard context-key idiom).

import (
	"context"
	"errors"
	"testing"

	tenantctx "github.com/xd1lab/harness-ai/internal/toolruntime/infra/tenant"
)

// foreignKey reproduces the standard context-key idiom from an unrelated
// package: an identically-shaped empty struct type.
type foreignKey struct{}

func TestTenantFromContextKeyNonCollision(t *testing.T) {
	t.Parallel()

	// A value stored under a different (but identically-shaped) unexported key
	// must NOT be read back as a tenant.
	ctx := context.WithValue(context.Background(), foreignKey{}, "not-a-tenant")
	if _, err := tenantctx.TenantFromContext(ctx); !errors.Is(err, tenantctx.ErrNoTenant) {
		t.Fatalf("TenantFromContext over foreign key = %v, want ErrNoTenant (key collision?)", err)
	}

	// And WithTenant must not be observable under the foreign key, nor disturb it.
	ctx = tenantctx.WithTenant(ctx, "tenant-A")
	if v, _ := ctx.Value(foreignKey{}).(string); v != "not-a-tenant" {
		t.Fatalf("foreign key value after WithTenant = %q, want %q", v, "not-a-tenant")
	}
	got, err := tenantctx.TenantFromContext(ctx)
	if err != nil {
		t.Fatalf("TenantFromContext: unexpected error %v", err)
	}
	if got != "tenant-A" {
		t.Fatalf("TenantFromContext = %q, want %q", got, "tenant-A")
	}
}
