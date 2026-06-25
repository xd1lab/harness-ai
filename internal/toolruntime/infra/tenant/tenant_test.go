package tenant_test

// RED (TDD) — ADR-0030 AC-4. These tests pin the contract of the NEW clean
// (stdlib-only) toolruntime tenant-context helper package
// internal/toolruntime/infra/tenant: WithTenant / TenantFromContext / ErrNoTenant.
// It mirrors internal/orchestrator/infra/db.WithTenant/TenantFromContext semantics
// but lives in its OWN toolruntime package so the tool-runtime never imports the
// orchestrator package (a layering violation). These tests are expected to FAIL to
// compile until the helper package exists (feature absent).

import (
	"context"
	"errors"
	"testing"

	tenantctx "github.com/xd1lab/harness-ai/internal/toolruntime/infra/tenant"
)

// TestWithTenantRoundTrips verifies a tenant id placed via WithTenant is read back
// by TenantFromContext unchanged.
func TestWithTenantRoundTrips(t *testing.T) {
	t.Parallel()

	ctx := tenantctx.WithTenant(context.Background(), "tenant-A")
	got, err := tenantctx.TenantFromContext(ctx)
	if err != nil {
		t.Fatalf("TenantFromContext after WithTenant: unexpected error %v", err)
	}
	if got != "tenant-A" {
		t.Errorf("TenantFromContext = %q, want %q", got, "tenant-A")
	}
}

// TestTenantFromContextAbsentFailsClosed verifies a bare context with no tenant
// returns ErrNoTenant (fail-closed), never an empty string with a nil error.
func TestTenantFromContextAbsentFailsClosed(t *testing.T) {
	t.Parallel()

	_, err := tenantctx.TenantFromContext(context.Background())
	if !errors.Is(err, tenantctx.ErrNoTenant) {
		t.Errorf("TenantFromContext(background) error = %v, want errors.Is(_, ErrNoTenant)", err)
	}
}

// TestTenantFromContextEmptyFailsClosed verifies an explicitly-empty tenant id is
// treated as absent (fail-closed) — mirrors the orchestrator helper.
func TestTenantFromContextEmptyFailsClosed(t *testing.T) {
	t.Parallel()

	ctx := tenantctx.WithTenant(context.Background(), "")
	_, err := tenantctx.TenantFromContext(ctx)
	if !errors.Is(err, tenantctx.ErrNoTenant) {
		t.Errorf("TenantFromContext(empty tenant) error = %v, want errors.Is(_, ErrNoTenant)", err)
	}
}

// TestWithTenantOverrides verifies the innermost WithTenant wins (a re-scoped ctx).
func TestWithTenantOverrides(t *testing.T) {
	t.Parallel()

	ctx := tenantctx.WithTenant(context.Background(), "tenant-A")
	ctx = tenantctx.WithTenant(ctx, "tenant-B")
	got, err := tenantctx.TenantFromContext(ctx)
	if err != nil {
		t.Fatalf("TenantFromContext: unexpected error %v", err)
	}
	if got != "tenant-B" {
		t.Errorf("TenantFromContext = %q, want innermost %q", got, "tenant-B")
	}
}
