// Package tenant holds the toolruntime's pure, dependency-light RLS
// tenant-context helpers ([WithTenant]/[TenantFromContext]): execute.Service
// places the request's verified tenant id here before invoking a tool, and the
// tenant-scoped memory store reads it back to run
// SELECT set_config('app.current_tenant', …, true) so RLS scopes every borrowed
// connection (ADR-0030).
//
// It imports only the standard library — no pgx — so the single-process
// cmd/boltrope-dev binary (which is fenced from pgx) can carry the tenant
// context through the in-memory memory store without dragging the Postgres
// driver into its transitive graph.
//
// This package deliberately does NOT import the orchestrator's
// [github.com/xd1lab/harness-ai/internal/orchestrator/infra/db] helper even
// though it mirrors its semantics: a toolruntime package importing an
// orchestrator package would be a layering violation. The two helpers are
// independent by design (ADR-0030).
package tenant

import (
	"context"
	"errors"
)

// ErrNoTenant is returned by [TenantFromContext] when the context carries no
// tenant id (absent or empty). Tenant-scoped stores treat this as a fail-closed
// condition: they never read or mutate tenant-scoped rows without a tenant, and
// RLS in the database is the backstop (ADR-0030). Recover it with [errors.Is].
var ErrNoTenant = errors.New("toolruntime/tenant: no tenant in context")

// tenantCtxKey is the unexported context key under which the verified tenant id
// is carried. It is a distinct type so it cannot collide with another package's
// context key (the standard context-key idiom).
type tenantCtxKey struct{}

// WithTenant returns a child context carrying tenantID as the verified tenant.
// execute.Service places the request's tenant id here (resolved upstream from
// the verified principal, never a tool-supplied field) before calling
// Tool.Execute; the tenant-scoped memory store reads it back via
// [TenantFromContext] to run set_config('app.current_tenant', …) so RLS scopes
// the query.
func WithTenant(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantCtxKey{}, tenantID)
}

// TenantFromContext returns the verified tenant id carried by ctx, or
// [ErrNoTenant] if none is present (or it is empty). Tenant-scoped stores use it
// to fail closed rather than issue an unscoped query.
func TenantFromContext(ctx context.Context) (string, error) {
	v, ok := ctx.Value(tenantCtxKey{}).(string)
	if !ok || v == "" {
		return "", ErrNoTenant
	}
	return v, nil
}
