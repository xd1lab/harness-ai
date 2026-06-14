// Package db holds the orchestrator's pure, dependency-light RLS tenant-context
// helpers ([WithTenant]/[TenantFromContext]): the edge auth places the verified
// tenant id here and the event store's pgx acquire-hook reads it back to run
// SET LOCAL app.current_tenant so RLS scopes every borrowed connection
// (architecture §6.7, §8.2). It imports only the standard library — no pgx — so a
// transport that needs only the tenant context (the client-edge auth interceptor,
// and through it the single-process cmd/boltrope-dev binary) does not drag the
// Postgres driver into its transitive graph. The pgx-based migration runner lives
// in the sibling
// [github.com/xd1lab/harness-ai/internal/orchestrator/infra/dbmigrate] package
// for exactly that reason (ADR-0024).
package db

import (
	"context"
	"errors"
)

// ErrNoTenant is returned by [TenantFromContext] (and surfaced by the event
// store) when the context carries no tenant id. The event store treats this as a
// fail-closed condition: it never appends or reads tenant-scoped rows without a
// tenant, and RLS in the database is the backstop (ADR-0011 §6.7; architecture
// §8.2). Recover it with [errors.Is].
var ErrNoTenant = errors.New("db: no tenant in context")

// tenantCtxKey is the unexported context key under which the verified tenant id
// is carried. It is a distinct type so it cannot collide with another package's
// context key (the standard context-key idiom).
type tenantCtxKey struct{}

// WithTenant returns a child context carrying tenantID as the verified tenant.
// The orchestrator's edge auth places the tenant id resolved from the verified
// principal token here (never a client-supplied field; architecture §8.2); the
// event store's pgx acquire path reads it back via [TenantFromContext] and runs
// SET LOCAL app.current_tenant so RLS scopes every borrowed connection
// (architecture §6.7). It is in the infra/db package because the GUC acquire-hook
// is this package's responsibility (architecture §5.1 infra/db).
func WithTenant(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantCtxKey{}, tenantID)
}

// TenantFromContext returns the verified tenant id carried by ctx, or
// [ErrNoTenant] if none is present (or it is empty). Callers that must scope a
// query to a tenant use it to fail closed rather than issue an unscoped query.
func TenantFromContext(ctx context.Context) (string, error) {
	v, ok := ctx.Value(tenantCtxKey{}).(string)
	if !ok || v == "" {
		return "", ErrNoTenant
	}
	return v, nil
}
