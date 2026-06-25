package memory

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	tenantctx "github.com/xd1lab/harness-ai/internal/toolruntime/infra/tenant"
)

// Store is the pgx-backed [app.MemoryStore] implementation: the durable,
// tenant-scoped long-term agent memory over the agent_memory table (ADR-0030;
// migration 0008). It is safe for concurrent use — each method acquires an
// independent connection and transaction.
//
// # Tenant isolation
//
// The tenant id is taken from the request context via
// [tenantctx.TenantFromContext] (NEVER a method argument). On every transaction
// the store runs SELECT set_config('app.current_tenant', …, true) so RLS (FORCE
// ROW LEVEL SECURITY) on the non-owner application role constrains every query
// to that tenant's rows. A missing tenant fails closed — [tenantctx.ErrNoTenant]
// is returned before any query runs, and the database's current_setting (no
// missing_ok flag) is the backstop (migration 0008; ADR-0030).
type Store struct {
	pool Pool
}

// New returns a [Store] over pool. The caller owns pool's lifecycle (Close), as
// a single pool is typically shared across the tool-runtime process.
func New(pool Pool) *Store {
	return &Store{pool: pool}
}

// Compile-time assertion that *Store satisfies the MemoryStore port.
var _ app.MemoryStore = (*Store)(nil)

// setLocalTenant scopes tx to tenantID for the remainder of the transaction.
// Uses the parameterized set_config function (not SET LOCAL with string
// interpolation) so a malformed tenant cannot inject SQL.
func setLocalTenant(ctx context.Context, tx pgx.Tx, tenantID string) error {
	if _, err := tx.Exec(ctx, "SELECT set_config('app.current_tenant', $1, true)", tenantID); err != nil {
		return fmt.Errorf("memory: setting tenant GUC: %w", err)
	}
	return nil
}

// beginTenantTx reads the verified tenant from ctx, acquires a connection,
// begins a transaction, and scopes it to that tenant. Returns tx, a cleanup
// func (defer it), and any error. It fails closed when no tenant is present and
// releases what it acquired on any error.
func (s *Store) beginTenantTx(ctx context.Context) (pgx.Tx, func(), error) {
	tenantID, err := tenantctx.TenantFromContext(ctx)
	if err != nil {
		// Fail closed: no verified tenant, no transaction.
		return nil, nil, fmt.Errorf("memory: %w", err)
	}
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	tx, err := pc.Begin(ctx)
	if err != nil {
		pc.Release()
		return nil, nil, fmt.Errorf("memory: begin tx: %w", err)
	}
	if err := setLocalTenant(ctx, tx, tenantID); err != nil {
		_ = tx.Rollback(ctx)
		pc.Release()
		return nil, nil, err
	}
	cleanup := func() {
		_ = tx.Rollback(ctx) // no-op if already committed
		pc.Release()
	}
	return tx, cleanup, nil
}

// Put UPSERTs value (and tags) under (namespace, key) for the context's tenant.
// It inserts a new entry or overwrites the value/tags of an existing one and
// bumps updated_at. It fails closed when the context carries no tenant.
//
// tags is normalised to a non-nil slice so the column receives '{}' rather than
// NULL (the column is NOT NULL DEFAULT '{}').
func (s *Store) Put(ctx context.Context, namespace, key, value string, tags []string) error {
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	if tags == nil {
		tags = []string{}
	}

	// tenant_id is taken from the GUC so RLS WITH CHECK accepts the row and the
	// PRIMARY KEY (tenant_id, namespace, mem_key) is honored.
	_, err = tx.Exec(ctx, `
		INSERT INTO agent_memory (tenant_id, namespace, mem_key, value, tags)
		VALUES (current_setting('app.current_tenant')::uuid, $1, $2, $3, $4)
		ON CONFLICT (tenant_id, namespace, mem_key)
		DO UPDATE SET value = EXCLUDED.value, tags = EXCLUDED.tags, updated_at = now()`,
		namespace, key, value, tags,
	)
	if err != nil {
		return fmt.Errorf("memory: Put upsert: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("memory: Put commit: %w", err)
	}
	return nil
}

// Get returns the entry stored under (namespace, key) for the context's tenant.
// found is false (nil error) when no such entry exists — a miss is a normal
// outcome, not an error. RLS-invisible rows are also "not found" (no existence
// oracle). It fails closed when the context carries no tenant.
func (s *Store) Get(ctx context.Context, namespace, key string) (app.MemoryEntry, bool, error) {
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return app.MemoryEntry{}, false, err
	}
	defer cleanup()

	var entry app.MemoryEntry
	err = tx.QueryRow(ctx, `
		SELECT namespace, mem_key, value, tags, created_at, updated_at
		  FROM agent_memory
		 WHERE namespace = $1 AND mem_key = $2`,
		namespace, key,
	).Scan(&entry.Namespace, &entry.Key, &entry.Value, &entry.Tags, &entry.CreatedAt, &entry.UpdatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return app.MemoryEntry{}, false, nil
	}
	if err != nil {
		return app.MemoryEntry{}, false, fmt.Errorf("memory: Get: %w", err)
	}
	// Read-only path; deferred rollback is a clean no-op.
	return entry, true, nil
}

// Search returns the context tenant's entries matching the filters: query is a
// case-insensitive SUBSTRING over the entry value (value only — the pinned
// recall surface), and every tag in tags must be present (tag AND-semantics via
// tags @> $). An empty query and empty tags lists recent entries (newest
// first). limit caps the result count; limit <= 0 applies
// [app.DefaultMemorySearchLimit] and any larger value is hard-capped to it. It
// fails closed when the context carries no tenant.
func (s *Store) Search(ctx context.Context, query string, tags []string, limit int) ([]app.MemoryEntry, error) {
	if limit <= 0 || limit > app.DefaultMemorySearchLimit {
		limit = app.DefaultMemorySearchLimit
	}
	if tags == nil {
		tags = []string{}
	}

	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	// value ILIKE '%'||$1||'%' matches everything when $1 is the empty string,
	// so an empty query lists entries. tags @> $2 requires ALL requested tags;
	// '{}' (no tags) matches every row.
	rows, err := tx.Query(ctx, `
		SELECT namespace, mem_key, value, tags, created_at, updated_at
		  FROM agent_memory
		 WHERE value ILIKE '%' || $1 || '%'
		   AND tags @> $2
		 ORDER BY updated_at DESC
		 LIMIT $3`,
		query, tags, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("memory: Search query: %w", err)
	}
	defer rows.Close()

	var out []app.MemoryEntry
	for rows.Next() {
		var entry app.MemoryEntry
		if err := rows.Scan(&entry.Namespace, &entry.Key, &entry.Value, &entry.Tags, &entry.CreatedAt, &entry.UpdatedAt); err != nil {
			return nil, fmt.Errorf("memory: Search scan: %w", err)
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory: Search rows: %w", err)
	}
	return out, nil
}

// Delete removes the entry under (namespace, key) for the context's tenant.
// Deleting an absent entry is not an error (idempotent) — RLS-invisible or
// missing rows simply affect zero rows. It fails closed when the context
// carries no tenant.
func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	if _, err := tx.Exec(ctx, `
		DELETE FROM agent_memory
		 WHERE namespace = $1 AND mem_key = $2`,
		namespace, key,
	); err != nil {
		return fmt.Errorf("memory: Delete: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("memory: Delete commit: %w", err)
	}
	return nil
}
