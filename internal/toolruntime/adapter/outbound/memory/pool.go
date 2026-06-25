// Package memory implements the durable, tenant-scoped long-term agent memory
// store over the agent_memory PostgreSQL table (ADR-0030; migration 0008).
//
// # Design
//
// [Store] satisfies
// [github.com/xd1lab/harness-ai/internal/toolruntime/app.MemoryStore] using
// pgx/v5 against the table created by migration 0008_agent_memory. It is a
// simple key/value store keyed by (tenant_id, namespace, mem_key) with
// tag/substring retrieval — NO vector embeddings or RAG (deliberately out of
// scope; ADR-0030). It backs the memory_write/memory_read/memory_search tools
// in production; the cmd/boltrope-dev binary uses the pgx-free in-memory
// sibling package instead so it stays fenced from the Postgres driver.
//
// # Tenant isolation
//
// Every public method reads the verified tenant from the request context via
// [github.com/xd1lab/harness-ai/internal/toolruntime/infra/tenant.TenantFromContext]
// — NEVER from a method argument — and scopes the transaction with
// SELECT set_config('app.current_tenant', …, true) so RLS (FORCE ROW LEVEL
// SECURITY) applies on the non-owner application role (migration 0008 mirrors
// 0003/0007). A missing tenant fails closed before any query runs. Tenant A can
// therefore never read or modify tenant B's memory.
//
// # Pool
//
// Like the dedup and event stores, [Store] works over a consumer-defined [Pool]
// interface so this package ships [SimplePool] (a fresh pgx connection per
// acquire, no puddle dependency) and production can swap in a pooled adapter
// behind [Pool].
package memory

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Pool is the minimal connection-acquisition surface the [Store] needs. It is
// declared here (in the package that uses it) so this package is decoupled from
// any concrete pool implementation — matching the dedup and event store
// adapters (architecture §5.1).
//
// Acquire returns a borrowed [PooledConn]; callers MUST call Release when done.
// The tenant GUC is set per-transaction by the store, not by the pool.
type Pool interface {
	// Acquire borrows a connection. The caller owns it until Release.
	Acquire(ctx context.Context) (PooledConn, error)
	// Close releases pool resources.
	Close()
}

// PooledConn is a borrowed connection plus its release hook.
type PooledConn interface {
	// Begin starts a transaction on the borrowed connection.
	Begin(ctx context.Context) (pgx.Tx, error)
	// Release returns (or closes) the connection.
	Release()
}

// SimplePool opens a fresh pgx connection per [SimplePool.Acquire] and closes
// it on Release. Like the dedup/event-store SimplePool it requires no puddle
// dependency; integration tests each get their own independent connection.
type SimplePool struct {
	cfg *pgx.ConnConfig
}

// NewSimplePool parses dsn and returns a [SimplePool]. Returns an error only if
// the DSN is malformed; no connection is made here.
func NewSimplePool(dsn string) (*SimplePool, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("memory: parsing DSN: %w", err)
	}
	return &SimplePool{cfg: cfg}, nil
}

// Acquire opens a fresh connection and returns it as a [PooledConn].
func (p *SimplePool) Acquire(ctx context.Context) (PooledConn, error) {
	conn, err := pgx.ConnectConfig(ctx, p.cfg)
	if err != nil {
		return nil, fmt.Errorf("memory: acquiring connection: %w", err)
	}
	return &simpleConn{conn: conn}, nil
}

// Close is a no-op: [SimplePool] holds no long-lived connections.
func (p *SimplePool) Close() {}

// simpleConn wraps a fresh pgx connection and closes it on Release.
type simpleConn struct {
	conn *pgx.Conn
}

// Begin starts a transaction on the wrapped connection.
func (c *simpleConn) Begin(ctx context.Context) (pgx.Tx, error) {
	return c.conn.Begin(ctx)
}

// Release closes the underlying connection using a background context so a
// cancelled request context does not skip the teardown.
func (c *simpleConn) Release() {
	_ = c.conn.Close(context.Background())
}

// Compile-time assertions.
var (
	_ Pool       = (*SimplePool)(nil)
	_ PooledConn = (*simpleConn)(nil)
)
