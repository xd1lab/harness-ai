// Package dedup implements the durable at-most-once tool-execution dedup store
// over the tool_executions PostgreSQL table (ADR-0012; architecture §7.2).
//
// # Design
//
// [Store] satisfies [github.com/boltrope/boltrope/internal/toolruntime/app.DedupStore]
// using pgx/v5 against the table created by migration 0002_events_blobs_ledgers.
// The key namespace is (tenant_id, session_id, idempotency_key) — server-derived,
// never client/model-supplied (ADR-0012 §"Idempotency key scoping").
//
// # Tenant isolation
//
// Every public method reads the tenant from the context via
// [github.com/boltrope/boltrope/internal/orchestrator/infra/db.TenantFromContext]
// and scopes the transaction with SET LOCAL app.current_tenant so RLS
// (FORCE ROW LEVEL SECURITY) applies on the non-owner application role
// (ADR-0013 §"Concrete RLS"; migration 0003_rls_policies).
//
// # Pool
//
// Like the event store, [Store] works over a consumer-defined [Pool] interface
// so this package ships [SimplePool] (fresh pgx connection per acquire, no
// puddle dependency) and production can swap in a pooled adapter behind [Pool].
//
// # Result storage
//
// The [domain.Observation] result is JSON-serialized into the result_ref TEXT
// column. For large tool outputs the blob ref carried in Observation.BlobRef
// points to the actual bytes; the column stores the lightweight descriptor.
package dedup

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Pool is the minimal connection-acquisition surface the [Store] needs. It is
// declared here (in the package that uses it) so this package is decoupled from
// any concrete pool implementation — matching the pattern used by the event
// store's adapter (architecture §5.1).
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
// it on Release. Like eventstore.SimplePool it requires no puddle dependency;
// integration tests each get their own independent connection, giving genuine
// concurrency in the concurrent-Begin test.
type SimplePool struct {
	cfg *pgx.ConnConfig
}

// NewSimplePool parses dsn and returns a [SimplePool]. Returns an error only
// if the DSN is malformed; no connection is made here.
func NewSimplePool(dsn string) (*SimplePool, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("dedup: parsing DSN: %w", err)
	}
	return &SimplePool{cfg: cfg}, nil
}

// Acquire opens a fresh connection and returns it as a [PooledConn].
func (p *SimplePool) Acquire(ctx context.Context) (PooledConn, error) {
	conn, err := pgx.ConnectConfig(ctx, p.cfg)
	if err != nil {
		return nil, fmt.Errorf("dedup: acquiring connection: %w", err)
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
