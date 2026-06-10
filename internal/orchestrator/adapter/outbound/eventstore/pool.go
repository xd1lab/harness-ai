package eventstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Pool is the minimal connection-acquisition surface the [Store] needs. It is a
// consumer-defined interface (declared here, in the package that USES it) so the
// store is decoupled from any concrete pool: production wiring can supply a
// *github.com/jackc/pgx/v5/pgxpool.Pool adapter, while this package ships
// [SimplePool] (a fresh-connection-per-acquire pgx-core pool) so the store and
// its integration tests run without the puddle pool dependency. Swapping in a
// real pool later is a non-breaking wiring change (architecture §5.1: the
// EventLogPort adapter implementation may change without touching the port).
//
// Acquire returns a borrowed [PooledConn]; the caller MUST call its Release when
// done so the connection is returned/closed. The tenant GUC (SET LOCAL
// app.current_tenant) is set per-transaction by the store, not by the pool, so a
// borrowed connection is tenant-neutral until a transaction scopes it.
type Pool interface {
	// Acquire borrows a connection from the pool. The returned [PooledConn] is
	// owned by the caller until Release is called.
	Acquire(ctx context.Context) (PooledConn, error)
	// Close releases all pool resources.
	Close()
}

// PooledConn is a borrowed connection plus its release hook. Begin starts a
// transaction on the underlying connection; Release returns the connection to
// the pool (or closes it, for [SimplePool]). It mirrors the subset of a pooled
// connection the store uses.
type PooledConn interface {
	// Begin starts a transaction on the borrowed connection.
	Begin(ctx context.Context) (pgx.Tx, error)
	// Release returns the connection to the pool. It is safe to call once; the
	// store calls it exactly once per Acquire via defer.
	Release()
}

// SimplePool is a [Pool] backed by pgx core that opens a FRESH connection per
// [SimplePool.Acquire] and closes it on Release. It exists because the pooled
// driver (github.com/jackc/pgx/v5/pgxpool) depends on puddle, which is not in
// this module's dependency set; a fresh-conn-per-acquire pool gives the store
// real, independent connections — exactly what the concurrent-append tests need
// (each goroutine gets its own session, so optimistic conflicts are genuine) —
// without that dependency.
//
// It is safe for concurrent use: each Acquire is an independent Connect. It
// deliberately does not cap or reuse connections (that is a real pool's job);
// for the migration runner and the test/dev event store this is sufficient, and
// production swaps in a pooled adapter behind [Pool].
type SimplePool struct {
	cfg *pgx.ConnConfig
}

// NewSimplePool parses dsn and returns a [SimplePool]. The DSN names the
// non-owner application role RLS binds (ADR-0011 §6.7); credentials are the
// caller's responsibility. It returns an error only if the DSN is malformed (no
// connection is made here — connections are opened lazily per Acquire).
func NewSimplePool(dsn string) (*SimplePool, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("eventstore: parsing DSN: %w", err)
	}
	return &SimplePool{cfg: cfg}, nil
}

// Acquire opens a fresh connection and returns it as a [PooledConn] whose
// Release closes it.
func (p *SimplePool) Acquire(ctx context.Context) (PooledConn, error) {
	conn, err := pgx.ConnectConfig(ctx, p.cfg)
	if err != nil {
		return nil, fmt.Errorf("eventstore: acquiring connection: %w", err)
	}
	return &simpleConn{conn: conn}, nil
}

// Close is a no-op for [SimplePool]: it holds no long-lived connections (each is
// opened and closed per Acquire/Release).
func (p *SimplePool) Close() {}

// simpleConn is a [PooledConn] wrapping a freshly-opened pgx connection that is
// closed on Release.
type simpleConn struct {
	conn *pgx.Conn
}

// Begin starts a transaction on the wrapped connection.
func (c *simpleConn) Begin(ctx context.Context) (pgx.Tx, error) {
	return c.conn.Begin(ctx)
}

// Release closes the underlying connection. The close uses a background context
// so a cancelled request context still tears the connection down.
func (c *simpleConn) Release() {
	_ = c.conn.Close(context.Background())
}

// Compile-time assertions.
var (
	_ Pool       = (*SimplePool)(nil)
	_ PooledConn = (*simpleConn)(nil)
)
