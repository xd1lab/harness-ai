package eventstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgxConn is the minimal surface the AfterConnect hook receives. It matches
// the pgxpool.Conn method set that callers of [WithAfterConnect] typically need
// (e.g. to run SET LOCAL at connection establishment). Using an interface here
// keeps the hook's signature free of the concrete pgxpool.Conn import in
// callers that only need to pass the hook along without calling pool methods.
type pgxConn interface {
	Ping(context.Context) error
}

// PgxOption is a functional option that mutates a [pgxpool.Config] before the
// pool is opened. Options are applied in argument order by [NewPgxPool].
type PgxOption func(*pgxpool.Config)

// WithMaxConns sets the maximum pool size. It maps directly to
// [github.com/jackc/pgx/v5/pgxpool.Config.MaxConns].
func WithMaxConns(n int32) PgxOption {
	return func(cfg *pgxpool.Config) { cfg.MaxConns = n }
}

// WithMinConns sets the minimum pool size. It maps directly to
// [github.com/jackc/pgx/v5/pgxpool.Config.MinConns].
func WithMinConns(n int32) PgxOption {
	return func(cfg *pgxpool.Config) { cfg.MinConns = n }
}

// WithMaxConnLifetime sets the maximum lifetime of a pooled connection. It
// maps to [github.com/jackc/pgx/v5/pgxpool.Config.MaxConnLifetime].
func WithMaxConnLifetime(d time.Duration) PgxOption {
	return func(cfg *pgxpool.Config) { cfg.MaxConnLifetime = d }
}

// WithAfterConnect registers a hook that is called once per newly established
// pooled connection, before the connection is handed to the application. The
// primary purpose is wiring the RLS GUC at connection-establishment time for
// [T-ORCH-04]: in production the orchestrator's infra/db layer installs a hook
// here that runs SET LOCAL app.current_tenant, scoping every borrowed
// connection to the verified tenant before the store uses it.
//
// The hook receives a [pgxConn] (a small interface over the underlying
// *pgx.Conn / *pgxpool.Conn) so callers can send startup commands without
// importing pgxpool directly.
//
// NOTE: SET LOCAL is transaction-scoped, not connection-scoped, so the hook
// here is a convenient extension point for any PER-CONNECT initialization (e.g.
// prepared statements, session-level GUCs). The per-transaction tenant scoping
// is still done inside [Store.beginTenantTx] via setLocalTenant as before;
// this hook covers connection-level work.
func WithAfterConnect(fn func(context.Context, pgxConn) error) PgxOption {
	return func(cfg *pgxpool.Config) {
		cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			return fn(ctx, conn)
		}
	}
}

// PgxPool is a [Pool] backed by a [github.com/jackc/pgx/v5/pgxpool.Pool].
// It is the production pool implementation: connection reuse, idle-connection
// management, health checks, and the AfterConnect hook for per-connection
// initialization (e.g. RLS setup at T-ORCH-04). [SimplePool] remains the
// test-only pool (one connection per acquire, no puddle dependency) so the
// unit and standard integration tests continue to run without a pooled driver.
//
// PgxPool satisfies the [Pool] interface defined in pool.go; the Store accepts
// it via that interface with no change.
type PgxPool struct {
	inner *pgxpool.Pool
}

// NewPgxPool parses dsn, applies opts (in order), and opens a
// [github.com/jackc/pgx/v5/pgxpool.Pool]. It returns an error if the DSN is
// malformed (ParseConfig) or if the pool cannot be opened (NewWithConfig, which
// dials at least one connection when MinConns > 0 or on first acquire).
//
// The returned [*PgxPool] is safe for concurrent use. The caller owns its
// lifecycle and must call [PgxPool.Close] when done.
func NewPgxPool(ctx context.Context, dsn string, opts ...PgxOption) (*PgxPool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("eventstore: parsing pgxpool DSN: %w", err)
	}
	for _, opt := range opts {
		opt(cfg)
	}
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("eventstore: opening pgxpool: %w", err)
	}
	return &PgxPool{inner: p}, nil
}

// Acquire borrows a connection from the underlying pgxpool. The returned
// [PooledConn] wraps a [github.com/jackc/pgx/v5/pgxpool.Conn]; Release returns
// it to the pool.
func (p *PgxPool) Acquire(ctx context.Context) (PooledConn, error) {
	if p.inner == nil {
		return nil, fmt.Errorf("eventstore: PgxPool not initialized (nil inner pool)")
	}
	conn, err := p.inner.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("eventstore: pgxpool acquire: %w", err)
	}
	return &pgxPoolConn{conn: conn}, nil
}

// Close closes all idle connections and waits for active connections to be
// released before returning. It delegates to the underlying pool's Close. It
// is safe to call on a zero-value [PgxPool] (nil inner).
func (p *PgxPool) Close() {
	if p.inner != nil {
		p.inner.Close()
	}
}

// pgxPoolConn is a [PooledConn] wrapping a borrowed [pgxpool.Conn]. Begin
// starts a transaction; Release returns the connection to the pool.
//
// It does NOT implement [listenConn]: pgxpool connections are returned to the
// pool on Release, so a connection-level LISTEN listener would be shared by
// all borrowers. Subscribe degrades to pure ticker-based polling when the
// borrowed connection does not implement listenConn (see subscribe.go). A
// future enhancement could use a dedicated long-lived connection for LISTEN
// alongside the pool.
type pgxPoolConn struct {
	conn *pgxpool.Conn
}

// Begin starts a transaction on the borrowed connection.
func (c *pgxPoolConn) Begin(ctx context.Context) (pgx.Tx, error) {
	return c.conn.Begin(ctx)
}

// Release returns the connection to the pool.
func (c *pgxPoolConn) Release() {
	c.conn.Release()
}

// Compile-time assertions.
var (
	_ Pool       = (*PgxPool)(nil)
	_ PooledConn = (*pgxPoolConn)(nil)
)
