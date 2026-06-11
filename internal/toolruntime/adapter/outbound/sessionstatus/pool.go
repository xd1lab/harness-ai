package sessionstatus

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// SimplePool opens a fresh pgx connection per [SimplePool.Acquire] and closes
// it on Release — the same no-puddle pattern as dedup.SimplePool. The reaper
// looks up statuses once per sweep (default minutes apart), so a fresh
// connection per call is deliberately cheap-enough and keeps this adapter
// free of pool dependencies; production can swap a pooled adapter behind
// [Pool].
type SimplePool struct {
	cfg *pgx.ConnConfig
}

// NewSimplePool parses dsn and returns a [SimplePool]. Returns an error only
// if the DSN is malformed; no connection is made here (the reaper's readiness
// is gated separately, and lookup failures are fail-safe).
func NewSimplePool(dsn string) (*SimplePool, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("sessionstatus: parsing DSN: %w", err)
	}
	return &SimplePool{cfg: cfg}, nil
}

// Acquire opens a fresh connection and returns it as a [PooledConn].
func (p *SimplePool) Acquire(ctx context.Context) (PooledConn, error) {
	conn, err := pgx.ConnectConfig(ctx, p.cfg)
	if err != nil {
		return nil, fmt.Errorf("sessionstatus: acquiring connection: %w", err)
	}
	return &simpleConn{conn: conn}, nil
}

// Close is a no-op: [SimplePool] holds no long-lived connections.
func (p *SimplePool) Close() {}

// simpleConn wraps a fresh pgx connection and closes it on Release.
type simpleConn struct {
	conn *pgx.Conn
}

// QueryRow runs a single-row query on the wrapped connection.
func (c *simpleConn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return c.conn.QueryRow(ctx, sql, args...)
}

// Release closes the underlying connection using a background context so a
// cancelled sweep context does not skip the teardown.
func (c *simpleConn) Release() {
	_ = c.conn.Close(context.Background())
}

// Compile-time assertions.
var (
	_ Pool       = (*SimplePool)(nil)
	_ PooledConn = (*simpleConn)(nil)
)
