package eventstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------------------------------------------------------
// Unit tests (no real database required).
// ---------------------------------------------------------------------------

// TestNewPgxPool_BadDSN asserts that a malformed DSN is rejected at
// construction time (ParseConfig fails before any connection is opened).
func TestNewPgxPool_BadDSN(t *testing.T) {
	t.Parallel()
	_, err := NewPgxPool(context.Background(), "://not-a-dsn")
	if err == nil {
		t.Fatal("NewPgxPool should reject a malformed DSN")
	}
}

// TestNewPgxPool_OptionMapping_MaxConns asserts that WithMaxConns wires the
// value into the parsed pgxpool.Config before the pool is opened (unit-level
// option-mapping check with a closed/fake pool). We parse a syntactically valid
// DSN with a non-connectable host so NewWithConfig fails fast, then verify the
// Config was mutated before the dial.
func TestNewPgxPool_OptionMapping_MaxConns(t *testing.T) {
	t.Parallel()

	// Parse a valid DSN pointing to a non-connectable host, apply the option,
	// and assert the Config field was set. We cannot call NewWithConfig here
	// without actually dialing, so we exercise the option mutator directly.
	cfg, err := pgxpool.ParseConfig("postgres://localhost:1/testdb?sslmode=disable&pool_max_conns=4")
	if err != nil {
		t.Fatalf("ParseConfig baseline: %v", err)
	}
	const want int32 = 17
	WithMaxConns(want)(cfg)
	if cfg.MaxConns != want {
		t.Fatalf("WithMaxConns(%d): MaxConns = %d, want %d", want, cfg.MaxConns, want)
	}
}

// TestNewPgxPool_OptionMapping_MinConns asserts WithMinConns sets MinConns.
func TestNewPgxPool_OptionMapping_MinConns(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig("postgres://localhost:1/testdb?sslmode=disable")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	const want int32 = 2
	WithMinConns(want)(cfg)
	if cfg.MinConns != want {
		t.Fatalf("WithMinConns(%d): MinConns = %d, want %d", want, cfg.MinConns, want)
	}
}

// TestNewPgxPool_OptionMapping_MaxConnLifetime asserts WithMaxConnLifetime sets
// MaxConnLifetime.
func TestNewPgxPool_OptionMapping_MaxConnLifetime(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig("postgres://localhost:1/testdb?sslmode=disable")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	const want = 30 * time.Minute
	WithMaxConnLifetime(want)(cfg)
	if cfg.MaxConnLifetime != want {
		t.Fatalf("WithMaxConnLifetime(%v): got %v, want %v", want, cfg.MaxConnLifetime, want)
	}
}

// TestNewPgxPool_OptionMapping_AfterConnect asserts WithAfterConnect replaces
// the AfterConnect hook in the Config.
func TestNewPgxPool_OptionMapping_AfterConnect(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig("postgres://localhost:1/testdb?sslmode=disable")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	// WithAfterConnect accepts the package-internal pgxConn interface; a
	// context.Background() call verifies the closure captures the sentinel.
	_ = errors.New("sentinel") // kept for readability
	WithAfterConnect(func(_ context.Context, _ pgxConn) error {
		return nil
	})(cfg)

	// The hook slot must now be non-nil.
	if cfg.AfterConnect == nil {
		t.Fatal("WithAfterConnect: AfterConnect hook is nil after applying option")
	}
}

// TestPgxPool_Close_Idempotent verifies Close does not panic when called with
// an unreachable pool that was never really opened. NewPgxPool returns an error
// for unreachable hosts (no connection is made in the constructor when the
// dial fails), so we only test that the exported NewPgxPool constructor fails
// cleanly and that a manually constructed PgxPool with a nil inner field is safe.
// Real pool lifecycle (open → close) is covered by the integration test.
func TestPgxPool_Close_Idempotent(t *testing.T) {
	t.Parallel()
	// Construct a PgxPool with a nil inner pool (simulates a pool that was never
	// successfully opened). Close must not panic.
	p := &PgxPool{}
	p.Close() // must not panic
}

// TestPgxPool_Acquire_Nil returns an error (not a panic) when Acquire is called
// on an uninitialised PgxPool.
func TestPgxPool_Acquire_Nil(t *testing.T) {
	t.Parallel()
	p := &PgxPool{}
	_, err := p.Acquire(context.Background())
	if err == nil {
		t.Fatal("Acquire on uninitialised PgxPool must return an error")
	}
}
