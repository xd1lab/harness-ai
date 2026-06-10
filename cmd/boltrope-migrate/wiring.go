// Command boltrope-migrate applies the embedded event-store migrations to the
// configured PostgreSQL DSN and exits — 0 on success, non-zero on failure
// (NFR-OPS-01, DOD-12; architecture §10.2). It is the migration step the
// docker-compose ordering gate runs after Postgres is healthy and before any
// service starts, so a service never accepts traffic against an unmigrated
// schema.
//
// It is intentionally tiny: all the work — connecting, the PostgreSQL >= 13
// version gate, the forward-only guard, and applying the embedded Up migrations
// — lives in the already-tested
// [github.com/boltrope/boltrope/internal/orchestrator/infra/db] library half.
// This binary only loads config (for the DSN) and maps the library's error to a
// process exit code.
package main

import (
	"context"
	"fmt"
	"io"

	"github.com/boltrope/boltrope/internal/orchestrator/infra/db"
	"github.com/boltrope/boltrope/internal/platform/config"
)

// parseConfig loads the shared service [config.Config] from flags + env. The
// migrate command only consumes postgres.dsn, but it reuses the common loader so
// precedence and validation are identical across binaries (NFR-OPS-04); the
// other required fields are validated too, which is harmless because compose
// supplies them via the same env the services use. args/environ are injected so
// the loader is unit-testable without touching the process state.
func parseConfig(args, environ []string) (*config.Config, error) {
	return config.Load(config.Options{Args: args, Environ: environ})
}

// run loads config and applies the migrations, returning a process exit code: 0
// on success (including the idempotent "already at latest" case), non-zero on a
// config error or a migration failure. It writes the failure reason to stderr.
// It is the testable core of [main]; ctx carries cancellation/deadline.
func run(ctx context.Context, args, environ []string, _, stderr io.Writer) int {
	cfg, err := parseConfig(args, environ)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err.Error())
		return 1
	}

	if err := db.Migrate(ctx, cfg.Postgres.DSN); err != nil {
		_, _ = fmt.Fprintf(stderr, "boltrope-migrate: %v\n", err)
		return 1
	}
	return 0
}
