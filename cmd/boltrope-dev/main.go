// SPDX-License-Identifier: Apache-2.0

// Command boltrope-dev is the single-process, loopback-only LOCAL DEV mode for
// Boltrope (feature K; ADR-0024). It runs the SAME agent loop the production
// orchestrator runs, in ONE binary — an in-memory event store, a no-exec
// in-process tool sandbox, the keyless stub model, and the dev-insecure auth path
// (a synthetic single-tenant principal in place of OIDC/RLS) — so a developer
// reaches first success at near pip-install speed.
//
// CRITICAL: this mode bypasses RLS, mTLS, and OIDC. It is fenced so it cannot be
// mistaken for / used as a production deployment: it is a SEPARATE binary
// (production images never package it), it requires the explicit `run`
// subcommand, it prints a loud NOT-FOR-PRODUCTION banner, it refuses to start on
// production signals, and it binds loopback-only unless explicitly acknowledged.
// All fencing lives in (and is tested through) dispatch in fence.go.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	os.Exit(run(os.Args[1:], envMap(os.Environ()), os.Stderr))
}

// run is main's testable core: it dispatches (parse + the three-layer misuse
// fence), and on success starts the server and blocks until an interrupt signal,
// then shuts down. It returns the process exit code.
func run(args []string, env map[string]string, stderr *os.File) int {
	exit, cfg := dispatch(args, env, stderr)
	if cfg == nil {
		return exit
	}

	srv, err := newServer(serveOpts{GRPCAddr: cfg.GRPCAddr, HTTPAddr: cfg.HTTPAddr})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "boltrope-dev: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stderr, "boltrope-dev: serving gRPC on %s, REST/SSE on %s (Ctrl-C to stop)\n",
		srv.GRPCAddr, srv.HTTPAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_, _ = fmt.Fprintf(stderr, "boltrope-dev: shutdown: %v\n", err)
		return 1
	}
	return 0
}

// shutdownTimeout bounds the graceful shutdown of both listeners.
const shutdownTimeout = 5 * time.Second

// envMap converts a "KEY=VALUE" environment slice (os.Environ) into the injected
// map dispatch consumes, so the fence is driven from the real process env in
// production but a hermetic map in tests.
func envMap(environ []string) map[string]string {
	out := make(map[string]string, len(environ))
	for _, kv := range environ {
		for i := 0; i < len(kv); i++ {
			if kv[i] == '=' {
				out[kv[:i]] = kv[i+1:]
				break
			}
		}
	}
	return out
}
