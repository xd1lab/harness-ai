// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"net"
	"strings"
)

// ackFlag is the explicit acknowledgement a developer must pass to bind either
// listener to a non-loopback address. Its verbosity is intentional: a
// non-loopback dev listener exposes an unauthenticated, no-RLS, no-mTLS edge, so
// the opt-in is a deliberate, conspicuous act, never a silent default.
const ackFlag = "--i-understand-this-is-not-production"

// Default loopback bind addresses. The dev binary NEVER defaults to the wildcard
// 0.0.0.0 (K-1 fence): loopback-only is the secure default that makes the no-RLS
// in-memory edge unreachable off the developer's host.
const (
	defaultGRPCAddr = "127.0.0.1:8089"
	defaultHTTPAddr = "127.0.0.1:8088"
)

// productionSignalEnv lists the environment variables whose mere presence forces
// a fail-closed refusal: each is a strong signal the binary is running somewhere
// it must never run (a Kubernetes/orchestrator pod, or against a real Postgres /
// OIDC issuer). This is the K-1 production-signal fence: dev mode bypasses RLS,
// mTLS, and OIDC, so it must be impossible to mistake for a production deployment.
var productionSignalEnv = []string{
	"KUBERNETES_SERVICE_HOST", // injected into every pod by the kubelet
	"BOLTROPE_POSTGRES__DSN",  // a real event-store DSN (RLS multi-tenant store)
	"BOLTROPE_OIDC_ISSUER",    // a real OIDC issuer (production edge auth)
}

// runConfig is the resolved, validated configuration for one `boltrope-dev run`.
// It is produced by dispatch ONLY when every fence has passed; a nil runConfig
// from dispatch means the binary must NOT start.
type runConfig struct {
	// GRPCAddr is the resolved gRPC listen address (loopback by default).
	GRPCAddr string
	// HTTPAddr is the resolved REST/SSE listen address (loopback by default).
	HTTPAddr string
}

// dispatch is the pure parse/guard seam the binary's main() calls. It returns the
// process exit code and, on success only, the resolved [runConfig]. It is
// hermetic: env is the INJECTED environment (not os.Environ) and stderr the
// INJECTED writer, so the three-layer misuse fence is fully unit-testable without
// binding a real listener.
//
// Layering (each must pass, in order):
//
//  1. Subcommand gate — the only v1 subcommand is `run`; a bare or unknown
//     invocation prints usage and exits 2 (non-default; can't start by accident).
//  2. Flag parse + re-scoped-flag rejection — --store=sqlite[:...] and
//     --enable-local-exec are re-scoped to roadmap and rejected, never ignored.
//  3. Production-signal fence — any productionSignalEnv present => fail-closed.
//  4. Loopback fence — a non-loopback bind on EITHER listener requires the ack
//     flag; otherwise refuse.
//
// On success it writes the loud multi-line banner to stderr and returns the
// resolved config with exit 0.
func dispatch(args []string, env map[string]string, stderr io.Writer) (int, *runConfig) {
	if len(args) == 0 {
		usage(stderr)
		return 2, nil
	}
	sub, rest := args[0], args[1:]
	if sub != "run" {
		_, _ = fmt.Fprintf(stderr, "boltrope-dev: unknown subcommand %q\n\n", sub)
		usage(stderr)
		return 2, nil
	}

	cfg, err := parseRunFlags(rest)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "boltrope-dev: %v\n", err)
		return 2, nil
	}

	// Layer 3: production-signal fence (fail-closed).
	if signal, ok := detectProductionSignal(env); ok {
		_, _ = fmt.Fprintf(stderr,
			"boltrope-dev: refusing to start: production signal %q present.\n"+
				"  dev mode has NO RLS, NO mTLS, NO OIDC and is loopback-only; it must never run in production.\n",
			signal)
		return 1, nil
	}

	// Layer 4: loopback fence (non-loopback bind requires the explicit ack).
	if !cfg.ack {
		for _, b := range []struct{ name, addr string }{
			{"--grpc-addr", cfg.grpcAddr},
			{"--http-addr", cfg.httpAddr},
		} {
			if !isLoopbackAddr(b.addr) {
				_, _ = fmt.Fprintf(stderr,
					"boltrope-dev: refusing to bind %s to non-loopback address %q without %s.\n"+
						"  dev mode exposes an unauthenticated, no-RLS edge; pass %s to accept the risk.\n",
					b.name, b.addr, ackFlag, ackFlag)
				return 1, nil
			}
		}
	}

	writeBanner(stderr, cfg)
	return 0, &runConfig{GRPCAddr: cfg.grpcAddr, HTTPAddr: cfg.httpAddr}
}

// parsedRunFlags holds the raw, validated flag values for `run`.
type parsedRunFlags struct {
	grpcAddr string
	httpAddr string
	ack      bool
}

// parseRunFlags parses the `run` subcommand flags by hand (the flag package's
// default error behavior calls os.Exit, which would defeat the hermetic
// dispatch contract). It rejects the re-scoped v1 flags with a "roadmap" reason.
func parseRunFlags(args []string) (parsedRunFlags, error) {
	cfg := parsedRunFlags{grpcAddr: defaultGRPCAddr, httpAddr: defaultHTTPAddr}

	for i := 0; i < len(args); i++ {
		arg := args[i]
		name, inlineVal, hasInline := splitFlag(arg)

		// value reads the inline (--flag=v) or next-token (--flag v) value.
		value := func() (string, error) {
			if hasInline {
				return inlineVal, nil
			}
			if i+1 >= len(args) {
				return "", fmt.Errorf("flag %s requires a value", name)
			}
			i++
			return args[i], nil
		}

		switch name {
		case "--grpc-addr":
			v, err := value()
			if err != nil {
				return parsedRunFlags{}, err
			}
			cfg.grpcAddr = v
		case "--http-addr":
			v, err := value()
			if err != nil {
				return parsedRunFlags{}, err
			}
			cfg.httpAddr = v
		case ackFlag:
			cfg.ack = true

		// --- re-scoped to roadmap (rejected, never silently ignored; AC-13) ---
		case "--store":
			// Both --store=sqlite and --store sqlite are rejected: SQLite/file
			// persistence is re-scoped to roadmap (K-2). In-memory is the only v1
			// store; there is no flag to select it.
			return parsedRunFlags{}, fmt.Errorf(
				"--store is not available in v1 (re-scoped to roadmap): dev mode is in-memory only")
		case "--enable-local-exec":
			// A real shell-capable local sandbox is roadmap and requires its own
			// ADR-0014-grade adversarial tests + a second opt-in (K-2).
			return parsedRunFlags{}, fmt.Errorf(
				"--enable-local-exec is not available in v1 (re-scoped to roadmap): dev sandbox is no-exec")

		default:
			return parsedRunFlags{}, fmt.Errorf("unknown flag %q", arg)
		}
	}
	return cfg, nil
}

// splitFlag splits "--flag=value" into ("--flag", "value", true); a bare "--flag"
// returns ("--flag", "", false). It special-cases --store= so the roadmap
// rejection fires on the flag NAME regardless of any inline value.
func splitFlag(arg string) (name, value string, hasInline bool) {
	if eq := strings.IndexByte(arg, '='); eq >= 0 {
		return arg[:eq], arg[eq+1:], true
	}
	return arg, "", false
}

// detectProductionSignal reports the first production-signal env var present in
// env (non-empty value), or ok=false when none is set.
func detectProductionSignal(env map[string]string) (string, bool) {
	for _, key := range productionSignalEnv {
		if strings.TrimSpace(env[key]) != "" {
			return key, true
		}
	}
	return "", false
}

// isLoopbackAddr reports whether addr's host is a loopback address (127.0.0.0/8,
// ::1, or the literal "localhost"). An unparseable or wildcard (0.0.0.0/::) host
// is NOT loopback, so it is fenced behind the ack flag.
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// No port (or malformed): treat the whole string as the host.
		host = addr
	}
	host = strings.TrimSpace(host)
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}

// usage prints the dev binary's usage. A bare or unknown invocation prints this
// and exits non-zero, so dev mode is never the default behavior.
func usage(w io.Writer) {
	_, _ = fmt.Fprint(w, `boltrope-dev — single-process, loopback-only LOCAL DEV mode for Boltrope.

Usage:
  boltrope-dev run [flags]

The only subcommand is `+"`run`"+`. There is no default action.

Flags:
  --grpc-addr host:port   gRPC listen address (default `+defaultGRPCAddr+`, loopback only)
  --http-addr host:port   REST/SSE listen address (default `+defaultHTTPAddr+`, loopback only)
  `+ackFlag+`
                          acknowledge and permit a NON-loopback bind (off by default)

NOT FOR PRODUCTION: in-memory store, NO RLS, NO mTLS, NO OIDC, loopback only,
no-exec sandbox. See ADR-0024.
`)
}

// writeBanner writes the loud, multi-line, NOT-FOR-PRODUCTION startup banner to
// stderr. The markers (NOT FOR PRODUCTION / IN-MEMORY / NO RLS / NO mTLS / NO
// OIDC / LOOPBACK ONLY / NO-EXEC) are load-bearing: they are the operator-facing
// proof of exactly which production safeguards this mode bypasses (K-1 §3).
func writeBanner(w io.Writer, cfg parsedRunFlags) {
	const bar = "============================================================================"
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n", bar)
	b.WriteString("  boltrope-dev — LOCAL DEV MODE — *** NOT FOR PRODUCTION ***\n")
	fmt.Fprintf(&b, "%s\n", bar)
	b.WriteString("  Event store : IN-MEMORY (non-persistent; lost on exit)\n")
	b.WriteString("  Security    : NO RLS  |  NO mTLS  |  NO OIDC  (synthetic single-tenant principal)\n")
	b.WriteString("  Network     : LOOPBACK ONLY\n")
	b.WriteString("  Sandbox     : NO-EXEC (model-generated shell is refused, never run on your host)\n")
	fmt.Fprintf(&b, "  gRPC        : %s\n", cfg.grpcAddr)
	fmt.Fprintf(&b, "  REST/SSE    : %s\n", cfg.httpAddr)
	if cfg.ack {
		fmt.Fprintf(&b, "  WARNING     : a NON-loopback bind was explicitly acknowledged (%s)\n", ackFlag)
	}
	fmt.Fprintf(&b, "%s\n", bar)
	_, _ = io.WriteString(w, b.String())
}
