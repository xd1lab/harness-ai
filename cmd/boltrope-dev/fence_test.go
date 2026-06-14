// SPDX-License-Identifier: Apache-2.0

package main

// T-5 (AC-2/AC-3/AC-4/AC-5/AC-6/AC-13) — RED. The three-layer misuse fence
// required by K-1 §3 and the task brief's CRITICAL SECURITY FENCING. Tests drive a
// pure parse/guard function with an INJECTED env map (not os.Environ) and an
// injected stderr writer, so they are hermetic and never bind a real listener. They
// reference dispatch()/the runConfig type and the banner markers, which do not exist
// yet → RED.
//
// dispatch is the seam the binary's main() calls. Contract under test:
//
//	exit, cfg := dispatch(args, env, stderr)
//
//	- no subcommand / unknown subcommand → exit 2, cfg == nil, usage on stderr (AC-2)
//	- a valid `run` with no production signals and loopback binds → exit 0, cfg != nil,
//	  the loud banner written to stderr (AC-3), both resolved binds loopback (AC-4)
//	- a non-loopback bind on EITHER listener without the ack flag → non-zero, cfg == nil,
//	  reason on stderr; WITH the ack flag → exit 0, cfg != nil (AC-5)
//	- any production signal present → non-zero, cfg == nil, reason on stderr (AC-6)
//	- a re-scoped flag (--store=sqlite[:...] / --enable-local-exec) → non-zero,
//	  cfg == nil, "not available in v1 (roadmap)" reason on stderr (AC-13)

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noEnv is the empty injected environment (no production signals).
func noEnv() map[string]string { return map[string]string{} }

// --- AC-2: no / unknown subcommand → exit 2 + usage to stderr ----------------

func TestDispatch_NoSubcommand_ExitsTwoWithUsage(t *testing.T) {
	var stderr bytes.Buffer
	exit, cfg := dispatch(nil, noEnv(), &stderr) // dispatch does not exist yet (RED)
	assert.Equal(t, 2, exit, "no subcommand must exit 2")
	assert.Nil(t, cfg, "no server config must be produced")
	assert.NotEmpty(t, strings.TrimSpace(stderr.String()), "usage must be written to stderr")
}

func TestDispatch_UnknownSubcommand_ExitsTwoWithUsage(t *testing.T) {
	var stderr bytes.Buffer
	exit, cfg := dispatch([]string{"serve"}, noEnv(), &stderr) // RED
	assert.Equal(t, 2, exit, "unknown subcommand must exit 2")
	assert.Nil(t, cfg)
	assert.NotEmpty(t, strings.TrimSpace(stderr.String()))
}

// --- AC-3: loud banner on a successful run -----------------------------------

func TestDispatch_Run_WritesLoudBannerToStderr(t *testing.T) {
	var stderr bytes.Buffer
	exit, cfg := dispatch([]string{"run"}, noEnv(), &stderr) // RED
	require.Equal(t, 0, exit, "a clean run with defaults must be permitted")
	require.NotNil(t, cfg)

	banner := stderr.String()
	for _, marker := range []string{
		"NOT FOR PRODUCTION",
		"IN-MEMORY",
		"NO RLS",
		"NO mTLS",
		"NO OIDC",
		"LOOPBACK ONLY",
	} {
		assert.Containsf(t, banner, marker, "banner must contain marker %q", marker)
	}
	// The no-exec sandbox marker (exact wording is an impl detail; assert the
	// load-bearing phrase).
	assert.Truef(t,
		strings.Contains(strings.ToUpper(banner), "NO-EXEC") || strings.Contains(strings.ToUpper(banner), "NO EXEC"),
		"banner must carry the no-exec sandbox marker; got:\n%s", banner)
}

// --- AC-4: default binds are loopback on BOTH listeners -----------------------

func TestDispatch_DefaultBinds_AreLoopbackNeverWildcard(t *testing.T) {
	var stderr bytes.Buffer
	exit, cfg := dispatch([]string{"run"}, noEnv(), &stderr) // RED
	require.Equal(t, 0, exit)
	require.NotNil(t, cfg)

	for _, addr := range []string{cfg.GRPCAddr, cfg.HTTPAddr} {
		host := addr[:strings.LastIndex(addr, ":")]
		assert.Truef(t, host == "127.0.0.1" || host == "localhost",
			"default bind host must be loopback, got %q (addr %q)", host, addr)
		assert.NotEqual(t, "0.0.0.0", host, "default bind must never be the wildcard address")
	}
}

// --- AC-5: non-loopback bind fenced without the ack flag (both listeners) -----

func TestDispatch_NonLoopbackBind_FenceMatrix(t *testing.T) {
	const ack = "--i-understand-this-is-not-production"
	cases := []struct {
		name      string
		args      []string
		wantAllow bool
	}{
		{"grpc nonloopback, no ack -> refuse", []string{"run", "--grpc-addr", "0.0.0.0:8089"}, false},
		{"http nonloopback, no ack -> refuse", []string{"run", "--http-addr", "0.0.0.0:8088"}, false},
		{"grpc nonloopback, ack -> allow", []string{"run", "--grpc-addr", "0.0.0.0:8089", ack}, true},
		{"http nonloopback, ack -> allow", []string{"run", "--http-addr", "0.0.0.0:8088", ack}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stderr bytes.Buffer
			exit, cfg := dispatch(tc.args, noEnv(), &stderr) // RED
			if tc.wantAllow {
				assert.Equal(t, 0, exit, "ack flag must permit a non-loopback bind")
				assert.NotNil(t, cfg)
			} else {
				assert.NotEqual(t, 0, exit, "a non-loopback bind without the ack flag must be refused")
				assert.Nil(t, cfg, "no server config on a fenced refusal")
				assert.NotEmpty(t, strings.TrimSpace(stderr.String()), "a refusal must explain itself on stderr")
			}
		})
	}
}

// --- AC-6: production-signal refusal (each signal in isolation) ---------------

func TestDispatch_ProductionSignal_Refuses(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
	}{
		{"kubernetes", map[string]string{"KUBERNETES_SERVICE_HOST": "10.0.0.1"}},
		//nolint:gosec // G101: a deliberately fake DSN literal used only to assert the production-signal fence refuses it.
		{"postgres dsn (double underscore)", map[string]string{"BOLTROPE_POSTGRES__DSN": "postgres://u:p@db:5432/x"}},
		{"oidc issuer", map[string]string{"BOLTROPE_OIDC_ISSUER": "https://issuer.example.com"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stderr bytes.Buffer
			exit, cfg := dispatch([]string{"run"}, tc.env, &stderr) // RED
			assert.NotEqual(t, 0, exit, "a production signal must force a fail-closed refusal")
			assert.Nil(t, cfg, "no server config when a production signal is present")
			assert.NotEmpty(t, strings.TrimSpace(stderr.String()), "the refusal reason must be on stderr")
		})
	}
}

// --- AC-13: re-scoped flags rejected in v1 -----------------------------------

func TestDispatch_RescopedFlags_Rejected(t *testing.T) {
	cases := [][]string{
		{"run", "--store=sqlite"},
		{"run", "--store=sqlite:/tmp/dev.db"},
		{"run", "--enable-local-exec"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var stderr bytes.Buffer
			exit, cfg := dispatch(args, noEnv(), &stderr) // RED
			assert.NotEqual(t, 0, exit, "a re-scoped v1 flag must be rejected, not silently ignored")
			assert.Nil(t, cfg)
			assert.Truef(t,
				strings.Contains(strings.ToLower(stderr.String()), "roadmap") ||
					strings.Contains(strings.ToLower(stderr.String()), "not available"),
				"rejection must explain the flag is re-scoped to roadmap; got %q", stderr.String())
		})
	}
}
