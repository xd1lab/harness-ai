package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/platform/config"
	"github.com/boltrope/boltrope/internal/toolruntime/app"
)

func baseConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		Server:       config.ServerConfig{GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"},
		Postgres:     config.PostgresConfig{DSN: "postgres://u@localhost:5432/db", Version: 14},
		OTLP:         config.OTLPConfig{Endpoint: ""},
		ModelGateway: config.ModelGatewayConfig{Endpoint: "localhost:9001"},
		Blob:         config.BlobConfig{Dir: t.TempDir()},
		LogLevel:     "info",
		DevInsecure:  true,
	}
}

// TestBuildRegistry_RegistersNativeTools asserts the wiring registers the full
// native tool set (read/write/edit/bash/glob/grep/webfetch/websearch) so
// ListTools returns them with valid specs (FR-TOOL-02, FR-EXT-01 AC-3).
func TestBuildRegistry_RegistersNativeTools(t *testing.T) {
	reg, err := buildRegistry(toolSettings{})
	require.NoError(t, err)
	specs, err := reg.List(context.Background())
	require.NoError(t, err)

	names := map[string]bool{}
	for _, s := range specs {
		names[s.Name] = true
		assert.NotEmpty(t, s.Description, "tool %q must have a description", s.Name)
		assert.NotEmpty(t, s.JSONSchema, "tool %q must have a schema", s.Name)
	}
	for _, want := range []string{"read", "write", "edit", "bash", "glob", "grep", "webfetch", "websearch"} {
		assert.True(t, names[want], "native tool %q must be registered", want)
	}
}

// TestBuildExecuteService_Constructs asserts the ExecuteTool use-case constructs
// from the wired collaborators (registry, runtime, egress, dedup, blobs) without
// error — the full tool-runtime dependency graph (T-TR-07).
func TestBuildExecuteService_Constructs(t *testing.T) {
	cfg := baseConfig(t)
	svc, reg, closers, err := buildToolRuntime(cfg, toolSettings{})
	require.NoError(t, err)
	require.NotNil(t, svc)
	require.NotNil(t, reg)
	// Closers (the dedup pool) must be returned for shutdown.
	assert.NotEmpty(t, closers)
	for _, c := range closers {
		_ = c() // closing a never-connected SimplePool is a no-op and must not panic.
	}
}

// TestRun_ConstructsAndShutsDown is the daemon smoke: Run wires the whole
// tool-runtime, serves over mTLS, and returns cleanly on context cancel — proving
// the dependency graph constructs without panic given a minimal config. Docker is
// NOT required: the container runtime probes lazily on first Create.
func TestRun_ConstructsAndShutsDown(t *testing.T) {
	t.Setenv("BOLTROPE_DEV_INSECURE", "1")
	cfg := baseConfig(t)

	ctx, cancel := context.WithCancel(context.Background())
	var buf strings.Builder
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, &buf) }()

	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// TestRun_ListenFailure asserts a bad listen address surfaces an error.
func TestRun_ListenFailure(t *testing.T) {
	t.Setenv("BOLTROPE_DEV_INSECURE", "1")
	cfg := baseConfig(t)
	cfg.Server.GRPCAddr = "256.256.256.256:1"
	err := Run(context.Background(), cfg, io.Discard)
	require.Error(t, err)
}

// ---- egress allowlist wiring -------------------------------------------

// TestParseAllowlist covers the BOLTROPE_TOOLRT_EGRESS_ALLOWLIST parser: a
// comma-separated list with surrounding whitespace and empty fields trimmed,
// and an unset/blank value yielding the deny-all empty allowlist.
func TestParseAllowlist(t *testing.T) {
	cases := map[string]struct {
		in   string
		want []string
	}{
		"empty":          {in: "", want: nil},
		"whitespace":     {in: "   ", want: nil},
		"single":         {in: "api.example.com", want: []string{"api.example.com"}},
		"multi":          {in: "a.example.com,b.example.com", want: []string{"a.example.com", "b.example.com"}},
		"trims spaces":   {in: " a.example.com , b.example.com ", want: []string{"a.example.com", "b.example.com"}},
		"drops empties":  {in: "a.example.com,,, ,b.example.com", want: []string{"a.example.com", "b.example.com"}},
		"keeps wildcard": {in: "*.example.com", want: []string{"*.example.com"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, parseAllowlist(tc.in))
		})
	}
}

// TestLoadToolSettings_EgressAllowlist asserts loadToolSettings reads the
// allowlist env var into the parsed slice operators configure egress with.
func TestLoadToolSettings_EgressAllowlist(t *testing.T) {
	t.Setenv("BOLTROPE_TOOLRT_EGRESS_ALLOWLIST", "api.example.com, *.internal.example.com")
	ts := loadToolSettings()
	assert.Equal(t, []string{"api.example.com", "*.internal.example.com"}, ts.EgressAllowlist)
}

// TestBuildEgress_DenyAllByDefault asserts that with no configured allowlist the
// broker denies every host for the default session (deny-by-default), proving an
// unconfigured deployment has no egress policy installed that would permit a host.
func TestBuildEgress_DenyAllByDefault(t *testing.T) {
	broker, policy := buildEgress(toolSettings{})
	require.NotNil(t, broker)
	assert.Equal(t, defaultSessionID, policy.SessionID)
	assert.Empty(t, policy.AllowedHosts)

	allowed, err := broker.Allow(context.Background(), defaultSessionID, "api.example.com")
	require.NoError(t, err)
	assert.False(t, allowed, "unconfigured broker must deny all egress")
}

// TestBuildEgress_InstallsConfiguredAllowlist asserts that a configured allowlist
// is installed on the broker via SetPolicy in the wiring (not just tests), so an
// operator CAN permit egress to the listed hosts while everything else stays denied.
func TestBuildEgress_InstallsConfiguredAllowlist(t *testing.T) {
	ts := toolSettings{EgressAllowlist: []string{"api.example.com", "*.internal.example.com"}}
	broker, policy := buildEgress(ts)
	require.NotNil(t, broker)
	assert.Equal(t, defaultSessionID, policy.SessionID)
	assert.Equal(t, ts.EgressAllowlist, policy.AllowedHosts)

	ctx := context.Background()
	allowed, err := broker.Allow(ctx, defaultSessionID, "api.example.com")
	require.NoError(t, err)
	assert.True(t, allowed, "configured exact host must be allowed")

	allowed, err = broker.Allow(ctx, defaultSessionID, "svc.internal.example.com")
	require.NoError(t, err)
	assert.True(t, allowed, "configured wildcard host must be allowed")

	allowed, err = broker.Allow(ctx, defaultSessionID, "attacker.tld")
	require.NoError(t, err)
	assert.False(t, allowed, "host outside the allowlist must stay denied")
}

// TestBuildEgress_PolicyMatchesWorkspaceBinding asserts the egress policy returned
// by buildEgress (and thus bound to the routing workspace) carries the same
// allowlist installed on the broker — the workspace's NetworkPolicy and the broker
// agree on the configured hosts for the session.
func TestBuildEgress_PolicyMatchesWorkspaceBinding(t *testing.T) {
	ts := toolSettings{EgressAllowlist: []string{"api.example.com"}}
	_, policy := buildEgress(ts)

	ws := newRoutingWorkspace(nil, defaultSessionID, policy)
	got, err := ws.NetworkPolicy(context.Background())
	require.NoError(t, err)
	assert.Equal(t, app.EgressPolicy{SessionID: defaultSessionID, AllowedHosts: []string{"api.example.com"}}, got)
}
