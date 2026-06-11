package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/platform/config"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app/truntimetest"
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
	svc, reg, rt, closers, err := buildToolRuntime(cfg, toolSettings{})
	require.NoError(t, err)
	require.NotNil(t, svc)
	require.NotNil(t, reg)
	require.NotNil(t, rt, "the container runtime must be returned so Run can start the §10.6 reaper")
	// Closers (the dedup pool + the runtime) must be returned for shutdown.
	assert.NotEmpty(t, closers)
	for _, c := range closers {
		// Closing a never-connected SimplePool / a runtime with no live sandboxes
		// is a no-op and must not panic.
		_ = c()
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
// broker denies every host for EVERY session (deny-by-default), proving an
// unconfigured deployment has no egress policy that would permit a host.
func TestBuildEgress_DenyAllByDefault(t *testing.T) {
	broker := buildEgress(toolSettings{})
	require.NotNil(t, broker)

	for _, sid := range []string{"sess-a", "sess-b"} {
		allowed, err := broker.Allow(context.Background(), sid, "api.example.com")
		require.NoError(t, err)
		assert.False(t, allowed, "unconfigured broker must deny all egress for session %q", sid)
	}
}

// TestBuildEgress_ConfiguredAllowlistGovernsEverySession asserts that the
// operator-configured allowlist is the broker's default policy for ANY session —
// sessions arrive implicitly with each ExecuteTool call, so the wiring cannot
// pre-install per-session policies at startup. Hosts outside it stay denied.
func TestBuildEgress_ConfiguredAllowlistGovernsEverySession(t *testing.T) {
	ts := toolSettings{EgressAllowlist: []string{"api.example.com", "*.internal.example.com"}}
	broker := buildEgress(ts)
	require.NotNil(t, broker)

	ctx := context.Background()
	for _, sid := range []string{"sess-a", "sess-b"} {
		allowed, err := broker.Allow(ctx, sid, "api.example.com")
		require.NoError(t, err)
		assert.True(t, allowed, "configured exact host must be allowed for session %q", sid)

		allowed, err = broker.Allow(ctx, sid, "svc.internal.example.com")
		require.NoError(t, err)
		assert.True(t, allowed, "configured wildcard host must be allowed for session %q", sid)

		allowed, err = broker.Allow(ctx, sid, "attacker.tld")
		require.NoError(t, err)
		assert.False(t, allowed, "host outside the allowlist must stay denied for session %q", sid)
	}
}

// ---- per-session sandbox routing (architecture §2.2, §5.3) ----------------

// TestSessionWorkspaces_RoutesPerSession is the X-03 regression test: tool
// calls from DIFFERENT sessions must land in DIFFERENT sandboxes — never one
// shared container. The router must provision (and key) a workspace per
// session id, so /workspace state never leaks across sessions.
func TestSessionWorkspaces_RoutesPerSession(t *testing.T) {
	ctx := context.Background()
	rt := truntimetest.NewFakeRuntimePort()
	router := newSessionWorkspaces(rt, nil)

	wsA, err := router.Workspace(ctx, "sess-a")
	require.NoError(t, err)
	wsB, err := router.Workspace(ctx, "sess-b")
	require.NoError(t, err)

	// State written through session A's workspace must be invisible to B's.
	require.NoError(t, wsA.Write(ctx, "/workspace/hello.txt", []byte("A's secret")))
	_, err = wsB.Read(ctx, "/workspace/hello.txt")
	require.Error(t, err, "session B must not see session A's files (cross-session isolation)")

	// The runtime must have been asked for one workspace PER session id.
	require.NotNil(t, rt.WorkspaceFor("sess-a"))
	require.NotNil(t, rt.WorkspaceFor("sess-b"))
}

// TestSessionWorkspaces_ReusesLiveWorkspace asserts a second call for the same
// session re-attaches to the SAME live workspace (no destructive re-Create —
// runtime.Create tears down an existing container for clean-workspace resume,
// so the router must Get before Create).
func TestSessionWorkspaces_ReusesLiveWorkspace(t *testing.T) {
	ctx := context.Background()
	rt := truntimetest.NewFakeRuntimePort()
	router := newSessionWorkspaces(rt, nil)

	ws1, err := router.Workspace(ctx, "sess-a")
	require.NoError(t, err)
	require.NoError(t, ws1.Write(ctx, "/workspace/state.txt", []byte("alive")))

	ws2, err := router.Workspace(ctx, "sess-a")
	require.NoError(t, err)
	got, err := ws2.Read(ctx, "/workspace/state.txt")
	require.NoError(t, err, "same session must re-attach to its live workspace, not a fresh one")
	assert.Equal(t, "alive", string(got))
}

// TestSessionWorkspaces_EmptySessionIDFailsClosed asserts the router refuses an
// empty session id instead of falling back to any shared sandbox key (the exact
// failure mode of the old defaultSessionID binding).
func TestSessionWorkspaces_EmptySessionIDFailsClosed(t *testing.T) {
	router := newSessionWorkspaces(truntimetest.NewFakeRuntimePort(), nil)
	_, err := router.Workspace(context.Background(), "")
	require.Error(t, err, "empty session id must be refused, never routed to a shared sandbox")
}

// TestSessionWorkspaces_StampsOperatorAllowlist asserts each session's sandbox
// is created with the operator-configured egress allowlist on ITS OWN session-
// scoped policy, so the workspace's NetworkPolicy and the broker default agree.
func TestSessionWorkspaces_StampsOperatorAllowlist(t *testing.T) {
	ctx := context.Background()
	rt := truntimetest.NewFakeRuntimePort()
	router := newSessionWorkspaces(rt, []string{"api.example.com"})

	ws, err := router.Workspace(ctx, "sess-a")
	require.NoError(t, err)
	got, err := ws.NetworkPolicy(ctx)
	require.NoError(t, err)
	assert.Equal(t, app.EgressPolicy{SessionID: "sess-a", AllowedHosts: []string{"api.example.com"}}, got)
}

// TestBuildSessionStatus_ValidDSNReturnsLookup asserts the daemon constructs a
// session-status authority for the reaper from the event-store DSN: a nil
// SessionStatusFunc would silently degrade §10.6 to TTL-only reaping.
func TestBuildSessionStatus_ValidDSNReturnsLookup(t *testing.T) {
	statusFn, closeStatus, err := buildSessionStatus(baseConfig(t))
	require.NoError(t, err)
	require.NotNil(t, statusFn, "the reaper must get a session-status authority (architecture §10.6)")
	require.NoError(t, closeStatus())
}

// TestBuildSessionStatus_MalformedDSNFails asserts a bad DSN is a fatal wiring
// error, not a silent TTL-only fallback.
func TestBuildSessionStatus_MalformedDSNFails(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Postgres.DSN = "postgres://u@localhost:not-a-port/db"
	_, _, err := buildSessionStatus(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "session-status")
}
