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
)

func baseConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		Server:       config.ServerConfig{GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"},
		Postgres:     config.PostgresConfig{DSN: "postgres://u@127.0.0.1:1/db?connect_timeout=1", Version: 14},
		OTLP:         config.OTLPConfig{Endpoint: ""},
		ModelGateway: config.ModelGatewayConfig{Endpoint: "localhost:9001"},
		Blob:         config.BlobConfig{Dir: t.TempDir()},
		LogLevel:     "info",
		DevInsecure:  true,
	}
}

// TestLoadOrchSettings_Defaults asserts the orchestrator-specific settings
// (downstream endpoints, default model, trust domain) default sensibly.
func TestLoadOrchSettings_Defaults(t *testing.T) {
	os := loadOrchSettings()
	assert.NotEmpty(t, os.ToolRuntimeEndpoint, "a default tool-runtime endpoint must be set")
	assert.NotEmpty(t, os.DefaultModel, "a default model must be set")
}

// TestBuildAuthConfig_DevMode asserts that in dev-insecure mode the edge-auth
// config is the permissive dev path (no Keyfunc/algorithms required). The
// production path (OIDC discovery + JWKS keyfunc, ADR-0020) is exercised
// end-to-end in orchestratord_oidc_test.go.
func TestBuildAuthConfig_DevMode(t *testing.T) {
	cfg := baseConfig(t)
	cfg.DevInsecure = true
	ac := buildAuthConfig(cfg, orchSettings{}, nil)
	assert.True(t, ac.DevInsecure, "dev-insecure config must enable the dev auth path")
}

// TestRun_ConstructsAndShutsDown is the daemon smoke: Run wires the whole
// orchestrator (event store pool, model-gateway + tool-runtime clients, approval
// gate, hooks, policy, sub-agent, context manager, OrchestratorService server),
// serves over mTLS, and returns cleanly on context cancel — proving the entire
// dependency graph constructs without panic given a minimal config. No external
// service is contacted at construction (the pool and gRPC clients connect lazily).
func TestRun_ConstructsAndShutsDown(t *testing.T) {
	t.Setenv("BOLTROPE_DEV_INSECURE", "1")
	cfg := baseConfig(t)

	ctx, cancel := context.WithCancel(context.Background())
	var buf strings.Builder
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, &buf) }()

	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(6 * time.Second):
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

// TestRun_ProductionAuthWithoutKeyfuncFailsClosed asserts that a non-dev
// orchestrator without an OIDC issuer/JWKS configured refuses to start (the
// edge-auth interceptor fails closed; NFR-SEC-01). DevInsecure is false and no
// SPIFFE source is wired, so this also exercises the SPIFFE-or-exit guard; either
// guard returning an error satisfies the fail-closed requirement.
func TestRun_ProductionAuthWithoutKeyfuncFailsClosed(t *testing.T) {
	cfg := baseConfig(t)
	cfg.DevInsecure = false
	err := Run(context.Background(), cfg, io.Discard)
	require.Error(t, err)
}
