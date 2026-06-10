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
	"github.com/xd1lab/harness-ai/internal/platform/secret/secrettest"
)

// baseConfig returns a Config that passes validation with no OTLP export and
// dev-insecure on (so wiring needs neither a collector nor a SPIRE agent).
func baseConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		Server:       config.ServerConfig{GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"},
		Postgres:     config.PostgresConfig{DSN: "postgres://localhost/x", Version: 14},
		OTLP:         config.OTLPConfig{Endpoint: ""},
		ModelGateway: config.ModelGatewayConfig{Endpoint: "localhost:9001"},
		Blob:         config.BlobConfig{Dir: t.TempDir()},
		LogLevel:     "info",
		DevInsecure:  true,
	}
}

// TestBuildProvider_DefaultOpenAICompat asserts that with no provider override
// the wiring constructs the keyless OpenAI-compatible provider against the
// configured base URL — the default that works in local/compose with no API key
// (FR-MODEL-01 AC-2 path).
func TestBuildProvider_DefaultOpenAICompat(t *testing.T) {
	gw := gatewaySettings{Provider: "", OpenAIBaseURL: "http://localhost:11434/v1"}
	prov, endpoint, err := buildProvider(context.Background(), gw, secrettest.NewFakeSecrets(nil))
	require.NoError(t, err)
	require.NotNil(t, prov)
	assert.Equal(t, "openaicompat", endpoint)
}

// TestBuildProvider_UnknownProviderErrors asserts an unrecognized provider kind
// fails fast (NFR-OPS-04) rather than silently picking a default.
func TestBuildProvider_UnknownProviderErrors(t *testing.T) {
	_, _, err := buildProvider(context.Background(), gatewaySettings{Provider: "wat"}, secrettest.NewFakeSecrets(nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wat")
}

// TestBuildProvider_StubRequiresNoKey asserts that provider=stub constructs
// without an API key (keyless E2E / DOD-05): no env var is read and no error is
// returned. The endpoint name "stub" is returned for capability resolution.
func TestBuildProvider_StubRequiresNoKey(t *testing.T) {
	gw := gatewaySettings{Provider: "stub"}
	prov, endpoint, err := buildProvider(context.Background(), gw, secrettest.NewFakeSecrets(nil))
	require.NoError(t, err)
	require.NotNil(t, prov)
	assert.Equal(t, "stub", endpoint)
}

// TestBuildProvider_AnthropicResolvesKey asserts that selecting the anthropic
// provider resolves its API key via the secrets port from the configured env-var
// NAME, and errors when that name is unset (env-only secrets; ADR-0013).
func TestBuildProvider_AnthropicResolvesKey(t *testing.T) {
	// keyEnv is the NAME of the env var holding the key, not a credential.
	const keyEnv = "ANTHROPIC_API_KEY"

	t.Run("key present", func(t *testing.T) {
		prov, endpoint, err := buildProvider(context.Background(),
			gatewaySettings{Provider: "anthropic", APIKeyEnv: keyEnv},
			secrettest.NewFakeSecrets(map[string]string{keyEnv: "sk-test"}))
		require.NoError(t, err)
		require.NotNil(t, prov)
		assert.Equal(t, "anthropic", endpoint)
	})

	t.Run("key missing errors", func(t *testing.T) {
		_, _, err := buildProvider(context.Background(),
			gatewaySettings{Provider: "anthropic", APIKeyEnv: keyEnv},
			secrettest.NewFakeSecrets(nil))
		require.Error(t, err)
	})
}

// TestRun_BadConfig_FailsFast asserts Run returns an error (does not panic or
// hang) when given a config missing a required field.
func TestRun_BadConfig_FailsFast(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Server.GRPCAddr = "" // make it invalid by re-validating via Run's guard
	// Run does not re-validate (Load did), so instead assert an invalid LISTEN
	// address surfaces an error quickly.
	cfg.Server.GRPCAddr = "256.256.256.256:1"
	err := Run(context.Background(), cfg, io.Discard)
	require.Error(t, err)
}

// TestRun_ConstructsAndShutsDown is the daemon smoke: Run wires the gateway,
// serves, and returns cleanly when the context is cancelled — proving the whole
// dependency graph constructs without panic given a minimal config.
func TestRun_ConstructsAndShutsDown(t *testing.T) {
	t.Setenv("BOLTROPE_DEV_INSECURE", "1")
	cfg := baseConfig(t)

	ctx, cancel := context.WithCancel(context.Background())
	var buf strings.Builder
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, &buf) }()

	// Give Run a moment to bind, then cancel and assert a clean return.
	waitThenCancel(t, cancel)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func waitThenCancel(t *testing.T, cancel context.CancelFunc) {
	t.Helper()
	// A short sleep lets the listeners bind before we trigger shutdown; the
	// lifecycle is otherwise deterministic.
	time.Sleep(150 * time.Millisecond)
	cancel()
}
