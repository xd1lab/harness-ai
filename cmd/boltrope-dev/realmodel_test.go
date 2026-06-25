// SPDX-License-Identifier: Apache-2.0

package main

// ADR-0029 (AC-1 / AC-2 / AC-3 / AC-4 / AC-5 / AC-12 / AC-14) — RED. The dev
// binary's REAL-MODEL opt-in plus the DEFAULT-POSTURE invariant.
//
// These tests pin the contract BEFORE implementation and therefore reference
// symbols that do not exist yet (the new parsedRunFlags fields, resolveModel, the
// banner's model/local-exec variants), so they are RED for the RIGHT reason: a
// missing feature, not a typo. They are hermetic — no real listener, no real
// model, no Docker — exercising the pure parse/resolve/banner seams only.

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/modelgateway/adapter/outbound/providers/stub"
)

// --- AC-1: DEFAULT INVARIANT — stub model, no-exec, loopback, NO-EXEC banner ---

// TestDefaultPosture_StubModel asserts that a bare `run` (no new flags) resolves
// the model id to "stub" and the keyless stub provider — never a real model.
func TestDefaultPosture_StubModel(t *testing.T) {
	cfg, err := parseRunFlags(nil)
	require.NoError(t, err)

	// The default model id is the literal "stub" (threaded into BOTH the loop
	// Config and the gRPC DefaultModel; AC-3).
	assert.Equal(t, "stub", cfg.model, "default model id must be \"stub\"")
	assert.Empty(t, cfg.modelURL, "default must have no real model URL")
	assert.False(t, cfg.enableLocalExec, "default must have local-exec OFF")
	assert.False(t, cfg.enableNativeSchema, "default must have native-schema OFF")

	port, id, _, isReal := resolveModel(cfg, noEnv())
	assert.Equal(t, "stub", id, "default resolved model id must be \"stub\"")
	assert.False(t, isReal, "default must NOT be a real model")
	require.NotNil(t, port)
	// The default wraps the keyless stub llm.Provider (devModel over stub.New()).
	_, isDevModel := port.(*devModel)
	assert.True(t, isDevModel, "default model port must be the *devModel wrapper")
}

// TestDefaultPosture_NoExecRuntime asserts a bare `run` resolves the no-exec
// Runtime (cmd/boltrope-dev/toolruntime.go) as the tool port — never the bridge.
func TestDefaultPosture_NoExecRuntime(t *testing.T) {
	cfg, err := parseRunFlags(nil)
	require.NoError(t, err)

	tools, err := resolveTools(cfg, noEnv())
	require.NoError(t, err)
	_, isNoExec := tools.(*Runtime)
	assert.True(t, isNoExec, "default tool port must be the no-exec *Runtime")
}

// TestDefaultPosture_BannerHasNoExecMarkerNotLocalExec asserts the default banner
// carries the NO-EXEC marker and NOT a LOCAL-EXEC marker, plus all the loud
// markers (AC-1 / AC-14).
func TestDefaultPosture_BannerHasNoExecMarkerNotLocalExec(t *testing.T) {
	cfg, err := parseRunFlags(nil)
	require.NoError(t, err)

	var b bytes.Buffer
	writeBanner(&b, cfg)
	banner := b.String()
	up := strings.ToUpper(banner)

	for _, marker := range []string{
		"NOT FOR PRODUCTION", "IN-MEMORY", "NO RLS", "NO mTLS", "NO OIDC", "LOOPBACK ONLY",
	} {
		assert.Containsf(t, banner, marker, "default banner must keep marker %q", marker)
	}
	assert.Contains(t, up, "NO-EXEC", "default banner must carry the NO-EXEC sandbox marker")
	assert.NotContains(t, up, "LOCAL-EXEC ENABLED",
		"default banner must NOT claim LOCAL-EXEC ENABLED when local-exec is OFF")
	// The default real-model flags being off, the banner must NOT print a Model line.
	assert.NotContains(t, banner, "Model       :",
		"default banner must NOT print a Model line when no real model is set")
}

// --- AC-2 / AC-3 / AC-4: --model-url selects the openaicompat-backed devModel ---

// TestModelURL_SelectsRealModelAndThreadsID asserts --model-url makes resolveModel
// return a real model (openaicompat-backed devModel) and that --model threads the
// id into the resolved id (which server.go feeds BOTH the loop Config and gRPC
// DefaultModel; AC-2 / AC-3).
func TestModelURL_SelectsRealModelAndThreadsID(t *testing.T) {
	cfg, err := parseRunFlags([]string{
		"--model-url", "http://localhost:11434/v1",
		"--model", "gemma",
	})
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:11434/v1", cfg.modelURL)
	assert.Equal(t, "gemma", cfg.model)

	port, id, endpoint, isReal := resolveModel(cfg, noEnv())
	require.NotNil(t, port)
	assert.True(t, isReal, "--model-url must select a real model")
	assert.Equal(t, "gemma", id, "--model must thread the model id")
	assert.Equal(t, "openaicompat", endpoint, "the real model endpoint label is openaicompat")
	// It is still wrapped as a *devModel (the loop's app.ModelGatewayPort), but the
	// underlying provider is NOT the stub.
	dm, ok := port.(*devModel)
	require.True(t, ok, "real model port must be the *devModel wrapper")
	_, isStub := dm.provider.(*stub.Provider)
	assert.False(t, isStub, "a real model must NOT be backed by the stub provider")
}

// TestModel_DefaultIDIsStub asserts --model defaults to "stub" when unset.
func TestModel_DefaultIDIsStub(t *testing.T) {
	cfg, err := parseRunFlags([]string{"--model-url", "http://localhost:11434/v1"})
	require.NoError(t, err)
	assert.Equal(t, "stub", cfg.model, "--model defaults to \"stub\" even with a real URL")
}

// TestModelAPIKeyEnv_ReadFromInjectedEnv asserts --model-api-key-env names an env
// var that is read from the INJECTED env (not os.Getenv), and that the key VALUE
// never appears in the banner (AC-4 / AC-14).
func TestModelAPIKeyEnv_NeverPrintedInBanner(t *testing.T) {
	const secretVal = "sk-super-secret-DO-NOT-LOG"
	cfg, err := parseRunFlags([]string{
		"--model-url", "http://localhost:11434/v1",
		"--model", "gemma",
		"--model-api-key-env", "MY_KEY",
	})
	require.NoError(t, err)
	assert.Equal(t, "MY_KEY", cfg.modelAPIKeyEnv)

	env := map[string]string{"MY_KEY": secretVal}
	// resolveModel must read the value from the injected env and NOT panic/err.
	port, _, _, isReal := resolveModel(cfg, env)
	require.NotNil(t, port)
	assert.True(t, isReal)

	var b bytes.Buffer
	writeBanner(&b, cfg)
	assert.NotContains(t, b.String(), secretVal,
		"the API key VALUE must NEVER be printed in the banner")
	assert.NotContains(t, b.String(), "MY_KEY",
		"even the key env NAME need not leak; the banner shows endpoint+id only")
}

// --- AC-14: banner shows the model endpoint+id when a real model is set ---

// TestBanner_RealModelShowsEndpointAndID asserts the banner prints a Model line
// with the endpoint + id (and not the key) when a real model is configured.
func TestBanner_RealModelShowsEndpointAndID(t *testing.T) {
	cfg, err := parseRunFlags([]string{
		"--model-url", "http://localhost:11434/v1",
		"--model", "gemma",
	})
	require.NoError(t, err)

	var b bytes.Buffer
	writeBanner(&b, cfg)
	banner := b.String()
	assert.Contains(t, banner, "Model       :", "real-model banner must print a Model line")
	assert.Contains(t, banner, "gemma", "the Model line must show the model id")
	assert.Contains(t, banner, "openaicompat", "the Model line must show the endpoint label")
}

// --- AC-14: banner LOCAL-EXEC variant ---

// TestBanner_LocalExecReplacesNoExecMarker asserts that with --enable-local-exec
// the banner REPLACES the NO-EXEC marker with a LOCAL-EXEC ENABLED (Docker
// isolation) marker, while keeping every loud marker (AC-14).
func TestBanner_LocalExecReplacesNoExecMarker(t *testing.T) {
	cfg, err := parseRunFlags([]string{"--enable-local-exec"})
	require.NoError(t, err)
	assert.True(t, cfg.enableLocalExec)

	var b bytes.Buffer
	writeBanner(&b, cfg)
	banner := b.String()
	up := strings.ToUpper(banner)

	assert.Contains(t, up, "LOCAL-EXEC ENABLED",
		"local-exec banner must announce LOCAL-EXEC ENABLED")
	assert.Contains(t, up, "DOCKER",
		"local-exec banner must mention Docker isolation")
	assert.NotContains(t, up, "NO-EXEC (MODEL-GENERATED",
		"local-exec banner must NOT keep the NO-EXEC refusal line")
	for _, marker := range []string{
		"NOT FOR PRODUCTION", "IN-MEMORY", "NO RLS", "NO mTLS", "NO OIDC", "LOOPBACK ONLY",
	} {
		assert.Containsf(t, banner, marker, "local-exec banner must KEEP marker %q", marker)
	}
}

// --- AC-5: --enable-native-schema applies the endpoint override ---

// TestNativeSchema_AppliesEndpointOverride asserts that --enable-native-schema
// (with --model-url) makes the resolved capability registry report
// SupportsJSONSchemaStrict=true for the openaicompat endpoint, and that WITHOUT
// the flag the conservative default (false) applies (AC-5).
func TestNativeSchema_AppliesEndpointOverride(t *testing.T) {
	t.Run("flag on -> native json_schema enabled", func(t *testing.T) {
		cfg, err := parseRunFlags([]string{
			"--model-url", "http://localhost:11434/v1",
			"--model", "gemma",
			"--enable-native-schema",
		})
		require.NoError(t, err)
		assert.True(t, cfg.enableNativeSchema)

		reg := buildCapabilityRegistry(cfg)
		require.NotNil(t, reg, "a real model must build a capability registry")
		caps := reg.Resolve("openaicompat", "gemma")
		assert.True(t, caps.SupportsJSONSchemaStrict,
			"--enable-native-schema must turn on native json_schema for the openaicompat endpoint")
	})

	t.Run("flag off -> conservative default", func(t *testing.T) {
		cfg, err := parseRunFlags([]string{
			"--model-url", "http://localhost:11434/v1",
			"--model", "gemma",
		})
		require.NoError(t, err)

		reg := buildCapabilityRegistry(cfg)
		require.NotNil(t, reg)
		caps := reg.Resolve("openaicompat", "gemma")
		assert.False(t, caps.SupportsJSONSchemaStrict,
			"without the flag the conservative default (no native json_schema) must apply")
	})
}

// --- AC-12: prod-signal fence retained even WITH the new model/local-exec flags --

// TestProdSignal_RefusesEvenWithNewFlags asserts that a production signal still
// forces a fail-closed refusal even when --enable-local-exec and --model-url are
// passed together (AC-12).
func TestProdSignal_RefusesEvenWithNewFlags(t *testing.T) {
	args := []string{
		"run",
		"--model-url", "http://localhost:11434/v1",
		"--model", "gemma",
		"--enable-local-exec",
		"--enable-native-schema",
	}
	for _, sig := range productionSignalEnv {
		t.Run(sig, func(t *testing.T) {
			var stderr bytes.Buffer
			exit, rc := dispatch(args, map[string]string{sig: "present"}, &stderr)
			assert.NotEqual(t, 0, exit, "a production signal must refuse even with the new opt-in flags")
			assert.Nil(t, rc, "no run config when a production signal is present")
			assert.NotEmpty(t, strings.TrimSpace(stderr.String()))
		})
	}
}
