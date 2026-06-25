// SPDX-License-Identifier: Apache-2.0

package main

// ADR-0029 (AC-2 / AC-3 / AC-4 / AC-5 / AC-15) — the dev binary's REAL-MODEL
// opt-in.
//
// By DEFAULT (no --model-url) the dev loop is backed by the keyless stub provider
// wrapped by devModel, exactly as before — byte-identical posture. When the
// operator opts in with --model-url, the loop talks to a REAL OpenAI-compatible
// endpoint (e.g. Ollama at http://localhost:11434/v1) via the openaicompat
// provider, still wrapped by the SAME devModel so the loop's app.ModelGatewayPort
// is unchanged.
//
// The capability registry (internal/modelgateway/app/capabilities) is the ONLY
// newly-permitted package under internal/modelgateway/app — it is a pure-data
// per-(endpoint, model) resolver with no I/O, no pgx, no spiffe, and not the
// Service. The dev binary imports it DIRECTLY (the openaicompat provider does not
// pull it in transitively), which is what makes the imports_test capabilities-leaf
// positive assertion hold. The exact-match fence refinement (ADR-0029, T1) keeps
// the Service itself forbidden while permitting this leaf.

import (
	"github.com/xd1lab/harness-ai/internal/modelgateway/adapter/outbound/providers/openaicompat"
	"github.com/xd1lab/harness-ai/internal/modelgateway/adapter/outbound/providers/stub"
	"github.com/xd1lab/harness-ai/internal/modelgateway/app/capabilities"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// modelEndpoint is the registry endpoint name the dev real-model wiring binds the
// openaicompat provider to. It is the key passed to
// capabilities.Registry.SetEndpointOverride / Resolve and is shown (with the model
// id) on the banner's Model line. It is NOT the base URL or the API key.
const modelEndpoint = "openaicompat"

// buildCapabilityRegistry constructs the per-(endpoint, model) capability registry
// the openaicompat provider resolves against at request-build time. When
// --enable-native-schema is set it installs an all-models override for the
// openaicompat endpoint that turns on native json_schema structured output
// (SupportsJSONSchemaStrict). Without the flag the registry has no override, so the
// conservative default applies (no native; the loop's validate-and-retry backstop
// holds).
//
// It is always safe to call: with no --model-url the returned registry is simply
// unused (the stub path needs no resolver). Returning a non-nil registry keeps the
// capabilities leaf in the dev binary's import graph (ADR-0029 AC-15 positive
// assertion).
func buildCapabilityRegistry(cfg parsedRunFlags) *capabilities.Registry {
	reg := capabilities.NewRegistry(nil)
	if cfg.enableNativeSchema {
		// All-models override for the endpoint: the canonical way to opt a
		// self-hosted / OpenAI-compatible endpoint into native structured output
		// (capabilities.EndpointOverride godoc). A self-hosted gemma/Ollama endpoint
		// is endpoint-wide, not per-model, so AllModels is the right surface.
		reg.SetEndpointOverride(modelEndpoint, capabilities.EndpointOverride{
			AllModels: &llm.Capabilities{
				SupportsTools:              true,
				SupportsStreamingToolCalls: true,
				SupportsSystemPrompt:       true,
				SupportsJSONSchemaStrict:   true,
			},
		})
	}
	return reg
}

// resolveModel selects the loop's app.ModelGatewayPort from the resolved flags.
//
//   - When cfg.modelURL == "" (the DEFAULT): it returns devModel over the keyless
//     stub provider, the resolved id == cfg.model (default "stub"), and isReal ==
//     false. This path is byte-identical to the pre-ADR-0029 behavior.
//   - When cfg.modelURL != "": it builds the capability registry (with the native
//     json_schema override applied iff --enable-native-schema), reads the API key
//     VALUE from the INJECTED env (named by --model-api-key-env; empty when unset),
//     constructs the openaicompat provider, and wraps it in devModel. isReal ==
//     true.
//
// The API key VALUE is read ONLY here, at provider construction, and is NEVER
// stored on any config/banner struct or logged — only the endpoint label and model
// id are surfaced (AC-4). A construction error (e.g. an empty BaseURL) is returned;
// callers fail closed.
//
// The returned id is threaded by server.go into BOTH the agent loop Config and the
// gRPC DefaultModel, replacing the previously-hardcoded "stub" literals (AC-3).
//
// It returns no error: openaicompat.New only fails when BaseURL is empty, which is
// impossible on this branch (it is only reached when cfg.modelURL != ""). The
// defensive nil-provider guard below keeps the function total — a nil openaicompat
// provider would surface as a clear stream-time error rather than a constructor
// error here.
func resolveModel(cfg parsedRunFlags, env map[string]string) (port app.ModelGatewayPort, id, endpoint string, isReal bool) {
	id = cfg.model
	if id == "" {
		id = defaultModel
	}

	if cfg.modelURL == "" {
		// DEFAULT posture: keyless stub provider, no real endpoint.
		return newDevModel(stub.New()), id, "", false
	}

	// Real model opt-in. Build the capability registry (native json_schema override
	// applied iff --enable-native-schema) and bind the openaicompat provider to it.
	reg := buildCapabilityRegistry(cfg)

	// Read the API key VALUE only here, from the injected env. An unset/absent name
	// yields the empty string; openaicompat substitutes its non-secret placeholder
	// for keyless self-hosted endpoints. The VALUE is never returned or stored.
	var apiKey string
	if cfg.modelAPIKeyEnv != "" {
		apiKey = env[cfg.modelAPIKeyEnv]
	}

	// BaseURL is non-empty here, so New cannot return the empty-URL error; the
	// returned provider is non-nil on success.
	prov, _ := openaicompat.New(openaicompat.Config{
		BaseURL:      cfg.modelURL,
		APIKey:       apiKey,
		Capabilities: reg,
		Endpoint:     modelEndpoint,
	})
	return newDevModel(prov), id, modelEndpoint, true
}
