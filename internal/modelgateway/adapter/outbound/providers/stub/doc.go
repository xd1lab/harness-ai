// Package stub implements a built-in, deterministic test/demo [llm.Provider] that
// streams scripted responses without contacting any external API.
//
// # Purpose
//
// The stub provider lets docker compose up run a real end-to-end agent task with
// BOLTROPE_MODELGW_PROVIDER=stub and no API key — it is the designated provider
// for local demo, CI smoke tests, and DOD-05 (keyless E2E). It is NOT suitable for
// production use and should never be configured with real user traffic.
//
// # Behavior
//
// Every [Provider.Stream] call returns the same deterministic script:
//
//  1. A [llm.TextDelta] event acknowledging the task.
//  2. If the request carries at least one [llm.ToolDef], a single canned
//     [llm.ToolCallDelta] for the first listed tool (call id "stub-call-1").
//  3. A terminal [llm.Done] with [llm.StopEnd] (or [llm.StopToolUse] when a tool
//     call was emitted) and believable fake [llm.Usage] counts.
//
// [Provider.Generate] builds a non-streaming [llm.Response] from the same script.
// [Provider.CountTokens] always returns a fixed estimate (512 tokens).
// [Provider.Capabilities] reports a conservative set suitable for most smoke tests.
//
// # Configuration
//
// Wire the provider by setting BOLTROPE_MODELGW_PROVIDER=stub (no other env vars
// are required or read). The model-gateway daemon selects it in [buildProvider] in
// cmd/boltrope-modelgwd/wiring.go when the setting matches "stub".
package stub
