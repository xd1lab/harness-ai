// Package tools implements the native tools that make up the Agent-Computer
// Interface (ADR-0013; architecture §2.3, §8.13): bash, read, write, edit, glob,
// grep, webfetch, and websearch. Each is a [github.com/boltrope/boltrope/internal/toolruntime/domain.Tool]
// that declares its [domain.ToolSpec] (name, model-facing description, input JSON
// Schema, and the [domain.SideEffect]/[domain.EgressClass] safety classifications)
// and executes through the injected app ports — a per-session
// [github.com/boltrope/boltrope/internal/toolruntime/app.Workspace] (filesystem +
// in-sandbox command execution) and, for the external-comms tools, the
// [github.com/boltrope/boltrope/internal/toolruntime/app.EgressBroker]
// (deny-by-default per-session allowlist).
//
// # Classification (FR-TOOL-02)
//
//	tool       SideEffect   EgressClass
//	read       ReadOnly     None
//	glob       ReadOnly     None
//	grep       ReadOnly     None
//	write      Mutating     None
//	edit       Mutating     None
//	bash       Mutating     None
//	webfetch   Mutating     External
//	websearch  Mutating     External
//
// read/glob/grep are read-only and eligible for the orchestrator's bounded
// read-only parallel pool. write/edit/bash mutate workspace state and are
// serialized per session. webfetch/websearch are external communication — a read
// of an attacker-controlled URL is a write to the attacker (ADR-0013) — so they
// carry [domain.EgressClassExternal], route through the [app.EgressBroker]
// (fail-closed on a denied or unparseable host), and are NOT parallelized as
// harmless reads (architecture §8.4, §9.2).
//
// # Validation boundary
//
// These tools assume their arguments were already validated against their JSON
// Schema (validate-then-execute; the registry wraps each tool with a validating
// decorator, FR-TOOL-01). Each [domain.Tool.Execute] is nonetheless defensive: it
// reads only the fields it understands and returns an error [domain.Observation]
// (never a panic) when a required field is missing or of the wrong type, so a
// tool invoked directly in a test without the registry wrapper still degrades
// gracefully.
package tools
