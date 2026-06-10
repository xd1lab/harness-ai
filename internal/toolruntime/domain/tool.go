// Package domain defines the tool-runtime's pure domain types: the [Tool]
// abstraction, its [ToolSpec] declaration (name, description, JSON Schema, and the
// safety classifications [SideEffect] and [EgressClass]), and the [Observation]
// result of executing a tool. It has zero infrastructure dependencies (no gRPC, no
// sandbox SDK, no gen/); execution mechanics live in the app/adapter layers.
//
// The tool-runtime is the authoritative producer of a tool's classification. The
// orchestrator keeps its own small copy of the SideEffect/EgressClass vocabulary
// for scheduling and policy (architecture §12.4); the two are reconciled at the
// gRPC transport edge.
package domain

import (
	"context"
	"encoding/json"
)

// SideEffect classifies whether a tool mutates state. It drives the orchestrator's
// scheduler — read-only tools may run concurrently in a bounded pool, mutating
// tools are serialized per session — and whether a tool is ever auto-retried
// (mutating tools are not) (ADR-0012; ADR-0014; architecture §9.2). MCP tools
// default to [SideEffectMutating] unless explicitly annotated (fail-safe).
type SideEffect string

const (
	// SideEffectReadOnly marks a tool that does not mutate workspace or external
	// state (e.g. read, glob, grep). It is eligible for the bounded read-only
	// parallel pool (architecture §9.2). Note: webfetch/websearch are NOT read-only
	// for scheduling purposes — they are [EgressClassExternal] and routed through
	// the egress/ask path (ADR-0013; architecture §8.4).
	SideEffectReadOnly SideEffect = "read_only"
	// SideEffectMutating marks a tool that mutates state (e.g. edit, write, bash).
	// It is serialized per session and never auto-retried at the RPC layer
	// (architecture §9.2; ADR-0012).
	SideEffectMutating SideEffect = "mutating"
)

// EgressClass classifies a tool's outbound-network reach. It is a THREE-class set
// — [EgressClassNone], [EgressClassInternal], [EgressClassExternal] — matching the
// wire EgressClass enum and FR-TOOL-02. An [EgressClassExternal] tool is subject to
// the per-session deny-by-default egress allowlist (enforced by the egress broker)
// and to the orchestrator's taint gate, and is never treated as a harmless
// parallelizable read (ADR-0013; architecture §8.4). Native tools declare their
// class explicitly; MCP and otherwise unannotated tools default fail-safe to
// [EgressClassExternal] (the maximally-gated class), never to [EgressClassNone].
type EgressClass string

const (
	// EgressClassNone marks a tool that performs no network egress.
	EgressClassNone EgressClass = "none"
	// EgressClassInternal marks a tool whose egress is confined to
	// internal/allowlisted hosts. It is still subject to the egress broker's
	// allowlist policy but is not external-world reach.
	EgressClassInternal EgressClass = "internal"
	// EgressClassExternal marks a tool that performs external communication (e.g.
	// webfetch, websearch, MCP http transports). It is reclassified from a "read"
	// to external comms because a read of an attacker-controlled URL is a write to
	// the attacker (ADR-0013; architecture §8.4). It is also the fail-safe class an
	// unannotated/MCP tool defaults to. The unset zero value ("") is likewise
	// treated as external by the egress gate, never as none.
	EgressClassExternal EgressClass = "external"
)

// ToolSpec is the declarative description of a tool: what it is called, what it
// does, the JSON Schema its arguments must satisfy (validated before execution —
// validate-then-execute; ADR-0013 §"Tool execution guardrails"; architecture
// §8.13), and the two safety classifications that govern scheduling and egress. It
// is the data the registry stores and merges (native + MCP) and the basis for the
// [github.com/xd1lab/harness-ai/internal/platform/llm.ToolDef] presented to the
// model.
type ToolSpec struct {
	// Name is the unique tool name the model uses to invoke the tool.
	Name string
	// Description is the model-facing description of what the tool does and when to
	// call it. For MCP tools this text is UNTRUSTED (tool-poisoning vector) and is
	// gated by approval-on-first-use before being surfaced (ADR-0013 §"MCP server
	// confinement").
	Description string
	// JSONSchema is the tool's input schema as raw JSON Schema (a JSON object),
	// carried verbatim. Inputs are validated against it before any execution
	// (architecture §8.13).
	JSONSchema json.RawMessage
	// SideEffect is the mutation classification driving concurrency and retry
	// (architecture §9.2). Defaults fail-safe to mutating for MCP tools.
	SideEffect SideEffect
	// EgressClass is the external-communication classification driving the egress
	// allowlist and taint gate (architecture §8.4). Defaults fail-safe to external
	// for MCP tools.
	EgressClass EgressClass
}

// IsReadOnly reports whether the tool is classified read-only (eligible for the
// bounded read-only parallel pool, subject to it not being external comms).
func (s ToolSpec) IsReadOnly() bool { return s.SideEffect == SideEffectReadOnly }

// IsExternal reports whether the tool performs external communication and is thus
// subject to the egress allowlist and taint gate.
func (s ToolSpec) IsExternal() bool { return s.EgressClass == EgressClassExternal }

// Observation is the result of executing a [Tool]: the model-visible content and
// whether the execution errored. It is the runtime-domain result type that the
// app/adapter layers turn into a streamed terminal result across the service
// boundary (and, in the orchestrator, into a persisted ToolResult event, offloading
// large content to a blob; architecture §6.4). It mirrors the (Content, IsError)
// shape of [github.com/xd1lab/harness-ai/internal/platform/llm.ToolResult] but is
// the tool-runtime's own domain type (architecture §12.4).
type Observation struct {
	// Content is the textual result of the tool execution as the model should see
	// it. For a large output this may be a lightweight/truncated descriptor with
	// the full bytes offloaded to a blob (see [Observation.Truncated] /
	// [Observation.BlobRef]; architecture §6.4).
	Content string
	// IsError reports whether the tool execution failed; when true the model is
	// told the call errored so it can adapt (architecture §3).
	IsError bool
	// Truncated reports whether Content is a truncated view of a larger output
	// retrievable via BlobRef.
	Truncated bool
	// BlobRef is the tenant-scoped blob key when the full output was offloaded,
	// empty when the content was inlined (architecture §6.4).
	BlobRef string
}

// Tool is an executable tool: its [ToolSpec] plus the ability to run against a
// validated set of arguments inside a session's workspace. Native tools implement
// this directly; MCP tools implement it by proxying to an MCP server via the MCP
// client. Inputs are schema-validated by the registry before [Tool.Execute] is
// invoked (validate-then-execute; architecture §8.13).
type Tool interface {
	// Spec returns the tool's declaration (name, description, schema,
	// classifications). It must be stable for a given registered tool.
	Spec() ToolSpec

	// Execute runs the tool for sessionID with the already-validated args and
	// returns the [Observation]. The supplied ctx carries cancellation that, for a
	// sandboxed tool, maps to a real process-group/cgroup kill of the in-sandbox
	// process tree, not merely an exec-wrapper cancel (architecture §9.3). A
	// non-nil error denotes a runtime/execution failure to surface; a tool that ran
	// but produced an error result reports that via [Observation.IsError] with a
	// nil error.
	Execute(ctx context.Context, sessionID string, args map[string]any) (Observation, error)
}
