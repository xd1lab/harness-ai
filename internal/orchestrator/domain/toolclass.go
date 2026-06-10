package domain

// SideEffect classifies whether a tool mutates state, which the orchestrator's
// scheduler uses to decide concurrency: read-only tools dispatch concurrently in a
// bounded pool, while mutating tools are serialized in emitted order via the
// per-session mutation mutex (ADR-0014; architecture §9.2). It is the
// orchestrator-side view of the classification the tool-runtime declares on each
// tool's spec; both services reason about it, and per architecture §12.4 each
// service owns its own copy of this small vocabulary rather than importing the
// other's domain. The authoritative producer is the tool-runtime
// ([github.com/boltrope/boltrope/internal/toolruntime/domain]); the wire mapping
// reconciles the two at the transport edge.
type SideEffect string

const (
	// SideEffectReadOnly marks a tool that does not mutate workspace or external
	// state (e.g. read, glob, grep). Read-only tools may be dispatched concurrently
	// through the bounded errgroup pool (architecture §9.2). NOTE: webfetch and
	// websearch are NOT read-only for scheduling — they carry
	// [EgressClassExternal] and go through the policy/egress path, never the
	// read-only pool (ADR-0013; architecture §8.4, §9.2).
	SideEffectReadOnly SideEffect = "read_only"
	// SideEffectMutating marks a tool that mutates state (e.g. edit, write, bash).
	// Mutating tools are serialized per session and are never auto-retried on the
	// RPC layer (ADR-0012; architecture §9.2). MCP tools default to mutating
	// (fail-safe; ADR-0014).
	SideEffectMutating SideEffect = "mutating"
)

// EgressClass classifies a tool's outbound-network reach, which the orchestrator's
// policy uses to apply the egress allowlist and the taint gate: an
// [EgressClassExternal] tool targeting a non-allowlisted host requires a human ask
// once untrusted content has entered the session (ADR-0013; architecture §8.4). It
// is a THREE-class set — [EgressClassNone], [EgressClassInternal],
// [EgressClassExternal] — matching the tool-runtime domain EgressClass, the wire
// EgressClass enum, and FR-TOOL-02. Like [SideEffect], it is the orchestrator-side
// copy of the tool-runtime's declared classification (architecture §12.4).
type EgressClass string

const (
	// EgressClassNone marks a tool that performs no network egress.
	EgressClassNone EgressClass = "none"
	// EgressClassInternal marks a tool whose egress is confined to
	// internal/allowlisted hosts; it is subject to the egress broker's allowlist
	// policy but is not external-world reach.
	EgressClassInternal EgressClass = "internal"
	// EgressClassExternal marks a tool that performs external communication (e.g.
	// webfetch, websearch, an MCP http tool). It is reclassified from a "read" to
	// external comms precisely because a read of an attacker URL is a write to the
	// attacker (ADR-0013; architecture §8.4). It is also the fail-safe class an
	// unannotated/MCP tool defaults to, and the unset zero value ("") is treated as
	// external by the egress gate, never as none (deny-by-default; ADR-0014).
	EgressClassExternal EgressClass = "external"
)
