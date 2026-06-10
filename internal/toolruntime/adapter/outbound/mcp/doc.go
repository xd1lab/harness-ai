// Package mcp implements [github.com/boltrope/boltrope/internal/toolruntime/app.MCPClientPort]
// as a minimal Model Context Protocol (MCP) client over JSON-RPC 2.0 on a stdio
// transport. It is one of the tool-runtime's outbound adapters: it spawns a
// third-party MCP server as a confined subprocess and talks to it over the
// subprocess's stdin/stdout (architecture §5.3 "mcp", §8.11; ADR-0013).
//
// # Protocol
//
// The client speaks the minimum of MCP needed by the tool-runtime, framed as
// newline-delimited JSON-RPC 2.0 over stdio:
//
//   - initialize — the handshake performed before any other call. The client
//     sends its protocol version and (empty) client info; the server replies
//     with its protocolVersion and serverInfo (name/version). The client then
//     sends the notifications/initialized notification.
//   - tools/list — fetched LAZILY, only when the registry asks (via
//     [Client.ListTools]); never eagerly at construction.
//   - tools/call — invokes a tool ([Client.CallTool]) and maps the result's
//     content blocks to a [github.com/boltrope/boltrope/internal/toolruntime/domain.Observation].
//
// Each [Client.ListTools]/[Client.CallTool] call spawns a fresh stdio session
// (subprocess), performs the handshake, issues the one request, and tears the
// session down. v1 favors this stateless-per-call shape over a long-lived
// connection pool; it keeps confinement and cancellation simple.
//
// # Trust boundary (untrusted server)
//
// A third-party MCP server is an untrusted supply-chain and prompt-injection
// vector (ADR-0013 §"MCP server confinement"; architecture §8.11). This client
// therefore:
//
//   - Treats tool NAMES, DESCRIPTIONS, and SCHEMAS from the server as untrusted
//     DATA. They are carried verbatim onto [github.com/boltrope/boltrope/internal/toolruntime/domain.ToolSpec]
//     and surfaced to the registry's approval-on-first-use gate; they are never
//     interpreted as instructions and never injected into a model prompt by this
//     package. Tool-poisoning text in a description survives byte-for-byte so a
//     reviewer (or the approval queue) can see exactly what the server sent.
//   - Defaults every discovered tool to the FAIL-SAFE classifications
//     [github.com/boltrope/boltrope/internal/toolruntime/domain.SideEffectMutating]
//     and [github.com/boltrope/boltrope/internal/toolruntime/domain.EgressClassExternal]
//     — never ReadOnly/None — so an unannotated MCP tool is maximally gated.
//   - Pins server identity/version: [app.MCPServerRef.VersionPin], when set, is
//     compared against the hash of the server's reported serverInfo
//     (see [PinFor]); a mismatch returns [ErrVersionPinMismatch] so the server
//     is gated until re-approved.
//   - Confines the subprocess: the spawned server is launched with a SCRUBBED
//     environment (configured via [WithServer]); it inherits NONE of
//     tool-runtime's environment by default, so service credentials and the
//     SPIRE Workload API socket/SVID are never exposed into the server's
//     namespace. (Network egress confinement for http-transport servers is
//     enforced separately by the egress broker; this client implements the
//     stdio transport.)
//
// # Concurrency
//
// A [Client] is safe for concurrent use by multiple goroutines: it holds only
// immutable configuration and spawns an independent session per call.
package mcp
