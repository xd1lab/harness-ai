// Package execute implements the tool-runtime's ExecuteTool use-case (T-TR-07):
// the orchestration that turns one validated tool call into a streamed terminal
// result, behind the frozen consumer-defined ports in
// [github.com/xd1lab/harness-ai/internal/toolruntime/app].
//
// # Flow
//
// Given a tool call plus a log-derived idempotency key, [Service.Execute]:
//
//  1. looks up the tool in the [app.ToolRegistry] (the registry returns a
//     validate-then-execute decorator, so JSON-Schema validation happens on
//     dispatch and a schema violation surfaces as an error
//     [github.com/xd1lab/harness-ai/internal/toolruntime/domain.Observation],
//     never a panic; FR-TOOL-01);
//  2. ensures the per-session [app.Workspace] sandbox exists via the
//     [app.RuntimePort] so cancellation propagates to a real in-sandbox kill
//     (architecture §9.3);
//  3. consults the durable dedup ledger ([app.DedupStore]): a Mutating call
//     whose key is already completed returns the prior result without
//     re-executing — at-most-once side effects (ADR-0012; architecture §7.2);
//  4. enforces the [app.EgressBroker] for [domain.EgressClassExternal] tools
//     before any execution — deny-by-default, fail-closed on ambiguity (ADR-0013;
//     architecture §8.4);
//  5. executes the tool, streaming interim [Progress] through the injected
//     [Emitter], then returns the terminal result, offloading output larger than
//     [BlobThresholdBytes] to the [github.com/xd1lab/harness-ai/internal/platform/blob.BlobStorePort]
//     (write-before-reference; architecture §6.4);
//  6. records completion (or failure) in the dedup ledger.
//
// # Purity & injection
//
// Nothing here imports gen/. All collaborators are injected via [Config] so the
// use-case is exercised entirely against the truntimetest fakes and the
// in-memory blob fake — no Docker, no Postgres, no network (architecture §5.3,
// §12.4). The gRPC server adapter maps gen ⇄ these app types at the transport
// edge and implements [Emitter] to relay progress on the wire.
//
// # Masking
//
// Best-effort secret masking of output before it leaves the boundary is
// defense-in-depth only and is applied by the caller's [Masker] when configured;
// the real exfiltration control is the egress broker, not masking (ADR-0013
// §"Output masking is defense-in-depth only"; architecture §8.10).
//
// Implementations are safe for concurrent use when their collaborators are.
package execute
