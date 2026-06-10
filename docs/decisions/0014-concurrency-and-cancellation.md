# 14. Concurrency and cancellation: single-goroutine loop, gated read-only parallelism, cgroup/PID-namespace kill, fenced lease, decoupled generation

Date: 2026-06-10
Status: Accepted

## Context

The agent loop manages several concurrent concerns: a streaming model call, parallel
tool execution (where safe), cancellation signals from the client, and sub-agent child
loops. Each of these has a correctness or availability hazard if handled incorrectly.

The draft had four gaps. First, it parallelized webfetch and websearch as harmless
reads; both are external communication and subject to the taint gate (ADR-0013).
Second, it cancelled tool execution by cancelling the docker exec client wrapper, which
kills only the wrapper — a SIGTERM-trapping process, a double-forked detached child, or
a fork bomb survives. Third, it deferred and then inadequately specified the
single-writer lease; a PostgreSQL session-scoped advisory lock has the wrong properties
(connection-pool-scoped, no TTL, no fencing; a hung-but-alive owner keeps its lock; a
connection blip silently frees it). Fourth, it coupled client delivery to provider
generation, meaning a slow client could hold a provider rate-limit/concurrency slot.

## Decision

**Single goroutine per session loop.** Each active session's agent loop runs in one
orchestrator goroutine that owns the turn lifecycle and the per-session in-memory state
(folded from the event log). It is the single writer to that session's stream, which is
why optimistic append rarely conflicts. context.Context threads through the entire turn
(client Run deadline to loop ctx to each downstream gRPC call); cancelling the client
stream, hitting a budget or turn cap, or receiving an Interrupt cancels the loop ctx
and propagates to all downstream calls.

**Bounded read-only parallelism, serialized mutations.** When a model response requests
multiple tool calls, the orchestrator partitions them:

- ReadOnly tools (glob, grep, read, and any tool with SideEffect == ReadOnly) dispatch
  concurrently through a bounded errgroup pool (SetLimit(N), default
  min(4, GOMAXPROCS), configurable).
- webfetch and websearch are NOT in the read-only pool; they are EgressClass = External,
  subject to the egress/ask gate, and scheduled through the policy path.
- State-mutating tools (edit, write, bash, any SideEffect == Mutating) are serialized
  in emitted order via a per-session mutation mutex — never concurrent with each other.
  Concurrent edits to one workspace are a correctness hazard and break deterministic
  replay.
- MCP tools default to Mutating/External unless explicitly annotated — fail-safe.

**Cgroup/PID-namespace kill with hard resource limits.** Each tool execution runs in
its own PID namespace and cgroup. On cancellation the signal goes to the whole process
group or cgroup (or the per-session container is stopped) with a SIGTERM to SIGKILL
deadline. Hard resource limits (CPU, memory, PIDs/ulimit, wall-clock) bound a
non-cooperating process regardless of signal handling. Required adversarial tests: a
SIGTERM-trapping process, a double-forked detached child, and a fork bomb must each be
terminated within the deadline. Unit tests for context-cancel cover the fake runtime
separately.

**Fenced lease with takeover.** We will use a lease row on `sessions` with
`lease_owner`, `lease_epoch`, `lease_expiry`, and a TTL renewed by heartbeat. Every
append carries the writer's lease_epoch; the append transaction rejects a stale epoch
(ADR-0011), so a fenced-out or stuck owner cannot append even if its expected_seq is
current. After TTL expiry a new owner atomically bumps lease_epoch and becomes the
writer. When an optimistic-append loser hits FAILED_PRECONDITION it reloads and rebases
if it still holds the lease, or yields the session if fenced. A projectord/ops check
flags sessions marked active with no event appended within X minutes (last_event_at),
enabling recovery. Combined with the durable execution intent from ADR-0012, a
stolen or expired lease cannot cause double side effects.

**Decoupled generation from delivery.** A relay stall/idle deadline (independent of the
turn deadline) detaches a stalled client. The loop generates, persists checkpoints and
assembled messages to the event log, and the client tails the log via the resumable
stream or Reattach. A slow client cannot backpressure the upstream provider into holding
a concurrency slot. Per-tenant in-flight generation caps bound provider-concurrency
exhaustion.

**Sub-agent concurrency.** Sub-agents run as child loops in their own goroutines with
their own sessions (forked or fresh); their event appends never contend with the
parent's stream. The parent blocks on the sub-agent tool call like any other tool.
Because the event store is in-process, recursive sub-agent writes are local pgx calls
with no per-event gRPC hop multiplied across recursion depth.

**Interrupt handling.** Control.Interrupt{session_id} signals the loop goroutine via a
per-session cancel function delivered through the ApprovalGate/control port (testable
without real gRPC). The loop appends a typed termination event and closes the stream
cleanly. State is in the log so an interrupted session is resumable.

## Consequences

- The single-goroutine loop keeps turn lifecycle simple and makes append conflict
  a rare exception rather than a common case.
- Bounded read-only parallelism gives performance where safe without exposing the
  mutation-correctness hazard of concurrent edits.
- Cgroup/PID-namespace kill with adversarial tests provides a real, verified guarantee
  that runaway sandbox processes are terminated — not an aspirational one.
- The fenced lease prevents a stuck or restarted writer from corrupting the event stream
  or double-executing side effects, and provides a TTL-based recovery path.
- Decoupled generation/delivery eliminates the availability hazard of a slow client
  holding a provider concurrency slot and makes reconnection cheap (tail from last seq).
- The explicit web-tool exclusion from the read-only pool ensures ADR-0013's taint gate
  is not bypassed by treating external-comms tools as harmless reads.
