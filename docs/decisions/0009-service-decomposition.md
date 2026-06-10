# 9. Service decomposition: 3 services + projectord worker (event store in-process)

Date: 2026-06-10
Status: Accepted

## Context

The research proposed a candidate four-way split: model-gateway, tool/execution-runtime,
persistence/event-store, and orchestrator. Before committing to that topology the
architecture applied a stress-test rule: a subsystem becomes its own deployable service
only if it has a distinct trust boundary, a distinct failure/scaling profile, or a
distinct dependency surface that would otherwise contaminate the rest of the binary —
AND extraction buys something an in-process package against a shared external datastore
cannot.

The most consequential question was whether a thin Go gRPC shim in front of PostgreSQL
(the draft event-store service) was worth its cost. Every agent turn appends multiple
events (UserMessage, AssistantMessage, ToolExecutionStarted, ToolResult, TurnFinished,
and so on), recursively multiplied across depth-limited sub-agents. Each append, as a
network call to a separate service, paid gRPC/mTLS amortization, protobuf
marshal/unmarshal, an extra failure mode, and a retry regime — for what is a single SQL
INSERT. The "independent backup/migration/read-scaling" justification belongs to
PostgreSQL, not to a Go shim; PostgreSQL is already a separate process with its own
backup, migration, and replica story.

The two services that do have genuinely distinct characteristics are model-gateway
(three heavy vendor SDKs plus an OpenAI-compatible adapter; I/O-bound on slow external
upstreams with distinct rate limits; independently scalable and restartable) and
tool-runtime (the only component that runs model-influenced code; distinct trust
boundary for the lethal-trifecta blast radius; must run under the tightest network
policy; can swap container for microVM behind its port).

A fourth deployable, projectord, is real read-side work that must run off the request
path and cannot be folded into the orchestrator without polluting the loop's lifecycle.

## Decision

We will deploy three long-lived service binaries plus one projection worker:

1. **orchestrator** (`cmd/boltrope-orchestratord`) — runs the agent loop, drives turns,
   enforces permissions and hooks, manages the context/token budget, and spawns
   depth-limited sub-agents. It also embeds the event store as an in-process package
   (`internal/orchestrator/adapter/outbound/eventstore`) that talks to PostgreSQL over
   pgx. The `EventLogPort` interface is consumer-defined in the orchestrator's app layer;
   if a concrete need for a separate writer service arises later, only the adapter
   implementation changes.

2. **model-gateway** (`cmd/boltrope-modelgwd`) — stateless provider-abstraction proxy:
   normalizes the internal message/tool model to and from each provider SDK, streams
   token deltas, counts tokens, exposes per-(endpoint,model) capability flags, and
   centralizes retry and stop-reason normalization.

3. **tool-runtime** (`cmd/boltrope-toolruntimed`) — owns the tool registry (native and
   MCP), validates tool inputs against JSON Schema, executes tools inside per-session
   sandboxes via the Workspace/Runtime port, runs MCP servers in confined sandboxes, and
   enforces egress policy at the network layer.

4. **projectord** (`cmd/boltrope-projectord`) — a long-lived worker (not on the request
   path) that tails the event log's Subscribe feed and runs read-side projections
   (cost-rollup, OTel export). Lag or restart never blocks a turn.

The sandbox is a runtime resource managed by tool-runtime behind the Workspace/Runtime
port, not a separate service. PostgreSQL and the egress broker are infrastructure.

In-process packages that are explicitly NOT services: the event store, context/memory
manager, permissions/guardrails engine, hooks/middleware runner, and sub-agent
orchestration. Sub-agents run as child loops in their own goroutines with their own
sessions; they write events as local pgx calls with no per-event network hop multiplied
across recursion depth — confirming evidence for the event-store demotion.

## Consequences

- The hot append path loses a gRPC service, its proto, its mTLS pair, and a per-append
  network round-trip, while retaining full resume/fork/replay capability because the
  events still live in PostgreSQL.
- Deployment surface is three services + one worker + one CLI + one migration binary,
  operable by a small team.
- The `EventLogPort` interface keeps a future extraction non-breaking (code change
  behind the port, not a schema change).
- Subsystem components that do not earn service status (context manager, policy engine,
  hooks) remain fast, synchronous, and testable without inter-service mocking.
- Sub-agent recursion produces no gRPC fan-out amplification.
- Tool-runtime can be placed under the tightest network policy and swapped from
  container to microVM behind its port without touching the orchestrator or gateway.
