# 10. Inter-service communication: gRPC/protobuf, server-streaming, resumable client edge, no broker on request path

Date: 2026-06-10
Status: Accepted

## Context

The agent loop is intrinsically request/response with streaming: a client submits a
turn, the orchestrator streams token deltas and tool-progress frames back, and control
messages (approve/deny/interrupt) arrive on a side channel. This pattern needs ordered,
flow-controlled, cancellation-propagating streaming with strongly-typed contracts.

A message broker (NATS/Kafka) on the request path would add an always-on dependency
and at-least-once redelivery semantics that conflict with the loop's need for ordered,
exactly-relayed token deltas and immediate backpressure. The event log already provides
durable async delivery where it matters; a broker on the synchronous path buys nothing.

REST/JSON over HTTP/1.1 internally lacks native server-streaming with flow control and
backpressure, and leaves service contracts untyped. Bidirectional streaming at the
public client edge is over-engineered and proxy-hostile.

Because the event store is in-process (ADR-0009) there is no event_store.proto and no
orchestrator-to-event-store RPC, removing one of the draft's four proto files.

## Decision

We will use gRPC over HTTP/2 with Protocol Buffers (proto3) for all service-to-service
calls. The IDL is compiled with buf (lint + breaking-change detection in CI) producing
Go stubs via protoc-gen-go and protoc-gen-go-grpc.

Streaming patterns per boundary:

- **Client → orchestrator Run**: server-streaming, resumable. Each outbound frame
  carries its event seq. The client request carries an optional last_event_id; a
  reconnecting client resumes from that seq via the Reattach path rather than getting a
  broken stream.
- **Client → orchestrator Control**: unary. Approve/Deny/Interrupt/Reattach are
  separate from the data stream; this decouples control from delivery and avoids bidi
  at the public edge.
- **Orchestrator → model-gateway Generate**: server-streaming. The gateway streams
  normalized StreamEvent{TextDelta|ThinkingDelta|ToolCallDelta|Pause|Done}. All
  provider stream-shape handling lives in the gateway; the orchestrator relay is
  provider-agnostic.
- **Orchestrator → tool-runtime ExecuteTool**: server-streaming. Long-running tools
  stream progress and partial stdout, then a terminal ToolResult. Cancellation
  propagates via context.
- **Event store (in-process)**: direct Go calls over EventLogPort. Append is a single
  optimistic SQL transaction; no network, no RPC.

Client-streaming and bidirectional streaming are avoided in v1.

The outer client API (CLI/IDE/CI/SDK to orchestrator) is gRPC by default with an
optional REST/JSON facade via grpc-gateway generated from the same protos. The REST
facade enforces identical authentication, authorization, and rate limiting to the gRPC
edge — it is a transcoding layer, not a second trust boundary.

Generation is decoupled from delivery: the orchestrator generates, persists checkpoints
and assembled messages to the event log, and the client tails the log. A relay
stall/idle deadline detaches a stalled client so a slow client cannot backpressure the
upstream provider into holding a rate-limit slot.

Service-to-service retry policy: auto-retry only UNAVAILABLE/DEADLINE_EXCEEDED on
genuinely idempotent calls (Capabilities, CountTokens, ExecuteTool of a ReadOnly tool).
Never auto-retry ExecuteTool of a Mutating tool or Generate. ExecuteTool carries a
log-derived idempotency key (hash(session_id, seq_of_ToolCall)) not a fresh UUID.

## Consequences

- gRPC's HTTP/2 framing provides native server-streaming with flow control and
  cancellation propagation — exactly what relaying token deltas across three hops
  requires.
- The strongly-typed IDL (protobuf + buf) enforces the "highly usable interface"
  discipline and catches breaking changes in CI.
- The resumable Run stream means client disconnects do not lose in-flight generation;
  the client re-attaches at the last delivered seq.
- No broker dependency on the request path; durability lives in the event log.
- The grpc-gateway REST facade gives non-gRPC clients access at zero duplication of
  auth logic.
- The retry policy distinction between read-only and mutating calls prevents accidental
  double execution of side-effecting tools.
- Decoupled generation/delivery eliminates the availability hazard of a slow client
  holding a provider concurrency slot.
