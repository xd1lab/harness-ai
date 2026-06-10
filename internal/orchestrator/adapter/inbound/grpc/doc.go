// Package grpc implements the orchestrator's client-facing gRPC edge: the
// generated [genproto.OrchestratorServiceServer] (CreateSession, GetSession,
// Run, Control, Fork) plus the edge authentication interceptor (T-ORCH-01,
// T-ORCH-02; FR-API-01/02/03).
//
// # Boundary
//
// This is the only place the orchestrator maps gen/ wire types ⇄ domain / llm
// kernel types on the client edge (architecture §12.4). The agent loop
// ([github.com/boltrope/boltrope/internal/orchestrator/app/agent]) and the
// consumer-defined ports ([github.com/boltrope/boltrope/internal/orchestrator/app])
// never see gen/; the [Server] adapts between them.
//
// # Run (server-stream, resumable)
//
// [Server.Run] is resumable by construction: the request carries after_seq, and
// delivery is driven entirely by [app.EventLogPort.Subscribe], which replays the
// committed events with seq strictly greater than after_seq and then tails live
// (FR-API-01). The agent loop is started on a background goroutine whose
// [agent.ClientSink] forwards live TextDelta/ThinkingDelta fragments; the loop
// tails the durable log so a slow client never backpressures upstream generation
// (NFR-REL-05; architecture §9.4). Each [genproto.RunEvent] frame carries its
// event seq (the Last-Event-ID) so a reconnecting client resumes exactly.
//
// # Control (unary)
//
// [Server.Control] performs out-of-band actions decoupled from the Run data
// stream (architecture §4.2): Approve/Deny resolve a pending
// [app.ApprovalGate.Resolve]; Interrupt cancels the running session loop's
// context (FR-LOOP-03); Reattach reports the current head as the resume cursor.
//
// # Fork / sessions
//
// [Server.Fork] delegates to [app.EventLogPort.Fork] under tenant-ownership
// enforcement (FR-STATE-03, architecture §8.9). CreateSession opens a fresh
// stream; GetSession returns the materialized [domain.Session] projection.
//
// # Edge auth (FR-API-03; architecture §8.7)
//
// [NewAuthInterceptor] validates the bearer JWT (iss/aud/exp), PINS the accepted
// signing algorithms and REJECTS alg=none, and derives the tenant_id + principal
// onto the request context (both as this package's typed values and via
// [db.WithTenant] so the event store's RLS GUC acquire-hook scopes every borrowed
// connection). A dev mode behind BOLTROPE_DEV_INSECURE accepts a fixed dev
// principal; absent dev mode and a verifier, the edge fails closed. Session
// ownership on Run/Control/Fork and a per-tenant in-flight concurrency cap are
// enforced in the handlers, not the interceptor.
package grpc
