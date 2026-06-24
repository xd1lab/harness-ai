<!-- SPDX-License-Identifier: Apache-2.0 -->

# ADR-0025: Event-log read + time-travel replay API — additive `ListSessionEvents`/`GetStateAtSeq` read RPCs, descriptor-by-default redaction, Load-then-fold (never Fork)

- **Status:** Accepted
- **Date:** 2026-06-24
- **Relates to:** ADR-0011 (event-store schema, tenant RLS, xmin-bounded projection cursor), ADR-0012 (durable turn boundaries), ADR-0013 (security model, RLS, tool-poisoning defenses), ADR-0020 (production OIDC edge auth), ADR-0022 (MCP Server mode), ADR-0023 (additive-proto precedent), ADR-0027 (Feature I admin read plane — sibling), the frozen `proto/` contract, the frozen `internal/platform/llm` kernel, and the frozen `app.EventLogPort`

## Context

The durable event log is write-only to humans: there was no read surface to list
a session's events or to reconstruct its state at an earlier sequence point. The
data existed (the `events` table, the pure fold functions) but nothing exposed it
safely. This is the wave-2 "make the invisible moat visible" theme — expose the
event log as a tenant-scoped, redacted **read plane**.

The questions this ADR settles: what carrier exposes event reads; how listing is
paginated safely; what redaction policy keeps sensitive payloads from leaking;
how "time-travel" reconstructs state without side effects; and the RLS scope.

## Decision

**1. Carrier — two additive read-only RPCs on `OrchestratorService`, with thin
REST + MCP facades.** Add `rpc ListSessionEvents(ListSessionEventsRequest)
returns (ListSessionEventsResponse)` and `rpc GetStateAtSeq(GetStateAtSeqRequest)
returns (GetStateAtSeqResponse)`. REST adds `GET /v1/sessions/{id}/events` and
`GET /v1/sessions/{id}/state`; MCP adds the `list_session_events` and
`get_state_at_seq` tools. Zero new auth — every facade reuses the shared
`authorizeTenant` + `authorizeSession` ownership path. REST-only is rejected (it
would force REST to re-implement ownership and drift from the other facades).

**2. Pagination — seq keyset.** `ListSessionEvents` pages on `after_seq`
(`seq > after_seq ORDER BY seq LIMIT page_size`, riding `idx_events_session_seq`,
no OFFSET); `page_size` defaults to 100 and is hard-capped at 1000. The response
carries `next_after_seq` and `has_more`. `after_seq` mirrors the existing
`RunRequest.after_seq`/`Last-Event-ID` resume vocabulary.

**3. Redaction — descriptor-by-default.** With `include_payload` unset (default)
the response is `EventDescriptor`s only: seq, type, actor, schema_version,
request_id, created_at (epoch ms), blob metadata, a bounded `summary`, and a
`redacted` flag. **Permanently omitted (even with `include_payload=true`):**
`provider_raw` (an opaque continuation blob) and `SessionStarted.SystemPrompt`.
`AssistantMessageDelta` crash checkpoints are **never** exposed (they are
recovery checkpoints, not delivery frames). A blob-bearing `ToolResult` returns a
blob descriptor (`has_blob` + media-type/size), never inlined bytes. Long text is
truncated into the summary (~2 KiB) with `redacted` set.
`MCPToolApprovalRequested.UntrustedDescription` is flagged untrusted, never
rendered as an instruction.

**4. Time-travel — Load-then-fold, NEVER Fork.** `GetStateAtSeq` reconstructs the
folded control/billing projection over the `[1..at_seq]` window using the bounded
`EventStore.LoadUpTo` read plus the SAME pure fold `GetSession`/`foldTotals` use.
It creates no session row and re-bills nothing (Fork would INSERT a billable
session row — explicitly not used). `at_seq <= 0` yields the empty state; `at_seq`
beyond head is clamped to head; the response echoes the reconstructed `at_seq`.

**5. Store methods — adapter-side superset, not the frozen port.** Two read-only
methods `LoadRange` and `LoadUpTo` are added to the `EventStore` consumer-superset
interface (`grpc/server.go`), NOT to the frozen `app.EventLogPort`. The pgx
`*Store` implements them via `beginTenantTx` (RLS auto-scoped); both are
side-effect-free.

**6. Timestamps are `int64` Unix epoch ms — no well-known types**, consistent
with the rest of the contract (proto imports zero WKTs).

## Consequences

- proto is additive (passes `buf breaking` FILE); `gen/` regenerated and committed
  in sync.
- The read path is provably side-effect-free (an integration test snapshots the
  sessions/events row counts before/after a series of reads).
- DEFERRED (honest): full model-visible conversation replay (v1 returns the
  control/billing projection only, to avoid dumping all sensitive payload at once);
  blob-bytes fetch endpoint (v1 returns descriptors only); a snapshot fast-path for
  very long sessions (v1 Loads the bounded window and folds).
