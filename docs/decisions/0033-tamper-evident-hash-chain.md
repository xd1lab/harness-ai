# 0033. Tamper-evident audit log: per-event content hash + per-session hash-chain

Date: 2026-06-26
Status: Accepted

## Context

The README's headline promise is an engine for teams that must "audit every run".
The event log already is the single source of truth — an append-only, ordered,
per-session record (ADR-0011) under Row-Level Security (ADR-0003 / migrations
0003). But "append-only by application convention" is not the same as
"tamper-EVIDENT". Anyone with direct SQL access (a DBA, a compromised app role, a
restored-from-backup mutation, a bad migration) could `UPDATE` a stored
`events.payload`, rewrite history, and leave **no trace** an auditor could detect.
For a self-hosted product whose differentiator is a defensible audit trail, that
is a structural gap — and it is one competitors who hold run state in memory or in
a mutable table cannot close after the fact.

The hardening this batch (Batch-5A) delivers is the **CORE** of tamper-evidence:
a cryptographic chain computed *at append time* so any later mutation of a stored
event is **detectable** by recomputation. It deliberately stops at
tamper-EVIDENT; tamper-PROOF (an attacker cannot forge a self-consistent rewrite)
needs **signed checkpoints + an external SIEM/WORM export** so the chain head is
anchored outside the database an attacker controls — that is the explicit
follow-on (Batch-5B), not this batch.

Forces and constraints:

- The hot append path is the single-writer, optimistic-concurrency,
  lease-fenced, `request_id`-idempotent transaction in the event store
  (ADR-0011 / -0014). Adding hashing must not change any of those semantics: a
  re-sent `(session_id, request_id)` must still short-circuit to the prior
  envelopes WITHOUT re-chaining or double-incrementing.
- The same hashes must be computable by the pgx-free `boltrope-dev` binary
  (ADR-0024) so dev/prod parity holds and a dev verify works — the chain
  algorithm therefore lives in the dependency-light `domain` package, not the
  pgx store.
- Migration must be additive and forward-only — the repo has **zero** down
  migrations by convention (the `CheckForwardOnly` guard enforces it), and the
  existing RLS policies must keep gating every row with no new GRANT.
- The read-plane exposure must be additive proto only (ADR-0010 / buf-breaking
  additive), mirroring how `ListSessionEvents` (ADR-0025) was wired on all three
  facades (gRPC / REST / MCP).

## Decision

We add a **per-event content hash** and a **per-session SHA-256 hash-CHAIN**,
computed at append time in the single-writer transaction, plus a side-effect-free
server-side **verify** operation and additive read-plane exposure of the hashes.

### The chain primitives (single pgx-free source of truth)

All hashing lives in `internal/orchestrator/domain/hashchain.go` (stdlib
`crypto/sha256` + `encoding/json` only), so both the pgx store and the dev binary
import the **same** helpers and parity holds by construction:

- **`MarshalEventPayload(e Event) ([]byte, error)`** = `json.Marshal(e)` — the
  byte-identical encoding persisted **verbatim** in the RAW
  `events.payload_canonical BYTEA` column. `encoding/json` encodes struct fields
  in declaration order and **sorts map keys**, so even a map-bearing payload
  (`ApprovalRequested.Args`, `map[string]any`) marshals deterministically. The
  prod store's `marshalPayload` delegates to this, so prod hashes — and stores in
  `payload_canonical` — the **exact same bytes**, and verify-on-read hashes those
  identical raw stored bytes.
- **`content_hash = ContentHash(payload)`** = `sha256.Sum256(payload)` over those
  exact stored `payload_canonical` bytes (a fresh 32-byte slice). Verify hashes
  the RAW stored bytes directly (no decode/re-marshal), so structural/additive
  tampering of the stored payload (key reorder, whitespace, an injected extra key)
  changes the hashed bytes and is detected — see the Consequences caveat.
- **`GenesisChainHash(sessionID)`** = `sha256.Sum256("boltrope-audit-genesis-v1:"
  + sessionID)` — a **session-derived** genesis (versioned domain-separation
  prefix) so two sessions never share a chain.
- **`chain_hash = ChainHash(prev, content_hash)`** =
  `sha256.Sum256(prev || content_hash)`, folded in per-session contiguous **seq**
  order. The first chained event of a session seeds `prev` from
  `GenesisChainHash(sessionID)`; thereafter `prev` is the prior event's
  `chain_hash`. The fold copies `prev` into a fresh buffer before appending
  `content_hash` (it does **not** use the naive `append(prev, content...)`, which
  would mutate the reused running-head slice and silently corrupt every
  subsequent link).

The chain is **per-session, not global** — it aligns with seq contiguity, RLS
tenant isolation, and the session being the natural audit unit.

### Storage (migration 0009, additive, forward-only)

`migrations/0009_event_hash_chain.up.sql` adds four **nullable** columns with no
`NOT NULL`, no `DEFAULT`, and no backfill:

- `events.payload_canonical BYTEA` — the EXACT, verbatim `json.Marshal` bytes
  `content_hash` is taken over, stored RAW (a BYTEA, **not** JSONB, so Postgres
  performs no normalization). This is the **authoritative** payload for verify
  and read-back; the pre-existing `events.payload` JSONB is retained as a
  queryable, NON-authoritative convenience copy.
- `events.content_hash BYTEA`
- `events.chain_hash BYTEA`
- `sessions.chain_head BYTEA` — the running per-session chain head (the last
  event's `chain_hash`; `NULL` = no chained events yet → append seeds from the
  genesis hash).

There is **no `.down.sql`**. Pre-0009 rows stay `payload_canonical = NULL` /
`content_hash = NULL` / `chain_hash = NULL` (**UNCHAINED**) forever — deliberate
and forward-only.
`ADD COLUMN` without a `DEFAULT` is a metadata-only catalog change (no table
rewrite), so it applies cleanly and instantly on top of 0008. **RLS is
unaffected**: the existing events/sessions policies from migrations 0003 still
gate every row, and `boltrope_app` already holds `UPDATE` on both tables, so the
new per-event INSERT columns and the append-time `UPDATE sessions SET chain_head`
need no new GRANT.

### Append-time fold (no hot-path change)

In the event store's `appendTx`, the hashes are folded **inside the existing
append transaction**, with every hot-path invariant byte-for-byte unchanged:

1. The **idempotency short-circuit** (a re-sent `(session_id, request_id)` →
   `loadByRequestID` returns the prior envelopes) runs FIRST and performs **no
   chain advance and no `chain_head` update** — a replay never double-advances the
   chain or `head_seq`.
2. The **optimistic-concurrency gate** UPDATE
   (`head_seq = $expected AND lease_epoch = $epoch AND status = 'active'`,
   `RETURNING head_seq`) is the SAME predicate, still classified by
   `classifyAppendFailure` on 0 rows. Lease-epoch fencing and
   `ConflictError`/`FencedError`/`SessionNotActiveError` classification are
   unchanged.
3. Only AFTER the gate matches: `prev` is seeded from `sessions.chain_head` (read
   in the same tx; `NULL → GenesisChainHash(sessionID)`). For each event in seq
   order, `content_hash = ContentHash(payload)` over the SAME `payload` bytes the
   INSERT builds via `MarshalEventPayload` — and those exact bytes are written
   VERBATIM into the RAW `events.payload_canonical` column (so verify hashes the
   identical raw stored bytes) — and `chain_hash = ChainHash(prev, content_hash)`;
   `prev` advances. `payload_canonical`, `content_hash`, and `chain_hash` are
   added to the per-event INSERT and the hashes are returned on the envelope.
4. A single `UPDATE sessions SET chain_head = $last_chain_hash WHERE id =
   $sessionID` runs in the SAME tx (RLS-scoped, no head_seq/lease re-gate — the
   gate already matched). `pg_notify` and `Commit` are unchanged.

`domain.EventEnvelope` gains additive fields `PayloadCanonical []byte` (the raw
stored bytes verify hashes), `ContentHash []byte`, and `ChainHash []byte` (all
nil for unchained/pre-0009 rows), populated on the append return path and on every
read-back (`scanEnvelopes` scans them as nullable `[]byte` and decodes the typed
payload from `payload_canonical` when present, falling back to the legacy `payload`
JSONB for pre-0009 rows). The `boltrope-dev` in-memory store computes the SAME
hashes on Append via the same domain helpers (tracking a per-session running head)
and carries the canonical bytes on its envelopes, so its loaded envelopes and its
verify behave identically to prod.

### Verify (read-only, recompute-and-compare)

`VerifyChainIntegrity(ctx, sessionID, fromSeq, toSeq) (domain.ChainVerification,
error)` is a read-only, RLS-scoped, side-effect-free store method (on both the
pgx store and the dev store; deliberately OUTSIDE the frozen `app.EventLogPort`,
like `LoadRange`/`LoadUpTo`). It re-reads the session's events in
`[fromSeq, toSeq]` seq order (`fromSeq<=0` → from 1; `toSeq<=0` or past head →
head), recomputes `content_hash` by hashing the RAW stored `payload_canonical`
bytes DIRECTLY (no decode/re-marshal) and `chain_hash` from the running chain
(seeded at the link entering the window — the prior event's stored `chain_hash`,
or `GenesisChainHash` at the chain genesis), and compares recomputed vs stored. A
chained row whose `payload_canonical` is unexpectedly NULL is itself anomalous and
is reported as a content mismatch.

`domain.ChainVerification = {Valid bool; FirstBadSeq int64; Reason string;
Checked int}`:

- A clean range → `Valid=true, FirstBadSeq=0, Checked=`(number of CHAINED events
  verified).
- The first mismatch → `Valid=false, FirstBadSeq=`the bad seq, with a `Reason`
  classifying a **content-hash mismatch** (a tampered payload) versus a **broken
  link** (a rewritten `chain_hash`, payload intact).
- A contiguous **leading prefix of pre-0009 NULL-hash rows** is **skipped**, not
  reported as tampered; verification begins at the first chained event and
  `Checked` counts only chained events.

### Read-plane exposure (additive proto, all three facades)

Mirroring `ListSessionEvents` (ADR-0025):

- Two additive fields on `EventDescriptor`: `bytes content_hash = 12;` and
  `bytes chain_hash = 13;` (next free numbers after `summary = 11`). These are
  NON-sensitive integrity digests (not payload), so `toGenEventDescriptor`
  exposes them regardless of `include_payload` and does NOT set the redacted
  flag.
- A new additive RPC `VerifySessionIntegrity(VerifySessionIntegrityRequest)
  returns (VerifySessionIntegrityResponse)` with `tenant_id` as a guard (like
  `ListSessionEventsRequest`), wired on **all three facades** with the SAME
  ownership path as `ListSessionEvents` (foreign tenant → `PermissionDenied`,
  RLS-invisible/missing → `NotFound`):
  - **gRPC** `Server.VerifySessionIntegrity` (`authorizeTenant` +
    `authorizeSession` → `VerifyChainIntegrity` → map to the response;
    store/load error → `Internal`).
  - **REST** `GET /v1/sessions/{id}/integrity` (under `withAuth`; `from_seq` /
    `to_seq` query params via `parseOptionalInt64`, typed-400 on parse error;
    protojson response) — mirrors the `listSessionEvents` handler shape.
  - **MCP** a 12th tool `verify_session_integrity` (required `session_id`,
    optional `from_seq`/`to_seq`) returning `protoToMap` of the response.

`gen/` is regenerated with `buf generate` and committed in sync; `buf breaking`
(additive only) and `buf lint` pass.

## Consequences

- **Tamper-EVIDENT, not tamper-PROOF — the load-bearing caveat.** A per-session
  hash-chain anchored *inside the same database* makes any later mutation of a
  stored event **detectable by recomputation**: an UPDATE to a payload breaks its
  `content_hash`; an UPDATE to a `chain_hash` breaks the link. But it does **not**
  stop an attacker with full write access from forging a **self-consistent
  rewrite** — recompute every downstream `content_hash`/`chain_hash` and the
  `sessions.chain_head`, and a from-scratch verify passes. Closing that gap
  requires anchoring the chain head **outside** the database the attacker
  controls: **signed checkpoints + a SIEM/WORM export** (Batch-5B) are the
  follow-on that makes the log tamper-PROOF, not merely tamper-evident. This ADR
  is honest that Batch-5A delivers the detection substrate and the verify surface,
  and that the external anchor is still pending.
- **Verify hashes the RAW stored bytes (the previously-undetected tamper class is
  closed).** `content_hash` is taken over the EXACT `json.Marshal` bytes the
  append path produced, and those bytes are persisted **verbatim** in a dedicated
  RAW `events.payload_canonical BYTEA` column (a BYTEA, **not** JSONB, so Postgres
  performs **no** normalization — no whitespace insertion, key reordering, or
  duplicate-key dedup). `VerifyChainIntegrity` recomputes `content_hash` by
  hashing those raw stored bytes **directly** — it does **not** decode and
  re-marshal. This closes a concrete **false-NEGATIVE** that the earlier
  JSONB-only / decode-then-re-marshal design had: because `events.payload` is
  JSONB, an attacker could rewrite the stored payload by (a) reordering keys, (b)
  adding whitespace, or (c) injecting an EXTRA key the v1 struct drops on decode
  (e.g. `{"TurnID":"t1","Model":"gpt","EVIL":"injected"}`); the decode→re-marshal
  would reproduce the identical Go bytes and the row would re-hash to its stored
  `content_hash`, leaving real durable-row tampering UNDETECTED. Hashing the raw
  `payload_canonical` bytes makes every one of those mutations change the hashed
  bytes, so they are now **detected at the tampered seq** (integration-tested:
  `TestVerify_DetectsStructuralJSONBTamper` covers reorder/whitespace/extra-key).
  This also makes verify **schema-version-agnostic**: a newer `schema_version`'s
  extra keys are simply part of the hashed canonical bytes, so they neither
  falsely pass nor are **falsely** reported as tampered (the inverse false-positive
  the older design risked is also gone). The legacy `events.payload` JSONB is
  retained only as a **queryable, NON-authoritative** convenience copy; the
  authoritative payload for both verify and read-back is `payload_canonical`
  (read-back decodes from it, falling back to the JSONB only for pre-0009 rows
  whose `payload_canonical` is NULL). Note this is the "raw-stored-bytes verify
  path" originally scoped as a Batch-5B hardening; it was pulled into this batch
  because it is required to close a demonstrated tamper hole on the CORE
  deliverable. The remaining caveat is the **tamper-EVIDENT vs tamper-PROOF** one
  above (a self-consistent full rewrite by a write-capable attacker is still only
  closed by the external signed-checkpoint anchor in Batch-5B).
- **Hot path is unchanged.** The optimistic gate, `request_id` idempotency
  (replay returns prior with no re-chain, no double-increment), lease fencing, RLS
  tenant scoping, and the late unique-violation reclassification at Commit are all
  byte-for-byte preserved. The fold and the single `chain_head` UPDATE run inside
  the existing transaction; the only added work is N SHA-256s and one UPDATE — no
  new round-trip, no new lock. An integration test proves a re-sent append does
  not advance `head_seq` or `chain_head`.
- **Dev/prod parity by construction.** Because both paths go through the same
  `domain` helpers and the same fold, an identical typed event stream yields
  byte-identical `content_hash`/`chain_hash` on the pgx store and the dev
  in-memory store — asserted by a parity test.
- **Backward-compatible.** Pre-0009 NULL-hash rows do not break Load / LoadRange /
  LoadUpTo (the fields scan to nil) or verify (the NULL prefix is skipped). New
  sessions are chained from their first event.
- **Additive proto only.** Two new `EventDescriptor` fields + one new RPC; no
  rename/renumber; `gen/` in sync; `buf breaking`/`buf lint` green. The integrity
  digests are exposed on every facade alongside the existing event-read surface.
- **Trade-off.** Storing `payload_canonical` duplicates the payload bytes (the
  RAW copy alongside the JSONB) in exchange for a robust, schema-version-agnostic
  verify that hashes exactly what is stored — the right trade for an audit log
  whose entire purpose is tamper-evidence. The forward-only migration trades the
  inability to un-add the columns for a clean, rewrite-free, instantly-applied
  DDL. The remaining hardening (external signed-checkpoint anchor + SIEM/WORM
  export, which upgrades tamper-EVIDENT to tamper-PROOF) is scoped into Batch-5B.
