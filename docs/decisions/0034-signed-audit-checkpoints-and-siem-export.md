# 0034. Signed audit checkpoints + SIEM export: tamper-PROOF, not merely tamper-EVIDENT

Date: 2026-06-26
Status: Accepted

## Context

[ADR-0033](0033-tamper-evident-hash-chain.md) (Batch-5A) made the event log
tamper-**EVIDENT**: a per-event `content_hash` and a per-session `chain_hash`,
folded at append time, let a verifier recompute the chain and **detect** any
later mutation of a stored event. ADR-0033 was deliberately honest about its
ceiling: the chain is anchored *inside the same database*, so an attacker holding
DB **write** access can forge a **self-consistent rewrite** — change a payload,
recompute that event's `content_hash`, then recompute every downstream
`chain_hash` and the `sessions.chain_head` — and a from-scratch verify passes.
Detection alone cannot close that gap.

Closing it needs two things the in-DB chain cannot provide:

1. An **anchor outside the database the attacker controls**. If the integrity
   value is signed by a key that does **not** live in the events DB, a rewritten
   history yields a different recomputed value whose stored signature no longer
   verifies — tamper is **PROVEN**, not merely evident.
2. **Evidence that survives a full-DB compromise**. Even a perfect signed anchor
   is only a detector; an independent, append-only copy of the audit descriptors
   shipped to an external sink (a SIEM / WORM store) means the evidence itself is
   not solely in the attacker's blast radius.

This batch (Batch-5B) delivers both, entirely **off the hot write path**. The
append transaction from ADR-0011/-0014/-0033 is **not modified**: the signer and
the SIEM exporter are read-only, operator-tier **projection consumers** that tail
the global event feed, reusing the existing projection `Runner` /
`Source` / `Cursor` machinery (the same pattern as the cost rollup in
[ADR-0026](0026-session-tenant-cost-read.md)).

Forces and constraints:

- **Hot path untouched.** The signer/exporter never participate in an append.
  They read the global `events` feed from a durable, xmin-bounded,
  gap-safe cursor — exactly like the cost projector — so an append never blocks on
  them and they never re-chain anything.
- **The signing key must live outside the events DB.** It is resolved from the
  operator environment via the existing `secret.SecretsPort`, held only as a
  redacting `secret.Secret`, and **never** logged, exported, or persisted. Only
  the signature and a public-derivable `key_id` reach the database.
- **Operator-tier, not the egress broker.** [ADR-0013](0013-security-model.md)'s
  egress broker governs **MODEL-INFLUENCED** channels only. The signer and the
  SIEM exporter are **operator-tier infrastructure** — like OTLP/metrics export —
  and use a plain `net/http` client for any outbound, **not** the egress broker.
  This trust boundary is stated explicitly below.
- **The checkpoint chain is GLOBAL, not tenant-scoped.** One signed chain spans
  all tenants, so it is read/written only by operator-tier consumers over the
  global feed and is **RLS-exempt** (modeled on `event_subscriptions`, not on the
  tenant-scoped `session_cost_events`).
- **Additive + forward-only.** Migration 0010 adds a brand-new table; the repo
  has zero down migrations (the `CheckForwardOnly` guard enforces it). The default
  is **no proto change** — the operator-tier verify runs against the operator
  connection, which the tenant-scoped gRPC edge cannot reach anyway.

## Decision

We add (1) a **signed checkpoint chain** over the events' content-hashes, anchored
by an Ed25519 key held outside the events DB; (2) a **`VerifyAuditCheckpoints`**
operation that recomputes and signature-verifies the chain (the tamper-PROOF
check), exposed via `harnessctl audit verify-checkpoints`; and (3) a **SIEM
export** consumer that ships descriptors-and-hashes-only audit frames to an
independent external sink. All three are off the hot path.

### The checkpoint_hash formula (single pgx-free source of truth)

The formula lives in `internal/orchestrator/domain/checkpoint.go` (stdlib
`crypto/sha256` only — the same dependency-light posture as `hashchain.go`), so
the signer and the verifier fold through the **identical** helpers. Pinned:

```
leavesDigest    = SHA-256( L1 || L2 || ... || Ln )
checkpoint_hash = SHA-256( prev_checkpoint_hash || leavesDigest )
```

- `L1..Ln` are the covered events' **raw 32-byte `content_hash` leaves**,
  concatenated in **ascending `global_id` order with NO separators**. The
  no-separator framing is unambiguous **only** because every leaf is a uniform 32
  bytes; the signer and verifier therefore **skip** nil / pre-0009 (unchained)
  `content_hash` rows so a short or nil leaf can never shift a boundary — matching
  the chain-verify's leading-NULL skip.
- The genesis `prev_checkpoint_hash` for the FIRST checkpoint is a fixed,
  versioned, domain-separated constant:
  `CheckpointGenesisPrev() = SHA-256("boltrope-audit-checkpoint-genesis-v1:")`.
  It is **distinct** from the per-session chain genesis prefix so a checkpoint
  genesis can never be confused with a session-chain genesis.
- Both `LeavesDigest` and `CheckpointHash` use the same **non-aliasing,
  copy-first** discipline as `domain.ChainHash` — `prev` is copied into a fresh
  buffer before appending, so a signer carrying one running `prev` forward across
  checkpoints cannot have an earlier head's backing array clobbered by a later
  fold.

A unit test pins the formula against a hand-computed vector and asserts it is
deterministic and order-sensitive (reordering the leaves changes
`checkpoint_hash`).

### Storage (migration 0010, additive, forward-only, RLS-exempt, append-only)

`migrations/0010_audit_checkpoints.up.sql` adds one new table:

```sql
CREATE TABLE audit_checkpoints (
    id                   BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    prev_checkpoint_hash BYTEA,                                  -- NULL for the genesis checkpoint
    checkpoint_hash      BYTEA       NOT NULL,                   -- the SIGNED value
    covers_from_global_id BIGINT     NOT NULL,
    covers_to_global_id  BIGINT      NOT NULL,                   -- idempotency key
    leaf_count           INT         NOT NULL,
    signature            BYTEA       NOT NULL,                   -- Ed25519 over checkpoint_hash
    key_id               TEXT        NOT NULL,                   -- public-derivable; NOT the private key
    algo                 TEXT        NOT NULL DEFAULT 'ed25519',
    signed_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_audit_checkpoints_covers_to ON audit_checkpoints (covers_to_global_id);
GRANT SELECT, INSERT ON audit_checkpoints TO boltrope_app;
```

- **Idempotency key:** the unique index on `covers_to_global_id` makes the
  signer's `INSERT ... ON CONFLICT (covers_to_global_id) DO NOTHING` a no-op when
  a range is re-covered (a crash re-read from the saved cursor).
- **Append-only:** `boltrope_app` is granted `SELECT` + `INSERT` only — never
  `UPDATE`/`DELETE` — so neither the signer nor a compromised app role can rewrite
  or delete a signed checkpoint in place. (An owner/superuser still can, but that
  is outside the app-role threat model and is exactly what the off-DB signing key
  defends against.)
- **RLS-exempt:** the table is GLOBAL operator-tier infrastructure (like
  `event_subscriptions`), so it carries **no** `ENABLE/FORCE ROW LEVEL SECURITY`
  and no tenant policies. It is read/written only by operator-tier consumers.
- **Forward-only:** no `.down.sql`; purely additive (a new table, no change to
  `events`/`sessions`, no backfill). A header comment documents the same
  forward-only / operator-tier / RLS-exempt rationale as 0007/0009.

### The signer (Ed25519, key resolved outside the DB)

`internal/platform/auditsign` is the greenfield signer/verifier (stdlib
`crypto/ed25519`):

- **Key resolution** via `secret.SecretsPort`:
  - `BOLTROPE_AUDIT_SIGNING_KEY` — base64 of **either** a 32-byte Ed25519 seed
    **or** a 64-byte `ed25519.PrivateKey` (validated by length after base64
    decode; PEM/PKCS8 deliberately **not** accepted — fewest footguns). The public
    key is derived from it.
  - `BOLTROPE_AUDIT_SIGNING_KEY_ID` — **REQUIRED** (non-empty) when a key is
    present; construction fails loudly otherwise (a signature with no `key_id`
    cannot be attributed/verified).
  - `BOLTROPE_AUDIT_PUBLIC_KEY` — base64 32-byte public key, for **verify-only**
    deployments that hold no private key.
  - Bare env names (no secret prefix), so a deployment that runs the signer can
    verify with the same environment.
- **Key secrecy:** the private key is held only as a `secret.Secret`, revealed
  **solely** at the `ed25519.Sign` call site, and is never logged, exported, or
  persisted. The signer logs only `key_id` + `algo`.
- **DISABLED-with-loud-warning is the safe default.** When
  `BOLTROPE_AUDIT_SIGNING_KEY` resolves to `secret.ErrNotFound`, `NewSigner`
  returns the `ErrSigningDisabled` sentinel, the signer projector is **not
  attached**, and `projectord` logs a single loud WARN at startup:
  *"audit checkpoint signing DISABLED: no BOLTROPE_AUDIT_SIGNING_KEY configured;
  the in-DB hash-chain is tamper-EVIDENT but not externally tamper-PROOF"*.
  **No ephemeral key is generated** — an ephemeral key would silently fail to
  anchor checkpoints durably, which is worse than honest disablement.

### The audit-checkpoint signer projector (own subscription)

`projection.AuditSigner` (attached via `WithAuditSigner`) tails the **global**
feed on its **own** subscription (default `audit-checkpoint`), so its cursor is
independent of cost-rollup and siem-export. It accumulates each event's
`content_hash` leaf in `(transaction_id, global_id)` order and, at each
checkpoint boundary, folds the range into `checkpoint_hash` (the formula above),
**signs** it, and inserts the `audit_checkpoints` row with
`covers_from_global_id` / `covers_to_global_id` / `leaf_count` and the running
`prev_checkpoint_hash`.

- **Boundary policy:** emit a checkpoint every `N` accumulated leaves (`N` from
  `BOLTROPE_AUDIT_CHECKPOINT_EVERY`, default 256) **and** flush a partial
  checkpoint on a short/caught-up batch so the head is anchored promptly.
- **Crash safety (the load-bearing subtlety).** The projection `Runner` saves its
  cursor **per batch**, but a checkpoint can span many batches — so a crash after
  a cursor save but before a flush would advance the cursor PAST leaves no
  checkpoint ever covered, and the runner alone would never re-deliver them. The
  signer therefore reloads, on (re)start, the checkpoint-chain head from
  `audit_checkpoints` (latest `checkpoint_hash` as the running `prev`,
  `MAX(covers_to_global_id)` as `lastCovered`) **and re-reads the unanchored tail
  directly from `events`** above that frontier (bounded by the same settled-only
  xmin predicate the feed fetch uses). That re-read is what anchors a stranded
  tail independently of where the cursor sits.
- **Idempotent / at-least-once:** leaves at or below `lastCovered` are skipped;
  the `INSERT ... ON CONFLICT (covers_to_global_id) DO NOTHING` makes a
  re-anchored range a no-op. Re-running the signer over already-covered events
  leaves the row count and the chain unchanged (integration-tested).
- Like the cost projector it **errors before the cursor advances** and never
  touches the append path. Rows with nil `content_hash` (pre-0009) are skipped as
  leaves.

### VerifyAuditCheckpoints (the tamper-PROOF check)

`eventstore.Store.VerifyAuditCheckpoints(ctx, verifier)` is the operator-tier
read that turns the signed chain into a **proof**. It is deliberately **NOT**
RLS-scoped: the checkpoint chain is global, so it runs on a **separate
operator/owner connection** (`beginOperatorTx`, distinct from the tenant-scoped
`beginTenantTx` that the rest of the `Store` and `VerifyChainIntegrity` use — the
tenant path is left exactly as-is so tenant isolation and the hot append path are
untouched). For each checkpoint row in `id` order it:

1. **(a)** re-reads the events' `content_hash` leaves over
   `[covers_from_global_id, covers_to_global_id]` (ascending `global_id`,
   NULL-hash rows skipped), recomputes `leavesDigest` + `checkpoint_hash` from the
   configured `prev` (the genesis constant for the first row, the prior row's
   **stored** `checkpoint_hash` otherwise) and asserts it equals the stored
   `checkpoint_hash`;
2. **(b)** asserts `prev_checkpoint_hash` links to the prior row (the checkpoint
   chain itself is intact — a rewritten/removed checkpoint breaks the prev-link);
3. **(c)** verifies the Ed25519 signature of the **recomputed** `checkpoint_hash`
   against the public key for the row's `key_id`.

It returns the pure `domain.CheckpointVerification {Valid, FirstBadCheckpointID,
Reason, Checked}`, classifying the first failure as a **leaf/hash mismatch**
(events tampered), a **broken prev-link** (the checkpoint chain rewritten), or an
**invalid signature** (forgery / wrong key — the load-bearing tamper-PROOF
signal). An empty `audit_checkpoints` table yields `Valid=true, Checked=0`
(nothing anchored yet is not a tamper).

The crucial property — proven by integration test against real Postgres — is that
this catches what the in-DB chain alone cannot: seed events, sign checkpoints,
then via the owner connection perform a **full, self-consistent in-DB rewrite** of
one covered event (recomputing its `content_hash`/`chain_hash` and the downstream
`chain_head` so `VerifyChainIntegrity` passes again), and `VerifyAuditCheckpoints`
still returns `Valid=false`: the recomputed `checkpoint_hash` differs, so the
signature made over the **original** hash no longer verifies.

### harnessctl `audit verify-checkpoints`

A new operator-tier `audit` subcommand group with one action,
`verify-checkpoints`, wired into the existing dispatch without disturbing the
session/run/approve/deny/interrupt/fork commands. Because every orchestrator gRPC
RPC is tenant-scoped (RLS) and the checkpoint chain is global, the CLI does **not**
use the gRPC client (per AC-12 default: **no proto change**). It connects
**directly** to the operator/owner Postgres DSN (`BOLTROPE_AUDIT_DATABASE_URL`,
falling back to `BOLTROPE_POSTGRES__DSN` — it must be an operator/owner role that
bypasses events' RLS), builds the `auditsign.Verifier` from the configured PUBLIC
key (`BOLTROPE_AUDIT_PUBLIC_KEY`, or derived from a configured signing key), runs
`VerifyAuditCheckpoints`, prints `VALID`/`INVALID` (with `Checked`, and on failure
`FirstBadCheckpointID` + `Reason`), and **exits non-zero on INVALID**. `gen/`
stays byte-identical — no RPC, no message, no field added.

### SIEM export (descriptors + hashes only, never raw payload)

`projection.SIEMExporter` (attached via `WithSIEMExporter`) tails the global feed
on its **own** subscription (default `siem-export`) and, per event, emits an audit
**FRAME** as JSON:

```json
{ "tenant_id", "session_id", "seq", "global_id", "event_type",
  "actor", "created_at", "content_hash" (hex), "chain_hash" (hex) }
```

The frame carries **DESCRIPTORS + HASHES ONLY**. The `siemFrame` struct has **no**
`payload` / `payload_canonical` field, so the raw event payload — which may carry
secrets — can **never** be serialized into a SIEM frame (unit-tested: a sentinel
secret placed in the source payload is absent from both the file-sink output and
the HTTP-sink body). `global_id` is in every frame so the SIEM dedups under the
at-least-once cursor.

Sinks, each active **only** when its env var is set (so the exporter can be
constructed unconditionally and is inert when nothing is configured):

- **FILE / NDJSON** — `BOLTROPE_SIEM_FILE`: one JSON object per line, appended
  (`O_APPEND`, so concurrent restarts never truncate prior evidence).
- **HTTP** — `BOLTROPE_SIEM_HTTP_URL`: POSTs an NDJSON batch with an optional
  `Authorization: Bearer <BOLTROPE_SIEM_HTTP_BEARER>`. The bearer is resolved via
  `secret.SecretsPort` and held as a `secret.Secret` so it redacts in logs.

It uses a **plain `net/http.Client`** with a sane timeout (operator-tier — **not**
the egress broker), errors **before** the cursor advances, and **must not block**
the cost projector.

> **Frame `actor` / `created_at` note.** The frame schema includes `actor` and
> `created_at` keys. The projection `Source.fetchBatchSQL` currently selects
> through `content_hash`/`chain_hash` only and does **not** yet project the
> `events.actor` / `events.created_at` columns, so today those two keys serialize
> empty/blank. The `EventRow.Actor` / `EventRow.CreatedAt` fields exist and the
> exporter reads them, so populating them is a one-line additive `SELECT`
> extension when an operator needs richer SIEM frames; the integrity-bearing
> fields (the hashes + identifiers) are fully populated today.

### Projectord wiring (env-gated, independent cursors, non-blocking)

`cmd/boltrope-projectord/wiring.go` fans out into **independent** projection
loops, **one per subscription**, each with its own `pgx.Conn` (a single
`pgx.Conn` is not concurrency-safe, and one `Runner` owns one
subscription/cursor):

- `cost-rollup` — **always** (cost projector + lag gauge + orphan-blob sweeper);
  unchanged when the new consumers are disabled.
- `audit-checkpoint` — **only** when `BOLTROPE_AUDIT_SIGNING_KEY` is set (else the
  loud WARN); the Ed25519 signer.
- `siem-export` — **only** when `BOLTROPE_SIEM_FILE` or `BOLTROPE_SIEM_HTTP_URL`
  is set; the SIEM exporter.

Each loop is independently resilient (its own reconnect/backoff), so a failing
SIEM sink or a signer DB error degrades **only its own subscription** and can
never stall cost-rollup's cursor.

## The operator-tier trust boundary (explicit)

This is the load-bearing security statement: **the signer and the SIEM exporter
are OPERATOR-TIER infrastructure, NOT model-influenced channels, and therefore do
NOT route through the egress broker.**

[ADR-0013](0013-security-model.md)'s egress broker exists to govern the channels a
**model** (or model-influenced tool) can reach — the taint gate, the deny-by-default
fetcher, the SSRF-safe hardened egress. Its threat model is *"the model tries to
exfiltrate or reach somewhere it shouldn't"*. The signer and the SIEM exporter sit
on the **other side** of that boundary: they are operator-configured, run in
`projectord` (which executes **no** model loop), carry only operator-supplied
configuration (the SIEM URL/bearer, the signing key), and emit only
operator-defined audit descriptors. They are the same tier as OTLP/metrics export
— infrastructure the operator stands up to observe and protect the system, not a
surface the model can influence.

Concretely:

- The SIEM HTTP sink and any verify outbound use a **plain `net/http.Client`** with
  a bounded timeout — never the `toolruntime` `EgressBroker`.
- The new `auditsign` package, `audit_signer.go`, and `siem_exporter.go` import
  **no** egress-broker package (code-level assertion / grep guard).
- Routing operator audit export through the model's egress broker would be a
  category error: it would subject operator infrastructure to a model-confinement
  policy it has no business being governed by, and would couple audit durability
  to the model channel's availability.

This boundary is what lets the SIEM exporter ship evidence to a sink the model can
never see or reach, and lets the signer hold a key the model never touches.

## Consequences

- **Tamper-PROOF, where 5A was only tamper-EVIDENT.** The signature anchors
  `checkpoint_hash` to a key **outside** the events DB. A write-capable attacker
  who forges a self-consistent in-DB rewrite (the exact attack ADR-0033 could not
  stop) changes a `content_hash` leaf → changes `leavesDigest` → changes the
  recomputed `checkpoint_hash` → the stored signature over the **original** hash
  fails to verify. `VerifyAuditCheckpoints` returns `Valid=false`. This is the
  load-bearing upgrade, proven by an integration test that simulates the full
  rewrite and asserts both the in-DB chain re-verifies clean **and** the signed
  checkpoints catch it.
- **Evidence survives a full-DB compromise.** The SIEM export ships an
  append-only, payload-free copy of the audit descriptors + hashes to an
  independent sink, so the evidence is not solely in the attacker's blast radius.
- **Hot path is byte-for-byte unchanged.** The signer and exporter are read-only
  operator-tier projection consumers; the append transaction, `insertEvent` /
  `marshalPayload`, the `sessions.chain_head` update, and migration 0009 are not
  modified. The existing eventstore append + hashchain integration tests pass
  unchanged.
- **Disabled-with-warning is the honest default.** With no signing key configured,
  checkpoints are off and `projectord` says so loudly; the log stays
  tamper-EVIDENT (5A) but is not externally tamper-PROOF until a key is provided.
  No ephemeral key is generated.
- **No proto change.** The operator-tier verify runs against the operator
  connection via `harnessctl`, which the tenant-scoped gRPC edge cannot reach
  anyway; `gen/` is byte-identical. An additive operator RPC remains a clean
  future option if an over-the-wire operator surface is ever wanted.
- **Trade-offs.** The signer carries its own crash-recovery re-read (it cannot
  rely on the per-batch cursor alone, since a checkpoint spans batches) — extra
  complexity for the guarantee that no settled leaf is ever left unanchored.
  Storing the signed checkpoints duplicates a small per-range digest; the SIEM
  export duplicates audit descriptors to an external store. Both are the right
  trade for an audit log whose entire purpose is defensible, externally-anchored
  tamper resistance. The frame `actor`/`created_at` enrichment is a deferred,
  additive `Source` `SELECT` extension (see the note above).
