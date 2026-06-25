<!-- SPDX-License-Identifier: Apache-2.0 -->

# ADR-0030: Long-term cross-session memory exposed only as tools, over an RLS-protected key/value store (no vector/RAG)

- **Status:** Accepted
- **Date:** 2026-06-25
- **Relates to:** [ADR-0011](0011-event-store-schema.md) (event-store schema, concrete RLS, tenant-scoped isolation), [ADR-0013](0013-security-model.md) (security model: RLS, tenant isolation, tool execution guardrails), [ADR-0015](0015-repository-layout.md) (repository layout, clean-package layering, depguard enforcement), [ADR-0024](0024-boltrope-dev-local-mode.md) / [ADR-0029](0029-boltrope-dev-real-model-and-local-exec-opt-in.md) (`boltrope-dev` separate binary + build-time pgx import fence)

## Context

The capability scorecard flagged a gap: the agent had **no way to remember
anything across sessions**. Every run started cold; a fact, preference, or
decision learned in one session was gone the next. Competitors expose some form
of agent memory, and the lack of it is a real capability hole — not a cosmetic
one.

Two design pressures shaped the response:

1. **How should memory reach the model?** The agent already has a clean,
   well-guarded path for giving the model new capabilities: **native tools**
   (`internal/toolruntime/tools`, registered into the registry, executed through
   `execute.Service`, schema-validated, side-effect- and egress-classified). The
   alternative — new gRPC/REST/MCP RPCs — would mean a proto change, three facade
   changes, and a parallel surface the model can't actually call on its own. The
   *consumer* of memory is the model, not an external client, so the tool surface
   is the natural fit.

2. **How much machinery?** The obvious "sophisticated" answer is vector
   embeddings + similarity search + a RAG pipeline. Product research deliberately
   ruled this out as **me-too complexity**: it adds an embedding-model dependency,
   an index to build/maintain/version, recall-quality tuning, and a whole new
   failure surface — for a v1 memory feature whose job is "remember this fact so
   you can recall it later," not "semantic search over a corpus." A simple
   key/value store with tag and substring retrieval covers the actual use case
   with a fraction of the surface area and zero new external dependencies.

A third, non-negotiable pressure is **tenant isolation**. Memory is durable,
shared-across-sessions tenant state. Tenant A must *never* be able to read or
modify tenant B's memory. The harness already has the right mechanism for this —
Postgres Row-Level Security keyed on `current_setting('app.current_tenant')`,
fail-closed when the GUC is unset (ADR-0011/0013, migrations 0003/0007). Memory
must inherit exactly that guarantee.

One wrinkle: the `Tool.Execute` interface
(`internal/toolruntime/domain/tool.go`) receives `sessionID` but **not**
`tenantID`. A memory tool that writes to an RLS-scoped store needs the tenant.
`execute.Service.Request` carries `TenantID` (it already scopes dedup and blobs),
but it was **not** being propagated into the `ctx` the tool sees. The orchestrator
has a clean tenant-context helper (`internal/orchestrator/infra/db/tenant.go`
`WithTenant`/`TenantFromContext`), but importing it from the tool-runtime would be
a **layering violation** (tool-runtime must not depend on the orchestrator).

## Decision

### 1. Memory is exposed ONLY as tools — no proto, no facade, no RPC

Three native tools, built from an `app.MemoryStore` port and registered into the
registry exactly like the existing `fs`/`shell`/`web` tools:

- **`memory_write`** — `SideEffect=Mutating`, `EgressClass=None`. Args
  `{namespace?, key, value, tags?}`. Upserts a durable key/value memory.
- **`memory_read`** — `SideEffect=ReadOnly`, `EgressClass=None`. Args
  `{namespace?, key}`. A miss is a **normal, non-error** observation
  ("no memory found for key X"), mirroring `websearch`'s "No results." — not an
  `IsError` observation.
- **`memory_search`** — `SideEffect=ReadOnly`, `EgressClass=None`. Args
  `{query?, tags?, limit?}`. Case-insensitive **substring** match over the value,
  AND-filtered by **all** supplied tags; `limit` is defaulted and hard-capped.
  An all-empty search lists recent entries up to the cap.

All three are `EgressClassNone`, so the execute egress gate is never invoked for
them, and none implements `EgressTargeter`. Recoverable failures (missing
required field, store error) surface as an **error `Observation` with a nil Go
error**, matching the `webfetch`/`fs` pattern — never a panic.

There is **no proto change, no `gen/` regeneration, and no REST/MCP/gRPC facade
change** in this batch. Memory's consumer is the model; the tool surface is the
whole surface.

### 2. Simple RLS-protected key/value + tag/substring retrieval — explicitly NO vector/embeddings/RAG

Migration **0008** adds `agent_memory`:

```sql
CREATE TABLE agent_memory (
  tenant_id  UUID NOT NULL REFERENCES tenants(id),
  namespace  TEXT NOT NULL DEFAULT 'default',
  mem_key    TEXT NOT NULL,
  value      TEXT NOT NULL,
  tags       TEXT[] NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, namespace, mem_key)
);
```

Retrieval is a substring `ILIKE` over `value` plus a `tags @> $tags`
array-contains (AND-semantics) filter — a GIN index on `tags` and a btree index
on `(tenant_id, namespace)` back the two scan shapes. **No embedding column, no
vector extension, no similarity operator, no RAG pipeline.** This is a deliberate
scope decision: the feature's job is durable recall of facts/preferences/
decisions by key or tag, and a key/value + substring store does that with no new
external dependency and a tiny failure surface. Semantic/vector recall is left
out — not half-built — and can be a future ADR if real demand appears.

### 3. Tenant isolation via RLS — fail-closed, a documented DELETE superset over 0003/0007

`agent_memory` mirrors the RLS template of migrations 0003/0007 **exactly**:
`ENABLE` + `FORCE ROW LEVEL SECURITY`, a `boltrope_app` grant, and per-operation
tenant-isolation policies keyed on
`tenant_id = current_setting('app.current_tenant')::uuid` with **no `missing_ok`
flag** — so an unset tenant GUC *raises*, i.e. **fails closed**, rather than
leaking rows.

One **deliberate, documented difference** from 0003/0007: those grant only
`SELECT, INSERT, UPDATE`. `agent_memory` additionally grants **`DELETE`** and
adds a matching `DELETE … USING` policy, because the `MemoryStore` port exposes
`Delete`. This is a documented **superset**, not a copy error.

The PG `MemoryStore` impl mirrors the dedup/eventstore `beginTenantTx` pattern:
each method opens a tx, runs the parameterized
`SELECT set_config('app.current_tenant', $1, true)`, executes the query so RLS
scopes it automatically, and commits — fail-closed on an empty tenant.

### 4. Tenant-via-ctx seam — a NEW clean tool-runtime helper, NOT the orchestrator's

The `Tool.Execute` signature does not carry the tenant, so we propagate it
through `ctx`. We add a **new, clean, stdlib-only** helper package at
`internal/toolruntime/infra/tenant` exposing `WithTenant`,
`TenantFromContext`, and `ErrNoTenant`, mirroring the *semantics* of
`internal/orchestrator/infra/db/tenant.go` (distinct unexported context-key type;
empty/absent ⇒ `ErrNoTenant`).

We **do not** import the orchestrator helper: tool-runtime depending on the
orchestrator package would be a **layering violation** (ADR-0015). The duplication
is a handful of trivial lines and buys a clean dependency graph; `go list -deps`
of the new package shows **only the standard library**.

`execute.Service.Execute` then sets, **before** the
`tool.Execute(execCtx, req.SessionID, req.Args)` call,
`execCtx = tenant.WithTenant(execCtx, req.TenantID)`. The wrap is on `execCtx`
(the timeout-derived ctx), so the tool's downstream store call carries **both**
the tenant value and the per-call deadline. The change is **purely additive**: no
existing return value or error path changes, and every existing tool-runtime test
stays green.

The `MemoryStore` impls (both PG and in-memory) read the tenant via this helper
and **fail closed** when it is absent.

### 5. Dev in-memory vs prod Postgres split — driven by the `boltrope-dev` pgx import fence

`boltrope-dev` is a separate binary that, by build-time invariant (ADR-0024/0029),
**must not import pgx**. So the two `MemoryStore` impls live in **separate Go
packages**:

- **Prod:** `internal/toolruntime/adapter/outbound/memory` — pgx-backed, RLS,
  tenant-scoped tx; wired into `cmd/boltrope-toolruntimed` with a pool built from
  the same `cfg.Postgres.DSN` as dedup.
- **Dev:** `internal/toolruntime/adapter/outbound/memory/inmem` — a clean,
  pgx-free, tenant-keyed map guarded by a `sync.RWMutex`, importing only the
  `app` port + the tenant helper + stdlib. Wired into `cmd/boltrope-dev`'s
  local-exec path.

Importing the `inmem` leaf does **not** pull the parent package's pgx (Go imports
are per-package, not per-directory), so the dev binary's import fence
(`TestDevBinary_DoesNotImportProductionEdges`) stays green **without
modification**. The in-memory store enforces the **same** tenant isolation via the
same ctx helper, so dev behaves like prod minus durability.

## Consequences

**Good.**
- The agent gains durable, cross-session memory — closing capability-scorecard
  gap #2 — through the model's own tool surface, with no proto/facade churn.
- Tenant isolation is inherited from the proven RLS mechanism (FORCE RLS,
  fail-closed on unset GUC), proven by an integration test against real Postgres
  *and* a unit test of the in-mem store; tenant A can never read or modify tenant
  B's memory.
- Zero new external dependency: no embedding model, no vector extension, no RAG
  pipeline to build, version, or tune. The failure surface is tiny.
- The dev binary keeps its pgx-free build-time fence: the in-mem and pgx stores
  are separate leaf packages.

**Bad / accepted trade-offs (honestly documented).**
- Retrieval is substring + tag-AND only — **no semantic/fuzzy recall**. A query
  that doesn't share a substring or a tag with a stored value won't find it. This
  is the deliberate no-RAG scope; richer recall is a future ADR, not a regression.
- The tenant-context helper is **duplicated** (a clean copy of the orchestrator's
  semantics) to avoid a layering violation. The duplication is intentional.
- `agent_memory` grants `DELETE` (a superset over 0003/0007); the store exposes
  `Delete`, but this batch ships **no `memory_delete` tool** — Delete is a
  store-only capability for now, keeping the model surface to three tools.

**Follow-up.**
- A `memory_delete` (or `memory_forget`) tool if/when the model needs to prune its
  own memory.
- A future ADR for semantic/vector recall **only** if real demand materializes —
  explicitly out of scope here.
