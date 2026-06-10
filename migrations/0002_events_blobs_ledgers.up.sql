-- 0002_events_blobs_ledgers.up.sql
-- ============================================================================
-- The append-only event log plus the off-table blob store, snapshots, the
-- read-side subscription cursor, and the durable tool-execution dedup ledger
-- (ADR-0011 §6.2; ADR-0012 §"Durable dedup ledger"). FORWARD-ONLY.
--
-- blobs is created BEFORE events because events.blob_ref is an FK into blobs
-- (the FK makes a dangling reference impossible: an event cannot reference a
-- blobs row not committed in the same transaction; architecture §6.4, §7.4).
-- ============================================================================

-- Large-output blob references (object-store offload) — TENANT-SCOPED identity.
-- PRIMARY KEY (tenant_id, ref): cross-tenant content-addressed dedup is
-- FORBIDDEN (a global content key is a cross-tenant existence oracle; §8.5).
CREATE TABLE blobs (
    tenant_id   UUID NOT NULL REFERENCES tenants(id),
    ref         TEXT NOT NULL,              -- per-tenant content key (e.g. sha256 of bytes); NOT global
    media_type  TEXT NOT NULL,
    size_bytes  BIGINT NOT NULL,
    storage_uri TEXT NOT NULL,              -- tenant-prefixed path/uri; bytes live OUTSIDE Postgres
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, ref)            -- composite: cross-tenant dedup is forbidden
);

-- The append-only event log (single source of truth).
--   global_id      — global ordering id (monotonic identity).
--   transaction_id — gap-safe poll cursor for projections (xid8; PG >= 13).
--   seq            — per-session contiguous sequence (1..N), tied to the
--                    sessions.head_seq transition so contiguity holds by
--                    construction (ADR-0011 §6.3).
--   request_id     — per-append idempotency token; a re-sent append with the
--                    same (session_id, request_id) returns the prior row as
--                    success rather than a conflict (ADR-0011 §6.3, §7.3).
CREATE TABLE events (
    global_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id(),
    tenant_id      UUID NOT NULL REFERENCES tenants(id),
    session_id     UUID NOT NULL REFERENCES sessions(id),
    seq            BIGINT NOT NULL,
    request_id     UUID NOT NULL,
    event_type     TEXT NOT NULL,
    schema_version INT NOT NULL DEFAULT 1,
    payload        JSONB NOT NULL,
    provider_raw   JSONB,
    blob_ref       TEXT,
    token_usage    JSONB,
    cost_usd       NUMERIC(12, 6),
    actor          TEXT NOT NULL DEFAULT 'system',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (global_id),
    -- per-session ordering + optimistic concurrency:
    CONSTRAINT uq_events_session_seq UNIQUE (session_id, seq),
    -- true idempotency backstop: a re-sent append with the same request_id is a
    -- no-op (the Go Append short-circuits on a prior (session_id, request_id)
    -- match and returns the committed rows as success; ADR-0011 §6.3). The seq is
    -- INCLUDED so one logical append may write N events under a single request_id
    -- (one event per seq) while a true duplicate of any single committed
    -- (request_id, seq) row is still rejected. A single-event append (the common
    -- case) reduces to the (session_id, request_id) uniqueness the ADR sketches.
    CONSTRAINT uq_events_session_request UNIQUE (session_id, request_id, seq),
    CONSTRAINT events_seq_positive CHECK (seq >= 1),
    -- composite FK so a blob_ref always resolves WITHIN the same tenant
    -- (matches blobs' composite PK; closes a cross-tenant ref) and is committed
    -- in the same transaction (write-before-reference, architecture §6.4):
    CONSTRAINT fk_events_blob FOREIGN KEY (tenant_id, blob_ref)
        REFERENCES blobs (tenant_id, ref)
);

CREATE INDEX idx_events_session_seq ON events (session_id, seq);         -- replay/load
CREATE INDEX idx_events_txn_global  ON events (transaction_id, global_id); -- gap-safe projection cursor
CREATE INDEX idx_events_tenant      ON events (tenant_id, global_id);      -- tenant-scoped scans

-- Snapshots (replay-from-snapshot optimization).
CREATE TABLE session_snapshots (
    session_id    UUID NOT NULL REFERENCES sessions(id),
    seq           BIGINT NOT NULL,           -- snapshot reflects events up to & incl. seq
    tenant_id     UUID NOT NULL REFERENCES tenants(id),
    parent_prefix JSONB,                      -- for forks: inherited (parent_id, at_seq) lineage
    state         JSONB NOT NULL,             -- derived session state
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, seq)
);

-- Projection / subscription checkpoints (read-side offsets). The dual
-- (last_transaction_id, last_global_id) cursor is the xmin-bounded safe-advance
-- pattern consumed by projectord (architecture §6.6, §10.4). This table is NOT
-- tenant-scoped (it is an operator/read-side artifact) and is excluded from RLS.
CREATE TABLE event_subscriptions (
    name                TEXT PRIMARY KEY,        -- e.g. "cost-rollup", "otel-exporter"
    last_transaction_id xid8 NOT NULL DEFAULT '0'::xid8,
    last_global_id      BIGINT NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Tool-execution dedup ledger (durable; survives restart) — ADR-0012 §7.2.
CREATE TABLE tool_executions (
    tenant_id       UUID NOT NULL REFERENCES tenants(id),
    session_id      UUID NOT NULL REFERENCES sessions(id),
    idempotency_key TEXT NOT NULL,            -- = hash(session_id, seq_of_ToolCall)
    status          TEXT NOT NULL,            -- started | completed | failed | unknown
    result_ref      TEXT,                     -- pointer to the ToolResult event/blob when completed
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id, idempotency_key),
    CONSTRAINT tool_executions_status_chk CHECK (status IN ('started', 'completed', 'failed', 'unknown'))
);
