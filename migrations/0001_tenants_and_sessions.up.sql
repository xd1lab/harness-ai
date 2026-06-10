-- 0001_tenants_and_sessions.up.sql
-- ============================================================================
-- Boltrope event store — tenancy & session aggregate roots (ADR-0011 §6.2).
--
-- FORWARD-ONLY. There is intentionally no 0001_..down.sql for the log tables
-- (sessions/events): destructive down-migrations on the event log are a
-- CI-blocked anti-pattern (ADR-0011 §"Migration policy"; architecture §6.1).
-- The forward-only convention is verified by the migrations package test
-- (TestNoDestructiveDownOnLogTables) and documented in the package doc.
--
-- Minimum PostgreSQL version is 13 (xid8 / pg_current_xact_id), enforced at
-- config validation and re-checked by the migrate runner before applying.
-- ============================================================================

-- A tenant is the isolation boundary; every tenant-scoped row carries tenant_id
-- and RLS keys on it (ADR-0011 §6.7).
CREATE TABLE tenants (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A session is the event-sourcing stream/aggregate. head_seq is the optimistic
-- version; lease_* implement the fenced single-writer lease (ADR-0014; §9.6).
CREATE TABLE sessions (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL REFERENCES tenants(id),
    parent_id       UUID REFERENCES sessions(id),          -- set for forks
    forked_from_seq BIGINT,                                 -- frozen parent seq the fork branched at
    status          TEXT NOT NULL DEFAULT 'active',         -- active | finished | failed
    head_seq        BIGINT NOT NULL DEFAULT 0,              -- optimistic version (last seq)
    lease_owner     TEXT,                                   -- current writer identity (SPIFFE ID + instance)
    lease_epoch     BIGINT NOT NULL DEFAULT 0,              -- monotonic fencing token
    lease_expires_at TIMESTAMPTZ,                           -- lease TTL; heartbeat renews
    last_event_at   TIMESTAMPTZ,                            -- stuck-session detector input
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sessions_status_chk CHECK (status IN ('active', 'finished', 'failed')),
    CONSTRAINT fork_seq_nonneg CHECK (forked_from_seq IS NULL OR forked_from_seq >= 0)
);

CREATE INDEX idx_sessions_tenant ON sessions (tenant_id);
CREATE INDEX idx_sessions_status ON sessions (status);
