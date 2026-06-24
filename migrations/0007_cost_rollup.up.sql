-- 0007_cost_rollup.up.sql
-- ============================================================================
-- Persistent, rebuildable per-turn cost rollup for the session/tenant cost-read
-- API (Feature O / cost-read; ADR-0026). FORWARD-ONLY.
--
-- Number coordination: Feature I already shipped 0006_sessions_list_index on
-- this branch (an index-only migration). Feature O's cost table therefore takes
-- 0007 (the documented merge-coordination outcome — R2 in IMPACT: "fold into O's
-- 0006 or renumber to 0007"). Shape is unchanged either way.
--
-- Design (DECISIONS §A/§D): per-EVENT-idempotent detail rows, NOT a blind
-- `cost = cost + delta`. The natural PK is the source event's global_id, so the
-- projectord write is
--     INSERT INTO session_cost_events (global_id, ...) ON CONFLICT (global_id) DO NOTHING
-- and re-processing the same event (crash re-read from the saved cursor) is an
-- identity no-op — never a double count, with or without a wrapping transaction.
-- per-session / per-tenant / per-model totals are all derived at read time via
-- SUM(...) GROUP BY model, so the projection is fully rebuildable (TRUNCATE then
-- re-fold from cursor 0 reproduces identical aggregates).
--
-- model is correlated at the WRITE side (projectord joins TurnStarted.Model to the
-- terminal TurnFinished/TurnAborted by TurnID); an uncorrelated event lands with
-- model='' (the read side renders it as the "unknown" bucket). per-tool cost is
-- DROPPED — the events carry no tool x cost dimension.
-- ============================================================================

CREATE TABLE session_cost_events (
    global_id           BIGINT      PRIMARY KEY,             -- = events.global_id (natural idempotency key)
    tenant_id           UUID        NOT NULL REFERENCES tenants(id),
    session_id          UUID        NOT NULL REFERENCES sessions(id),
    model               TEXT        NOT NULL DEFAULT '',     -- from TurnStarted.Model; '' = unknown
    event_type          TEXT        NOT NULL,                -- 'TurnFinished' | 'TurnAborted'
    cost_usd            NUMERIC(20, 10) NOT NULL DEFAULT 0,
    input_tokens        BIGINT      NOT NULL DEFAULT 0,
    output_tokens       BIGINT      NOT NULL DEFAULT 0,
    cache_read_tokens   BIGINT      NOT NULL DEFAULT 0,
    cache_write_tokens  BIGINT      NOT NULL DEFAULT 0,
    reasoning_tokens    BIGINT      NOT NULL DEFAULT 0,
    occurred_at         TIMESTAMPTZ NOT NULL DEFAULT now(),  -- projection write time (not the billing authority)
    CONSTRAINT scost_event_type_chk CHECK (event_type IN ('TurnFinished', 'TurnAborted'))
);

-- per-tenant aggregation scan (chargeback): tenant + model.
CREATE INDEX idx_scost_tenant_model ON session_cost_events (tenant_id, model);
-- per-session aggregation scan (with the model partition).
CREATE INDEX idx_scost_session ON session_cost_events (session_id, model);

-- ----------------------------------------------------------------------------
-- RLS: mirror migrations/0003's FORCE RLS + tenant-scoped SELECT/INSERT/UPDATE
-- policies. The read side (orchestrator gRPC server) uses boltrope_app +
-- SET LOCAL app.current_tenant, so reads are automatically tenant-scoped and a
-- cross-tenant read sees zero rows. The projectord write path SET LOCALs the
-- per-event tenant before each insert, so the WITH CHECK binds the write too
-- (enforcing under a NOBYPASSRLS writer; advisory under an owner/bypassing one).
-- A missing GUC fails closed (current_setting without missing_ok raises).
-- ----------------------------------------------------------------------------

GRANT SELECT, INSERT, UPDATE ON session_cost_events TO boltrope_app;

ALTER TABLE session_cost_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_cost_events FORCE ROW LEVEL SECURITY;
CREATE POLICY scost_tenant_isolation_select ON session_cost_events
    FOR SELECT USING (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY scost_tenant_isolation_insert ON session_cost_events
    FOR INSERT WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY scost_tenant_isolation_update ON session_cost_events
    FOR UPDATE USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
