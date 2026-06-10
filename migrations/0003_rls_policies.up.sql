-- 0003_rls_policies.up.sql
-- ============================================================================
-- Concrete Row-Level Security (ADR-0011 §6.7; architecture §6.7, §8.3).
-- FORWARD-ONLY.
--
-- Mechanism:
--   * The application connects via a NON-OWNER role WITHOUT BYPASSRLS
--     (boltrope_app), created here idempotently. The migration runner connects
--     as the owner/superuser; the app pool connects as boltrope_app so RLS is
--     enforced on it (the owner is exempt unless FORCE is set — which it is).
--   * On every connection/transaction the app runs
--     SET LOCAL app.current_tenant = '<verified tenant uuid>' (the orchestrator
--     pgx acquire hook; ADR-0011 §6.7). tenant_id comes from the VERIFIED
--     principal token, never a client-supplied field.
--   * FORCE ROW LEVEL SECURITY makes the policies apply even to the table owner,
--     so a forgotten WHERE predicate cannot leak cross-tenant rows in tests OR
--     in production.
--   * Policies cover SELECT/INSERT/UPDATE (the append path inserts and updates),
--     keyed on current_setting('app.current_tenant')::uuid, so a missing policy
--     fails closed.
--
-- current_setting('app.current_tenant') WITHOUT the missing_ok flag raises an
-- error when the GUC is unset — that is deliberately fail-closed: a connection
-- that forgot to set the tenant cannot read or write any tenant-scoped row.
-- ============================================================================

-- Create the non-owner application role idempotently. NOLOGIN by default here;
-- the deployment provisions a password/login grant out-of-band. It explicitly
-- has NOBYPASSRLS so it can never see across tenants.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'boltrope_app') THEN
        CREATE ROLE boltrope_app NOBYPASSRLS;
    END IF;
END
$$;

-- The app role needs table DML privileges (RLS narrows rows; GRANT gowerns the
-- verb). It does NOT own the tables, so FORCE RLS binds it.
GRANT USAGE ON SCHEMA public TO boltrope_app;
GRANT SELECT, INSERT, UPDATE ON tenants, sessions, events, session_snapshots, blobs, tool_executions TO boltrope_app;
-- events.global_id is GENERATED ALWAYS AS IDENTITY (no sequence grant needed);
-- no other table uses a serial sequence.

-- ----------------------------------------------------------------------------
-- Helper: enable + FORCE RLS and install tenant-scoped policies on one table.
-- Inlined per-table below (no plpgsql loop) so the DDL is explicit and greppable.
-- ----------------------------------------------------------------------------

-- tenants: scope by the row's own id (a tenant may only see/modify itself).
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenants_tenant_isolation_select ON tenants
    FOR SELECT USING (id = current_setting('app.current_tenant')::uuid);
CREATE POLICY tenants_tenant_isolation_insert ON tenants
    FOR INSERT WITH CHECK (id = current_setting('app.current_tenant')::uuid);
CREATE POLICY tenants_tenant_isolation_update ON tenants
    FOR UPDATE USING (id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (id = current_setting('app.current_tenant')::uuid);

-- sessions
ALTER TABLE sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions FORCE ROW LEVEL SECURITY;
CREATE POLICY sessions_tenant_isolation_select ON sessions
    FOR SELECT USING (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY sessions_tenant_isolation_insert ON sessions
    FOR INSERT WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY sessions_tenant_isolation_update ON sessions
    FOR UPDATE USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);

-- events
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
ALTER TABLE events FORCE ROW LEVEL SECURITY;
CREATE POLICY events_tenant_isolation_select ON events
    FOR SELECT USING (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY events_tenant_isolation_insert ON events
    FOR INSERT WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY events_tenant_isolation_update ON events
    FOR UPDATE USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);

-- session_snapshots
ALTER TABLE session_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_snapshots FORCE ROW LEVEL SECURITY;
CREATE POLICY snapshots_tenant_isolation_select ON session_snapshots
    FOR SELECT USING (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY snapshots_tenant_isolation_insert ON session_snapshots
    FOR INSERT WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY snapshots_tenant_isolation_update ON session_snapshots
    FOR UPDATE USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);

-- blobs
ALTER TABLE blobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE blobs FORCE ROW LEVEL SECURITY;
CREATE POLICY blobs_tenant_isolation_select ON blobs
    FOR SELECT USING (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY blobs_tenant_isolation_insert ON blobs
    FOR INSERT WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY blobs_tenant_isolation_update ON blobs
    FOR UPDATE USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);

-- tool_executions
ALTER TABLE tool_executions ENABLE ROW LEVEL SECURITY;
ALTER TABLE tool_executions FORCE ROW LEVEL SECURITY;
CREATE POLICY tool_exec_tenant_isolation_select ON tool_executions
    FOR SELECT USING (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY tool_exec_tenant_isolation_insert ON tool_executions
    FOR INSERT WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY tool_exec_tenant_isolation_update ON tool_executions
    FOR UPDATE USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
