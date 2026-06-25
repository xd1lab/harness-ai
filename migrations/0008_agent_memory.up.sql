-- 0008_agent_memory.up.sql
-- ============================================================================
-- Long-term, cross-session agent memory: a tenant-scoped, RLS-protected
-- key/value store exposed to the model ONLY as tools (memory_write /
-- memory_read / memory_search). NO new proto/facade/RPC; NO vector/embeddings/
-- RAG (deliberately out of scope — simple KV + tag/substring recall; ADR-0030).
-- FORWARD-ONLY (no .down.sql; the repo has zero down migrations by convention).
--
-- Mechanism (mirrors migrations/0003 / 0007):
--   * The app pool connects as the NON-OWNER, NOBYPASSRLS role boltrope_app and
--     runs SELECT set_config('app.current_tenant', '<verified tenant uuid>',
--     true) at the start of each tenant-scoped tx, so RLS scopes every query.
--   * FORCE ROW LEVEL SECURITY applies the policies even to the table owner, so
--     a forgotten WHERE predicate cannot leak cross-tenant rows.
--   * current_setting('app.current_tenant') WITHOUT the missing_ok flag raises
--     when the GUC is unset — deliberately fail-closed: a tx that forgot to set
--     the tenant can neither read nor write any row.
--
-- DELIBERATE SUPERSET vs 0003/0007: those grant SELECT/INSERT/UPDATE only.
-- agent_memory ALSO grants DELETE and installs a matching DELETE policy because
-- the MemoryStore port exposes Delete(). This is an intentional, documented
-- superset, not a copy error.
-- ============================================================================

CREATE TABLE agent_memory (
    tenant_id   UUID        NOT NULL REFERENCES tenants(id),
    namespace   TEXT        NOT NULL DEFAULT 'default',
    mem_key     TEXT        NOT NULL,
    value       TEXT        NOT NULL,
    tags        TEXT[]      NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, namespace, mem_key)
);

-- Tag-membership search (memory_search tag AND-filter uses tags @> $x).
CREATE INDEX idx_agent_memory_tags ON agent_memory USING GIN (tags);
-- Namespace listing / substring scans within a tenant.
CREATE INDEX idx_agent_memory_ns ON agent_memory (tenant_id, namespace);

-- ----------------------------------------------------------------------------
-- RLS: enable + FORCE, with per-op tenant-isolation policies keyed on the
-- app.current_tenant GUC. DELETE policy added (superset of 0003/0007) for
-- MemoryStore.Delete. A missing GUC fails closed (current_setting raises).
-- ----------------------------------------------------------------------------

GRANT SELECT, INSERT, UPDATE, DELETE ON agent_memory TO boltrope_app;

ALTER TABLE agent_memory ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_memory FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_memory_tenant_isolation_select ON agent_memory
    FOR SELECT USING (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY agent_memory_tenant_isolation_insert ON agent_memory
    FOR INSERT WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY agent_memory_tenant_isolation_update ON agent_memory
    FOR UPDATE USING (tenant_id = current_setting('app.current_tenant')::uuid)
    WITH CHECK (tenant_id = current_setting('app.current_tenant')::uuid);
CREATE POLICY agent_memory_tenant_isolation_delete ON agent_memory
    FOR DELETE USING (tenant_id = current_setting('app.current_tenant')::uuid);
