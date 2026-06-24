-- 0006_sessions_list_index.up.sql
-- ============================================================================
-- Composite indexes for the admin/tenant ListSessions read surface
-- (Feature I / ADR-0027). FORWARD-ONLY, ADDITIVE (CREATE INDEX only).
--
-- ListSessions pages the sessions table keyset-style on the composite
-- (created_at, id) total order, optionally filtered by status and a half-open
-- created_at window, always RLS-scoped to the caller's tenant (the SET LOCAL
-- app.current_tenant GUC + the 0003 sessions SELECT policy). The query is
-- therefore always tenant_id-prefixed; the two indexes below are tenant-prefixed
-- to match that predicate and carry the (created_at, id) keyset as their tail so
-- both the sort and the keyset comparison are index-ordered.
--
-- Gap this closes: 0001 created only idx_sessions_tenant (tenant_id) and
-- idx_sessions_status (status) — neither serves the (created_at, id) keyset sort
-- or the status+created_at composite, so without these a list would sort/scan.
--
--   * idx_sessions_tenant_created       serves the no-status-filter list (the
--     common case): tenant_id equality + (created_at, id) ordered range/keyset.
--   * idx_sessions_tenant_status_created serves the status-filtered list:
--     tenant_id + status equality + (created_at, id) ordered range/keyset.
--
-- No new RLS policy is needed: the 0003 sessions SELECT policy
-- (tenant_id = current_setting('app.current_tenant')::uuid) already governs the
-- read, and an index is not a row-visibility surface. No GRANT change: boltrope_app
-- already holds SELECT on sessions (0003). This migration only adds indexes; it
-- creates, alters, or drops no table, column, constraint, policy, or grant.
-- ============================================================================

CREATE INDEX idx_sessions_tenant_created
    ON sessions (tenant_id, created_at, id);

CREATE INDEX idx_sessions_tenant_status_created
    ON sessions (tenant_id, status, created_at, id);
