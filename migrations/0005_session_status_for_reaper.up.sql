-- 0005_session_status_for_reaper.up.sql
-- ============================================================================
-- Session-status lookup for the tool-runtime sandbox reaper (architecture
-- §10.6). FORWARD-ONLY.
--
-- The reaper must reclaim sandboxes of finished/failed sessions immediately,
-- keyed off the authoritative sessions table — but sessions has FORCE ROW
-- LEVEL SECURITY scoped by the app.current_tenant GUC (0003), and the reaper
-- holds only a session id: it has no verified tenant principal to satisfy the
-- predicate, and the GUC read is deliberately fail-closed (raises when unset).
--
-- Approach: a SECURITY DEFINER function owned by the migration role exposes
-- EXACTLY one narrow fact — the status text of the session with the given
-- exact UUID — and nothing else. EXECUTE is granted to boltrope_app only.
-- Alternatives rejected:
--   * a USING (true) SELECT policy for boltrope_app would break tenant
--     isolation for EVERY service sharing the role;
--   * a dedicated BYPASSRLS reaper role would force a second provisioned
--     credential into every deployment for a status-only probe.
--
-- Privilege requirement: the migration role must bypass RLS (superuser — as
-- in the compose and testcontainer deployments — or BYPASSRLS). If it does
-- not, FORCE RLS binds the definer too: the function sees no rows and returns
-- NULL, which the tool-runtime adapter maps to SessionUnknown + error, so the
-- reaper RETAINS the sandbox and falls back to TTL reaping — a fail-safe
-- degradation, never a wrong reap.
-- ============================================================================

CREATE FUNCTION session_status_for_reaper(p_session_id UUID)
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
    SELECT status FROM sessions WHERE id = p_session_id
$$;

-- PUBLIC gets EXECUTE on new functions by default; this function bypasses RLS,
-- so lock it down to exactly the application role.
REVOKE ALL ON FUNCTION session_status_for_reaper(UUID) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION session_status_for_reaper(UUID) TO boltrope_app;
