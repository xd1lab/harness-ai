-- 0004_session_mode.up.sql
-- ============================================================================
-- Session-scoped permission mode (ADR-0019; resolves the ADR-0018 §4 deferral).
--
-- FORWARD-ONLY, additive. `ADD COLUMN ... DEFAULT` is a metadata-only change in
-- PostgreSQL 11+ (no table rewrite) and is safe for a rolling/expand deploy
-- (ADR-0011 §"Migration policy"). There is intentionally no down-migration: the
-- forward-only convention for the sessions/events log tables is CI-enforced
-- (migrations.CheckForwardOnly / TestNoDestructiveDownOnLogTables).
--
-- `mode` is the session's standing permission operating mode for the
-- deny->mode->allow->ask pipeline (policy.Mode). It is set once at CreateSession
-- from the VERIFIED, non-bypass request and is immutable for the session's life;
-- a fork inherits its parent's mode. Existing rows and any INSERT that omits the
-- column default to 'default' — the most-restrictive mode (ask for every
-- risk-tiered tool). 'bypass' is operator-only/server-side and is rejected on a
-- client-supplied CreateSession (architecture §8.13; ADR-0013 "Constrained
-- bypass"); the column permits the value for a future operator-set path, but no
-- client request can store it.
-- ============================================================================

ALTER TABLE sessions
    ADD COLUMN mode TEXT NOT NULL DEFAULT 'default';

-- The stored values are domain.PermissionMode (event.go): note 'acceptEdits' is
-- camelCase here (the persisted/domain spelling), which deliberately differs from
-- policy.Mode's 'accept_edits'; the orchestrator edge maps between the two
-- explicitly (ADR-0019).
ALTER TABLE sessions
    ADD CONSTRAINT sessions_mode_chk CHECK (mode IN ('default', 'acceptEdits', 'plan', 'bypass'));
