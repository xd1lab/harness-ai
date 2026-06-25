-- 0009_event_hash_chain.up.sql
-- ============================================================================
-- Tamper-EVIDENT audit log: a per-event content hash + a per-session SHA-256
-- hash-CHAIN, computed at append-time in the single-writer transaction
-- (ADR-0033). This migration is the storage half: it only ADDS the nullable
-- columns the append path folds into and the verify path recomputes against.
--
--   events.content_hash BYTEA  — SHA-256 over the EXACT events.payload bytes
--                                stored for the row (json.Marshal output).
--   events.chain_hash   BYTEA  — SHA-256(prev_chain_hash || content_hash),
--                                ordered by the per-session contiguous seq;
--                                the first chained event in a session seeds
--                                prev from a session-derived genesis.
--   sessions.chain_head BYTEA  — the running per-session chain head (the last
--                                event's chain_hash). NULL = no chained events
--                                yet (-> append seeds from the genesis hash).
--
-- FORWARD-ONLY (no .down.sql; the repo has zero down migrations by
-- convention — the CheckForwardOnly guard enforces this). PURELY ADDITIVE and
-- NULLABLE: no NOT NULL, no DEFAULT, NO backfill. Pre-0009 rows therefore stay
-- content_hash = NULL / chain_hash = NULL (UNCHAINED) forever — this is
-- deliberate. The read path scans the columns as nullable []byte (leaving the
-- envelope fields nil) and VerifyChainIntegrity skips a contiguous leading
-- NULL-hash prefix, beginning chain verification at the first chained event.
--
-- RLS IS UNAFFECTED: the existing events / sessions SELECT/INSERT/UPDATE
-- policies from migrations/0003 still gate every row, and boltrope_app already
-- holds UPDATE on both tables (so the append-time `UPDATE sessions SET
-- chain_head = ...` and the per-event INSERT carrying the two new columns need
-- NO new GRANT). ADD COLUMN without a DEFAULT is a metadata-only catalog change
-- (no table rewrite), so this applies cleanly and instantly on top of 0008.
-- ============================================================================

ALTER TABLE events
    ADD COLUMN content_hash BYTEA,
    ADD COLUMN chain_hash   BYTEA;

ALTER TABLE sessions
    ADD COLUMN chain_head BYTEA;
