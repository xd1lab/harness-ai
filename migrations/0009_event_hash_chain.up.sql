-- 0009_event_hash_chain.up.sql
-- ============================================================================
-- Tamper-EVIDENT audit log: a per-event content hash + a per-session SHA-256
-- hash-CHAIN, computed at append-time in the single-writer transaction
-- (ADR-0033). This migration is the storage half: it only ADDS the nullable
-- columns the append path folds into and the verify path recomputes against.
--
--   events.payload_canonical BYTEA — the EXACT, verbatim json.Marshal bytes the
--                                append path takes content_hash over, stored
--                                RAW (a BYTEA, NOT JSONB, so Postgres performs
--                                NO normalization: no whitespace insertion, key
--                                reordering, or duplicate-key dedup). This is
--                                the AUTHORITATIVE payload for both verify and
--                                read-back. The pre-existing events.payload JSONB
--                                is retained as a queryable, NON-authoritative
--                                convenience copy. Hashing the raw stored bytes
--                                (no decode/re-marshal) makes structural/additive
--                                JSONB tampering — key reorder, whitespace, an
--                                injected extra key dropped on decode — change
--                                the hashed bytes and therefore DETECTABLE
--                                (closes the false-negative the JSONB-only design
--                                had; ADR-0033, Batch-5B hardening pulled in).
--   events.content_hash BYTEA  — SHA-256 over events.payload_canonical (the
--                                exact stored bytes), recomputed by verify from
--                                the identical raw bytes.
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
-- payload_canonical = NULL / content_hash = NULL / chain_hash = NULL (UNCHAINED)
-- forever — this is deliberate. The read path scans the columns as nullable
-- []byte (leaving the envelope fields nil), decodes from payload_canonical when
-- present (falling back to the legacy payload JSONB for pre-0009 rows whose
-- payload_canonical is NULL), and VerifyChainIntegrity skips a contiguous
-- leading NULL-hash prefix, beginning chain verification at the first chained
-- event.
--
-- RLS IS UNAFFECTED: the existing events / sessions SELECT/INSERT/UPDATE
-- policies from migrations/0003 still gate every row, and boltrope_app already
-- holds UPDATE on both tables (so the append-time `UPDATE sessions SET
-- chain_head = ...` and the per-event INSERT carrying the two new columns need
-- NO new GRANT). ADD COLUMN without a DEFAULT is a metadata-only catalog change
-- (no table rewrite), so this applies cleanly and instantly on top of 0008.
-- ============================================================================

ALTER TABLE events
    ADD COLUMN payload_canonical BYTEA,
    ADD COLUMN content_hash      BYTEA,
    ADD COLUMN chain_hash        BYTEA;

ALTER TABLE sessions
    ADD COLUMN chain_head BYTEA;
