-- 0010_audit_checkpoints.up.sql
-- ============================================================================
-- SIGNED CHECKPOINT CHAIN: the storage half of turning the tamper-EVIDENT
-- in-DB hash-chain (0009) into a tamper-PROOF audit log (ADR-0034, Batch-5B).
--
-- 0009 made tampering DETECTABLE *within* the DB (per-event content_hash + a
-- per-session chain_hash). But an attacker holding DB write can recompute every
-- content/chain hash and the in-DB chain re-verifies clean. This table stores
-- an Ed25519-SIGNED checkpoint chain over the events' content_hashes; the
-- signing key lives OUTSIDE the events DB (resolved from the operator
-- environment via secret.SecretsPort), so a full in-DB history rewrite cannot
-- forge the signatures — VerifyAuditCheckpoints recomputes each checkpoint_hash
-- from the (possibly tampered) content_hashes and the stored signature then
-- fails to verify. That signature anchoring is what 0009 alone could not give.
--
--   id                   — BIGINT identity PK, ascending == checkpoint order.
--   prev_checkpoint_hash — the prior row's checkpoint_hash; NULL for the first
--                          (genesis) checkpoint (verify seeds prev from the
--                          fixed domain-separated genesis constant). Chains the
--                          checkpoints so a rewritten/removed checkpoint breaks
--                          the prev-link.
--   checkpoint_hash      — SHA-256( prev_checkpoint_hash || leavesDigest ) where
--                          leavesDigest = SHA-256( L1 || .. || Ln ) over the
--                          covered events' raw 32-byte content_hash leaves in
--                          ascending global_id order (ADR-0034). This is the
--                          value that is SIGNED.
--   covers_from_global_id /
--   covers_to_global_id  — inclusive [from, to] events.global_id range the
--                          checkpoint anchors. covers_to_global_id is the
--                          IDEMPOTENCY key (unique index below) so re-running
--                          the signer over an already-covered range is a no-op
--                          (INSERT ... ON CONFLICT (covers_to_global_id) DO
--                          NOTHING).
--   leaf_count           — number of content_hash leaves folded (== to-from+1
--                          minus any skipped pre-0009 NULL-hash rows).
--   signature            — Ed25519 signature over checkpoint_hash.
--   key_id               — opaque id of the signing key (BOLTROPE_AUDIT_SIGNING_
--                          KEY_ID); lets verify pick the right public key. The
--                          PRIVATE key is NEVER stored here (only the signature
--                          + a public-derivable key_id).
--   algo                 — signature algorithm; 'ed25519' today.
--   signed_at            — wall-clock of the signer write (NOT a billing/legal
--                          authority; the cryptographic anchor is the signature).
--
-- FORWARD-ONLY (no .down.sql; the repo has zero down migrations by convention —
-- the CheckForwardOnly guard enforces this) and PURELY ADDITIVE (a brand-new
-- table; no change to events/sessions, NO backfill). Applies cleanly on top of
-- 0009.
--
-- OPERATOR-TIER, RLS-EXEMPT (modeled on event_subscriptions / migrations/0002,
-- NOT on the tenant-scoped cost_rollup / 0007): the checkpoint chain spans ALL
-- tenants and is read/written ONLY by operator-tier consumers (the audit-signer
-- projection Runner and VerifyAuditCheckpoints, which run on the operator/owner
-- connection over the GLOBAL event feed). It is therefore NOT tenant-scoped and
-- carries NO ROW LEVEL SECURITY — deliberately NO ENABLE/FORCE ROW LEVEL
-- SECURITY and NO tenant policies. (Trust boundary, ADR-0034: the signer + SIEM
-- exporter are operator-tier infrastructure like OTLP/metrics export, governed
-- by ADR-0013 only over MODEL-INFLUENCED channels.)
--
-- APPEND-ONLY: boltrope_app is granted SELECT + INSERT only — never UPDATE or
-- DELETE — so the signer can append checkpoints but neither it nor a compromised
-- app role can silently rewrite or delete a signed checkpoint in place. (An
-- owner/superuser still can, but that is outside the app-role threat model and
-- is exactly what the off-DB signing key defends against.)
-- ============================================================================

CREATE TABLE audit_checkpoints (
    id                   BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    prev_checkpoint_hash BYTEA,                                  -- NULL for the genesis checkpoint
    checkpoint_hash      BYTEA       NOT NULL,                   -- SHA-256(prev || leavesDigest); the SIGNED value
    covers_from_global_id BIGINT     NOT NULL,                   -- inclusive first events.global_id leaf
    covers_to_global_id  BIGINT      NOT NULL,                   -- inclusive last events.global_id leaf (idempotency key)
    leaf_count           INT         NOT NULL,                   -- number of content_hash leaves folded
    signature            BYTEA       NOT NULL,                   -- Ed25519 signature over checkpoint_hash
    key_id               TEXT        NOT NULL,                   -- signing key id (public-derivable; NOT the private key)
    algo                 TEXT        NOT NULL DEFAULT 'ed25519', -- signature algorithm
    signed_at            TIMESTAMPTZ NOT NULL DEFAULT now()      -- signer write time (not a legal/billing authority)
);

-- IDEMPOTENCY KEY: an already-covered range cannot double-insert. The signer's
-- INSERT uses ON CONFLICT (covers_to_global_id) DO NOTHING so a re-run over the
-- same seeded events (crash re-read from the saved cursor) is a no-op.
CREATE UNIQUE INDEX uq_audit_checkpoints_covers_to ON audit_checkpoints (covers_to_global_id);

-- Operator-tier append-only grant: SELECT + INSERT only (no UPDATE/DELETE). The
-- table is RLS-EXEMPT (no ENABLE/FORCE ROW LEVEL SECURITY, no tenant policies) —
-- it is a global operator artifact like event_subscriptions, not tenant data.
GRANT SELECT, INSERT ON audit_checkpoints TO boltrope_app;
