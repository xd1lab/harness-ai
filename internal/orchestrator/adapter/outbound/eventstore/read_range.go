// SPDX-License-Identifier: Apache-2.0

package eventstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	infradb "github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
)

// This file is the read-only event-range surface backing the event-log read +
// time-travel API (Feature M / event-read): LoadRange (a keyset page of events)
// and LoadUpTo (the bounded fold window for at-seq reconstruction). Both are
// ADDITIVE methods on the adapter-side EventStore consumer-superset, NOT on the
// frozen app.EventLogPort, so the `var _ app.EventLogPort = (*Store)(nil)`
// assertion is unaffected.
//
// Like every other store read both go through beginTenantTx -> SET LOCAL
// app.current_tenant -> RLS, so a foreign tenant sees nothing (the SQL carries no
// tenant_id filter; RLS scopes the rows). They are read-only and side-effect-free:
// the load-bearing "time-travel uses Load-then-fold, NEVER Fork" guarantee — a
// read must never create a session row or append an event.

// selectEventsRangeSQL is the keyset page read: events with seq strictly greater
// than the cursor, oldest first, capped at a limit. It rides idx_events_session_seq
// (the (session_id, seq) index) and uses NO OFFSET so deep pages do not degrade.
const selectEventsRangeSQL = "SELECT " + eventColumns +
	" FROM events WHERE session_id = $1 AND seq > $2 ORDER BY seq LIMIT $3"

// selectEventsUpToSeqSQL is the bounded fold window: events with seq <= the
// inclusive upper bound, oldest first — the [1..at_seq] window the server folds to
// reconstruct the at-seq projection. No LIMIT (the whole window is folded).
const selectEventsUpToSeqSQL = "SELECT " + eventColumns +
	" FROM events WHERE session_id = $1 AND seq <= $2 ORDER BY seq"

// selectEventsBetweenSeqSQL is the inclusive verify window: events with
// fromSeq <= seq <= toSeq, oldest first — the bounded slice VerifyChainIntegrity
// re-reads to recompute and compare each stored content_hash/chain_hash.
const selectEventsBetweenSeqSQL = "SELECT " + eventColumns +
	" FROM events WHERE session_id = $1 AND seq >= $2 AND seq <= $3 ORDER BY seq"

// selectSessionHeadSeqSQL reads a session's current head_seq within the
// verify tx (RLS-scoped). A foreign-tenant or absent session returns no row, so
// the window resolves to an empty stream (Checked=0) without leaking existence.
const selectSessionHeadSeqSQL = "SELECT head_seq FROM sessions WHERE id = $1"

// selectChainHashAtSeqSQL reads the STORED chain_hash of a single seq within the
// verify tx — the one extra row read that seeds the running chain when the verify
// window does not start at the session's first chained event (open question #1:
// seed from seq=fromSeq-1's stored chain_hash rather than re-folding from seq 1).
const selectChainHashAtSeqSQL = "SELECT chain_hash FROM events WHERE session_id = $1 AND seq = $2"

// LoadRange returns sessionID's events with seq strictly greater than afterSeq,
// oldest first, capped at limit (a keyset page). It is the store half of the
// ListSessionEvents RPC. It is RLS-scoped via beginTenantTx, so a foreign tenant
// sees no rows; a context with no tenant fails closed (beginTenantTx errors). It
// is read-only and side-effect-free.
func (s *Store) LoadRange(ctx context.Context, sessionID string, afterSeq int64, limit int) ([]domain.EventEnvelope, error) {
	tenantID, err := infradb.TenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	rows, err := tx.Query(ctx, selectEventsRangeSQL, sessionID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("eventstore: load-range query: %w", err)
	}
	envs, err := scanEnvelopes(rows, tenantID)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("eventstore: commit load-range: %w", err)
	}
	return envs, nil
}

// LoadUpTo returns sessionID's events with seq <= atSeq, oldest first — the
// bounded window the server folds for at-seq state reconstruction. atSeq <= 0
// yields the empty window (no events); atSeq beyond head yields the whole stream.
// It is RLS-scoped via beginTenantTx (foreign tenant -> no rows; no tenant ->
// fail-closed) and is read-only and side-effect-free.
func (s *Store) LoadUpTo(ctx context.Context, sessionID string, atSeq int64) ([]domain.EventEnvelope, error) {
	tenantID, err := infradb.TenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	rows, err := tx.Query(ctx, selectEventsUpToSeqSQL, sessionID, atSeq)
	if err != nil {
		return nil, fmt.Errorf("eventstore: load-up-to query: %w", err)
	}
	envs, err := scanEnvelopes(rows, tenantID)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("eventstore: commit load-up-to: %w", err)
	}
	return envs, nil
}

// VerifyChainIntegrity re-reads sessionID's events in the [fromSeq,toSeq] seq
// window, recomputes the per-event content_hash and the per-session chain_hash,
// and compares them to the stored values — the tamper-evidence read (ADR-0033,
// AC-7/AC-8/AC-9). It is read-only and side-effect-free: it routes through
// beginTenantTx (SET LOCAL app.current_tenant -> RLS), creates no rows, and
// commits the read-only tx to end it cleanly. A foreign tenant sees no rows
// (RLS), so it cannot probe another tenant's integrity (returns Checked=0).
//
// Window resolution: fromSeq<=0 -> 1; toSeq<=0 or beyond head -> the session's
// current head_seq (read in-tx). An empty or RLS-hidden session resolves to an
// empty window and returns Valid=true, FirstBadSeq=0, Checked=0.
//
// Backward compat (AC-9): a contiguous LEADING prefix of pre-0009 NULL-content
// hash rows is skipped (not reported as tampered, not counted); verification
// begins at the first chained (non-NULL content_hash) event. Checked counts only
// the CHAINED events verified.
//
// Chain seeding (open question #1): the running prev for the first chained event
// in the window is the session genesis ([domain.GenesisChainHash]) when that
// event is the session's first chained row, else the STORED chain_hash of the
// prior seq (one extra row read) — so a window starting mid-chain does not
// re-fold from seq 1.
//
// Raw-bytes content verification (ADR-0033): verify recomputes content_hash by
// hashing the RAW stored events.payload_canonical bytes DIRECTLY — no
// decode/re-marshal. payload_canonical holds the verbatim json.Marshal bytes the
// append path took content_hash over, persisted as a BYTEA (Postgres does NOT
// normalize it). Hashing the literal stored bytes makes structural/additive
// tampering of the stored payload — key reorder, whitespace, an injected extra
// key dropped on decode — change the hashed bytes and therefore DETECTABLE.
// (This closes the false-negative the earlier decode-then-re-marshal design had,
// which round-tripped byte-identically even after such mutations.) It is
// schema-version-agnostic: a newer schema_version's extra keys are part of the
// hashed canonical bytes, so they neither falsely pass nor falsely fail.
//
// A chained row (content_hash != nil) is always written WITH its
// payload_canonical in the same INSERT, so a chained row whose payload_canonical
// is NULL is itself anomalous (a tampered/partial row) and is reported as a
// content mismatch.
func (s *Store) VerifyChainIntegrity(ctx context.Context, sessionID string, fromSeq, toSeq int64) (domain.ChainVerification, error) {
	tenantID, err := infradb.TenantFromContext(ctx)
	if err != nil {
		return domain.ChainVerification{}, err
	}
	tx, cleanup, err := s.beginTenantTx(ctx)
	if err != nil {
		return domain.ChainVerification{}, err
	}
	defer cleanup()

	// Resolve the upper bound to the session's head (RLS-scoped). A missing or
	// foreign-tenant session has no visible head: treat as an empty window.
	var head int64
	herr := tx.QueryRow(ctx, selectSessionHeadSeqSQL, sessionID).Scan(&head)
	if errors.Is(herr, pgx.ErrNoRows) {
		if cerr := tx.Commit(ctx); cerr != nil {
			return domain.ChainVerification{}, fmt.Errorf("eventstore: commit verify (no session): %w", cerr)
		}
		return domain.ChainVerification{Valid: true}, nil
	}
	if herr != nil {
		return domain.ChainVerification{}, fmt.Errorf("eventstore: verify reading head_seq: %w", herr)
	}

	if fromSeq <= 0 {
		fromSeq = 1
	}
	if toSeq <= 0 || toSeq > head {
		toSeq = head
	}
	if fromSeq > toSeq {
		// Empty window (e.g. head=0, or fromSeq beyond toSeq): nothing to verify.
		if cerr := tx.Commit(ctx); cerr != nil {
			return domain.ChainVerification{}, fmt.Errorf("eventstore: commit verify (empty window): %w", cerr)
		}
		return domain.ChainVerification{Valid: true}, nil
	}

	rows, err := tx.Query(ctx, selectEventsBetweenSeqSQL, sessionID, fromSeq, toSeq)
	if err != nil {
		return domain.ChainVerification{}, fmt.Errorf("eventstore: verify range query: %w", err)
	}
	envs, err := scanEnvelopes(rows, tenantID)
	rows.Close()
	if err != nil {
		return domain.ChainVerification{}, err
	}

	// Skip the contiguous leading NULL-content_hash prefix (pre-0009 rows): they
	// are unchained, not tampered, and are neither verified nor counted (AC-9).
	start := 0
	for start < len(envs) && envs[start].ContentHash == nil {
		start++
	}
	if start >= len(envs) {
		// No chained events in the window (all-NULL or empty): vacuously valid.
		if cerr := tx.Commit(ctx); cerr != nil {
			return domain.ChainVerification{}, fmt.Errorf("eventstore: commit verify (no chained rows): %w", cerr)
		}
		return domain.ChainVerification{Valid: true}, nil
	}

	// Seed the running chain head entering the first chained event. If that event
	// is the session's very first chained row (its seq is the first chained seq of
	// the session) seed from the session genesis; otherwise seed from the STORED
	// chain_hash of the prior seq (one extra row read).
	firstChained := envs[start]
	prev, err := s.verifySeedPrev(ctx, tx, sessionID, firstChained.Seq)
	if err != nil {
		return domain.ChainVerification{}, err
	}

	checked := 0
	for i := start; i < len(envs); i++ {
		e := envs[i]
		// Recompute content_hash by hashing the RAW stored payload_canonical bytes
		// directly (no decode/re-marshal) — so reorder/whitespace/extra-key tampering
		// of the stored payload changes the hash and is detected. A chained row with a
		// NULL payload_canonical is anomalous (the column is written with content_hash
		// in the same INSERT) and is treated as a content mismatch.
		if e.PayloadCanonical == nil {
			if cerr := tx.Commit(ctx); cerr != nil {
				return domain.ChainVerification{}, fmt.Errorf("eventstore: commit verify (missing canonical): %w", cerr)
			}
			return domain.ChainVerification{
				Valid:       false,
				FirstBadSeq: e.Seq,
				Reason:      fmt.Sprintf("content-hash mismatch at seq %d (missing canonical payload)", e.Seq),
				Checked:     checked,
			}, nil
		}
		recomputedContent := domain.ContentHash(e.PayloadCanonical)
		if !bytes.Equal(recomputedContent, e.ContentHash) {
			if cerr := tx.Commit(ctx); cerr != nil {
				return domain.ChainVerification{}, fmt.Errorf("eventstore: commit verify (content mismatch): %w", cerr)
			}
			return domain.ChainVerification{
				Valid:       false,
				FirstBadSeq: e.Seq,
				Reason:      fmt.Sprintf("content-hash mismatch at seq %d (payload tampered)", e.Seq),
				Checked:     checked,
			}, nil
		}
		recomputedChain := domain.ChainHash(prev, recomputedContent)
		if !bytes.Equal(recomputedChain, e.ChainHash) {
			if cerr := tx.Commit(ctx); cerr != nil {
				return domain.ChainVerification{}, fmt.Errorf("eventstore: commit verify (broken link): %w", cerr)
			}
			return domain.ChainVerification{
				Valid:       false,
				FirstBadSeq: e.Seq,
				Reason:      fmt.Sprintf("broken link: chain-hash mismatch at seq %d", e.Seq),
				Checked:     checked,
			}, nil
		}
		prev = e.ChainHash
		checked++
	}

	if cerr := tx.Commit(ctx); cerr != nil {
		return domain.ChainVerification{}, fmt.Errorf("eventstore: commit verify: %w", cerr)
	}
	return domain.ChainVerification{Valid: true, Checked: checked}, nil
}

// verifySeedPrev returns the running prev_chain_hash entering the chained event
// at firstChainedSeq within the verify tx. It reads the prior seq's STORED
// chain_hash; if no prior chained row exists (the prior seq is absent or is an
// unchained NULL-hash row), the event is the session's first chained row and the
// seed is the session genesis ([domain.GenesisChainHash]). This is the single
// extra row read that lets a mid-chain window verify without re-folding from
// seq 1 (open question #1).
func (s *Store) verifySeedPrev(ctx context.Context, tx pgx.Tx, sessionID string, firstChainedSeq int64) ([]byte, error) {
	if firstChainedSeq <= 1 {
		return domain.GenesisChainHash(sessionID), nil
	}
	var priorChain []byte
	err := tx.QueryRow(ctx, selectChainHashAtSeqSQL, sessionID, firstChainedSeq-1).Scan(&priorChain)
	if errors.Is(err, pgx.ErrNoRows) || priorChain == nil {
		// No prior chained row (gap, or prior row is a pre-0009 NULL-hash row):
		// this chained event is the session's first link -> seed from genesis.
		return domain.GenesisChainHash(sessionID), nil
	}
	if err != nil {
		return nil, fmt.Errorf("eventstore: verify seeding prev chain at seq=%d: %w", firstChainedSeq-1, err)
	}
	return priorChain, nil
}
