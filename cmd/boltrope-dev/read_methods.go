// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// This file adds the read-only event-range (Feature M / event-read) and cost-read
// (Feature O / cost-read) halves of the [igrpc.EventStore] consumer-superset to
// the dev binary's in-memory [Store], so the single-process dev server exposes the
// same admin/read RPCs as the production pgx store. They fold over the in-memory
// event map (single-tenant, no RLS in dev mode; K-2). They are read-only.

// LoadRange returns sessionID's events with seq strictly greater than afterSeq,
// oldest first, capped at limit (a keyset page) — the dev backing for
// ListSessionEvents.
func (s *Store) LoadRange(_ context.Context, sessionID string, afterSeq int64, limit int) ([]domain.EventEnvelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.EventEnvelope
	for _, e := range s.sessions[sessionID] {
		if e.Seq > afterSeq {
			out = append(out, e)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

// LoadUpTo returns sessionID's events with seq <= atSeq, oldest first — the dev
// backing for GetStateAtSeq's bounded fold window.
func (s *Store) LoadUpTo(_ context.Context, sessionID string, atSeq int64) ([]domain.EventEnvelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.EventEnvelope
	for _, e := range s.sessions[sessionID] {
		if e.Seq <= atSeq {
			out = append(out, e)
		}
	}
	return out, nil
}

// VerifyChainIntegrity re-reads sessionID's in-memory events in the
// [fromSeq,toSeq] seq window, recomputes the per-event content_hash and the
// per-session chain_hash via the SHARED, pgx-free domain helpers, and compares
// them to the stored values — the dev-store half of the tamper-evidence read
// (ADR-0033, AC-10). It mirrors the prod pgx store's VerifyChainIntegrity
// {Valid,FirstBadSeq,Reason,Checked} semantics, including the leading
// NULL-hash-prefix skip (a forked child starts at its first chained event), so a
// dev VerifySessionIntegrity RPC behaves identically to prod.
//
// It is read-only and side-effect-free. Window resolution mirrors prod:
// fromSeq<=0 -> 1; toSeq<=0 or beyond head -> head. An empty/unknown session
// yields Valid=true, FirstBadSeq=0, Checked=0. Dev mode is single-process and
// single-tenant (K-2: no RLS), so there is no tenant scoping to apply.
func (s *Store) VerifyChainIntegrity(_ context.Context, sessionID string, fromSeq, toSeq int64) (domain.ChainVerification, error) {
	s.mu.Lock()
	all := append([]domain.EventEnvelope(nil), s.sessions[sessionID]...)
	s.mu.Unlock()

	head := int64(0)
	if n := len(all); n > 0 {
		head = all[n-1].Seq
	}
	if fromSeq <= 0 {
		fromSeq = 1
	}
	if toSeq <= 0 || toSeq > head {
		toSeq = head
	}
	if fromSeq > toSeq {
		return domain.ChainVerification{Valid: true}, nil
	}

	// Slice the inclusive [fromSeq,toSeq] window in seq order. The dev store keeps
	// envelopes appended in seq order, so this is a contiguous filter.
	var window []domain.EventEnvelope
	for _, e := range all {
		if e.Seq >= fromSeq && e.Seq <= toSeq {
			window = append(window, e)
		}
	}

	// Skip the contiguous leading NULL-content_hash prefix (unchained rows): they
	// are not tampered, and are neither verified nor counted (AC-9 parity).
	start := 0
	for start < len(window) && window[start].ContentHash == nil {
		start++
	}
	if start >= len(window) {
		return domain.ChainVerification{Valid: true}, nil
	}

	// Seed the running chain head entering the first chained event in the window:
	// if it is the session's first chained row seed from genesis, else from the
	// STORED chain_hash of the prior seq (mirrors prod's verifySeedPrev).
	firstChained := window[start]
	prev := s.devVerifySeedPrev(all, sessionID, firstChained.Seq)

	checked := 0
	for i := start; i < len(window); i++ {
		e := window[i]
		payload, err := domain.MarshalEventPayload(e.Event)
		if err != nil {
			return domain.ChainVerification{}, fmt.Errorf("eventstore(dev): verify re-marshaling seq=%d payload: %w", e.Seq, err)
		}
		recomputedContent := domain.ContentHash(payload)
		if !bytes.Equal(recomputedContent, e.ContentHash) {
			return domain.ChainVerification{
				Valid:       false,
				FirstBadSeq: e.Seq,
				Reason:      fmt.Sprintf("content-hash mismatch at seq %d (payload tampered)", e.Seq),
				Checked:     checked,
			}, nil
		}
		recomputedChain := domain.ChainHash(prev, recomputedContent)
		if !bytes.Equal(recomputedChain, e.ChainHash) {
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
	return domain.ChainVerification{Valid: true, Checked: checked}, nil
}

// devVerifySeedPrev returns the running prev_chain_hash entering the chained event
// at firstChainedSeq. It reads the prior seq's STORED chain_hash from the in-memory
// stream; if no prior chained row exists (gap, or the prior row is an unchained
// NULL-hash row), the event is the session's first chained row and the seed is the
// session genesis. Mirrors the prod store's verifySeedPrev (open question #1).
func (s *Store) devVerifySeedPrev(all []domain.EventEnvelope, sessionID string, firstChainedSeq int64) []byte {
	if firstChainedSeq <= 1 {
		return domain.GenesisChainHash(sessionID)
	}
	for _, e := range all {
		if e.Seq == firstChainedSeq-1 {
			if e.ChainHash == nil {
				return domain.GenesisChainHash(sessionID)
			}
			return e.ChainHash
		}
	}
	return domain.GenesisChainHash(sessionID)
}

// SessionCostByModel folds the per-model cost rollup for one session by
// correlating TurnStarted.Model to the terminal TurnFinished/TurnAborted by
// TurnID — the dev backing for GetSessionCost (no persisted table in dev mode).
func (s *Store) SessionCostByModel(_ context.Context, sessionID string) ([]igrpc.ModelCostRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return devFoldModelCost(s.sessions[sessionID]), nil
}

// TenantCostByModel folds the per-model rollup across every session the dev store
// holds (single-tenant in dev mode) — the dev backing for GetTenantCost.
func (s *Store) TenantCostByModel(_ context.Context) ([]igrpc.ModelCostRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []domain.EventEnvelope
	for _, evs := range s.sessions {
		all = append(all, evs...)
	}
	return devFoldModelCost(all), nil
}

// TenantSessionCostCount counts the distinct sessions carrying a cost-bearing
// terminal event — the dev backing for GetTenantCostResponse.session_count.
func (s *Store) TenantSessionCostCount(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int64
	for _, evs := range s.sessions {
		for _, env := range evs {
			switch env.Event.(type) {
			case domain.TurnFinished, domain.TurnAborted:
				count++
			default:
				continue
			}
			break // this session has cost; count it once
		}
	}
	return count, nil
}

// devFoldModelCost correlates TurnStarted.Model to terminal events by (session,
// TurnID) and folds the per-model cost/usage/turns; an uncorrelated terminal event
// contributes to the "" (unknown) bucket. It mirrors the pgx store's
// SUM ... GROUP BY model result over the dev event map.
func devFoldModelCost(events []domain.EventEnvelope) []igrpc.ModelCostRow {
	type turnKey struct{ session, turn string }
	modelByTurn := map[turnKey]string{}
	for _, env := range events {
		if ts, ok := env.Event.(domain.TurnStarted); ok {
			modelByTurn[turnKey{env.SessionID, ts.TurnID}] = ts.Model
		}
	}
	byModel := map[string]*igrpc.ModelCostRow{}
	add := func(model string, cost float64, usage llm.Usage) {
		r := byModel[model]
		if r == nil {
			r = &igrpc.ModelCostRow{Model: model}
			byModel[model] = r
		}
		r.CostUSD += cost
		r.Usage = addDevUsage(r.Usage, usage)
		r.Turns++
	}
	for _, env := range events {
		switch ev := env.Event.(type) {
		case domain.TurnFinished:
			add(modelByTurn[turnKey{env.SessionID, ev.TurnID}], ev.CostUSD, ev.Usage)
		case domain.TurnAborted:
			add(modelByTurn[turnKey{env.SessionID, ev.TurnID}], ev.CostUSD, ev.UsageSoFar)
		}
	}
	out := make([]igrpc.ModelCostRow, 0, len(byModel))
	for _, r := range byModel {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}

// addDevUsage returns the field-wise sum of two usage snapshots.
func addDevUsage(a, b llm.Usage) llm.Usage {
	return llm.Usage{
		InputTokens:      a.InputTokens + b.InputTokens,
		OutputTokens:     a.OutputTokens + b.OutputTokens,
		CacheReadTokens:  a.CacheReadTokens + b.CacheReadTokens,
		CacheWriteTokens: a.CacheWriteTokens + b.CacheWriteTokens,
		ReasoningTokens:  a.ReasoningTokens + b.ReasoningTokens,
	}
}
