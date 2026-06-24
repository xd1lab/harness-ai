// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
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
