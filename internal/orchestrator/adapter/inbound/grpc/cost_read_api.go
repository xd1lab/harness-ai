// SPDX-License-Identifier: Apache-2.0

package grpc

import (
	"context"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// This file implements the session/tenant cost read surface (Feature O /
// cost-read; ADR-0026): the two additive RPCs GetSessionCost and GetTenantCost,
// plus the read-only ModelCostRow type the EventStore consumer-superset exposes.
// Both reuse the SAME ownership path as the rest of the edge (authorizeTenant /
// authorizeSession, zero new auth). The per-model breakdown is produced at the
// WRITE side (projectord correlates TurnStarted.Model to the terminal event by
// TurnID and lands it in session_cost_events.model); the read side only
// SUM(...) GROUP BY model. An uncorrelated terminal event carries model "" in the
// store, which the server maps to the "unknown" bucket (never dropped).

// ---- read-only cost row type (the EventStore superset contract) -------------

// ModelCostRow is one per-model cost rollup row returned by the cost-read store
// methods ([EventStore.SessionCostByModel] / [EventStore.TenantCostByModel]). It
// is owned by this edge (the consumer that defines the EventStore interface) and
// aliased by the eventstore adapter so *Store satisfies the interface without a
// wrapper. It is domain/stdlib-typed (no gen/), keeping the store
// transport-agnostic. Model "" is the uncorrelated bucket the server renders as
// "unknown".
type ModelCostRow struct {
	// Model is the correlated model id; "" when the terminal event could not be
	// correlated to a TurnStarted (the server renders this as "unknown").
	Model string
	// CostUSD is the summed cost for this model (SQL-side NUMERIC sum -> float64).
	CostUSD float64
	// Usage is the summed token usage for this model.
	Usage llm.Usage
	// Turns is the count of cost-bearing terminal events folded into this model.
	Turns int64
}

// unknownModelLabel is the wire label for the uncorrelated ("") model bucket.
const unknownModelLabel = "unknown"

// ---- GetSessionCost ---------------------------------------------------------

// GetSessionCost returns an owned session's per-model cost rollup plus the session
// total (ADR-0026). It reuses the FULL ownership path (authorizeTenant +
// authorizeSession): a foreign-tenant session is PermissionDenied, a missing /
// RLS-invisible one is NotFound. The per-model breakdown is read from the
// session_cost_events projection (SUM ... GROUP BY model) and sorted by cost_usd
// descending; the total is the sum of the breakdown (a partition).
func (s *Server) GetSessionCost(ctx context.Context, req *genproto.GetSessionCostRequest) (*genproto.GetSessionCostResponse, error) {
	tenant, err := s.authorizeTenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	if _, err := s.authorizeSession(ctx, tenant, req.GetSessionId()); err != nil {
		return nil, err
	}

	rows, err := s.log.SessionCostByModel(ctx, req.GetSessionId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "orchestrator: session cost: %v", err)
	}
	byModel, total := toGenCostBreakdown(rows)
	return &genproto.GetSessionCostResponse{
		SessionId: req.GetSessionId(),
		TenantId:  tenant,
		Total:     total,
		ByModel:   byModel,
	}, nil
}

// ---- GetTenantCost ----------------------------------------------------------

// GetTenantCost returns the authenticated tenant's per-model cost aggregate, the
// tenant total, and the count of distinct sessions carrying cost (ADR-0026). It
// runs authorizeTenant ONLY (a tenant-range guard — the request tenant_id must
// match the principal when non-empty but is never a filter key); the row set is
// RLS-scoped to the principal's tenant at the store, so omitting tenant_id yields
// the identical result. There is NO authorizeSession (this is the tenant range).
func (s *Server) GetTenantCost(ctx context.Context, req *genproto.GetTenantCostRequest) (*genproto.GetTenantCostResponse, error) {
	tenant, err := s.authorizeTenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}

	rows, err := s.log.TenantCostByModel(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "orchestrator: tenant cost: %v", err)
	}
	count, err := s.log.TenantSessionCostCount(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "orchestrator: tenant session count: %v", err)
	}
	byModel, total := toGenCostBreakdown(rows)
	return &genproto.GetTenantCostResponse{
		TenantId:     tenant,
		Total:        total,
		ByModel:      byModel,
		SessionCount: count,
	}, nil
}

// ---- mappers ----------------------------------------------------------------

// toGenCostBreakdown maps store cost rows to the wire by_model breakdown (sorted
// by cost_usd descending, empty model rendered as "unknown") and the folded total
// (the partition the by_model sums reconstruct exactly).
func toGenCostBreakdown(rows []ModelCostRow) ([]*genproto.ModelCost, *genproto.CostTotals) {
	byModel := make([]*genproto.ModelCost, 0, len(rows))
	var (
		totalCost  float64
		totalUsage llm.Usage
		totalTurns int64
	)
	for _, r := range rows {
		model := r.Model
		if model == "" {
			model = unknownModelLabel
		}
		byModel = append(byModel, &genproto.ModelCost{
			Model:   model,
			CostUsd: r.CostUSD,
			Usage:   toGenUsage(r.Usage),
			Turns:   r.Turns,
		})
		totalCost += r.CostUSD
		totalUsage = addUsage(totalUsage, r.Usage)
		totalTurns += r.Turns
	}
	// Sort by cost_usd descending (the contract: most expensive model first).
	sort.SliceStable(byModel, func(i, j int) bool {
		return byModel[i].GetCostUsd() > byModel[j].GetCostUsd()
	})
	total := &genproto.CostTotals{
		CostUsd: totalCost,
		Usage:   toGenUsage(totalUsage),
		Turns:   totalTurns,
	}
	return byModel, total
}
