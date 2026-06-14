// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// Compile-time assertion: devModel satisfies the 4-method [app.ModelGatewayPort]
// (Generate/Stream/CountTokens/Capabilities) the loop depends on.
var _ app.ModelGatewayPort = (*devModel)(nil)

// devModel adapts a plain [llm.Provider] directly to the orchestrator's
// [app.ModelGatewayPort] — WITHOUT a model-gateway daemon, gRPC, or
// modelgateway/app.Service. The two interfaces have identical method shapes
// (Generate/Stream/CountTokens/Capabilities over the llm kernel types), so this
// is a thin pass-through.
//
// This is the deliberate dev-mode wiring (K-1/K-2 BLOCKER 4): the production
// modelgateway/app.Service has NO Generate method and its NewService fails closed
// without a CapabilityResolver + CostFunc + Endpoint, so it cannot back the loop's
// ModelGatewayPort in a single process. Wrapping the keyless stub llm.Provider
// directly is what test/eval/harness.go effectively proves works end-to-end, and
// it keeps the dev binary free of the entire model-gateway service edge (enforced
// by the import-graph guard in imports_test.go).
type devModel struct {
	provider llm.Provider
}

// newDevModel wraps an [llm.Provider] (e.g. the keyless stub) as a dev
// [app.ModelGatewayPort]. The constructor accepts an llm.Provider directly — never
// a *modelgateway/app.Service or a gRPC client — pinning the dev seam.
func newDevModel(provider llm.Provider) *devModel {
	return &devModel{provider: provider}
}

// Generate delegates to the wrapped provider. The loop never calls Generate at
// runtime (it always streams), but the method must exist to satisfy the port.
func (m *devModel) Generate(ctx context.Context, req llm.Request) (*llm.Response, error) {
	return m.provider.Generate(ctx, req)
}

// Stream delegates to the wrapped provider. This is the only Model method the loop
// calls at runtime, so it is the runtime-load-bearing path.
func (m *devModel) Stream(ctx context.Context, req llm.Request) (llm.StreamReader, error) {
	return m.provider.Stream(ctx, req)
}

// CountTokens delegates to the wrapped provider.
func (m *devModel) CountTokens(ctx context.Context, req llm.Request) (int, error) {
	return m.provider.CountTokens(ctx, req)
}

// Capabilities delegates to the wrapped provider, keyed by model id.
func (m *devModel) Capabilities(ctx context.Context, model string) (llm.Capabilities, error) {
	return m.provider.Capabilities(ctx, model)
}
