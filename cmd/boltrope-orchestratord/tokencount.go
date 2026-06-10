package main

import (
	"context"

	"github.com/boltrope/boltrope/internal/orchestrator/app"
	"github.com/boltrope/boltrope/internal/orchestrator/app/agentctx"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// gatewayTokenCounter is the production [agentctx.TokenCounter]: it measures the
// model-visible window via the model-gateway's capability-gated CountTokens, and
// falls back to a cheap local character-based estimate when the gateway reports
// the model does not support counting (or any error). The compaction trigger only
// needs a monotone, approximately-correct measure of window growth, never a
// billing-grade count (architecture §11.6: CountTokens is never used for
// billing), so a local estimate is an acceptable degraded mode.
type gatewayTokenCounter struct {
	gw app.ModelGatewayPort
}

// newGatewayTokenCounter returns a [gatewayTokenCounter] over the model-gateway
// port.
func newGatewayTokenCounter(gw app.ModelGatewayPort) *gatewayTokenCounter {
	return &gatewayTokenCounter{gw: gw}
}

// Count returns the input token count for the window. It asks the gateway first;
// on any error (including ErrUnsupported) it returns the local estimate so the
// context manager can still decide on window growth rather than being blinded.
func (c *gatewayTokenCounter) Count(ctx context.Context, model string, msgs []llm.Message, tools []llm.ToolDef) (int, error) {
	req := llm.Request{Model: model, Messages: msgs, Tools: tools}
	if n, err := c.gw.CountTokens(ctx, req); err == nil {
		return n, nil
	}
	return estimateTokens(msgs, tools), nil
}

// estimateTokens is the local fallback: a coarse ~4-chars-per-token estimate over
// the rendered message and tool-definition text. It is monotone in window size,
// which is all the compaction trigger needs.
func estimateTokens(msgs []llm.Message, tools []llm.ToolDef) int {
	const charsPerToken = 4
	chars := 0
	for _, m := range msgs {
		for _, p := range m.Content {
			if p.Text != nil {
				chars += len(p.Text.Text)
			}
		}
	}
	for _, t := range tools {
		chars += len(t.Name) + len(t.Description) + len(t.JSONSchema)
	}
	return chars / charsPerToken
}

// Compile-time assertion that gatewayTokenCounter satisfies the port.
var _ agentctx.TokenCounter = (*gatewayTokenCounter)(nil)
