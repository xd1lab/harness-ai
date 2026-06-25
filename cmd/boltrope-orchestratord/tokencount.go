package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agentctx"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
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
//
// It counts every token-bearing [llm.ContentPart] variant, not just text:
// assistant Text, the textual ToolResult.Content fed back to the model, the
// ToolCall name plus its JSON-marshaled args, and Thinking text. Image bytes are
// deliberately excluded because they are not model-text tokens. The ContentPart
// union has exactly one field non-nil per part (see message.go), so the branches
// are mutually exclusive and cannot double-count. The Args contribution is
// deterministic regardless of map iteration order because json.Marshal sorts
// object keys; on a marshal error we fall back to fmt rendering so a
// non-serializable arg still contributes non-zero characters.
func estimateTokens(msgs []llm.Message, tools []llm.ToolDef) int {
	const charsPerToken = 4
	chars := 0
	for _, m := range msgs {
		for _, p := range m.Content {
			switch {
			case p.Text != nil:
				chars += len(p.Text.Text)
			case p.ToolResult != nil:
				chars += len(p.ToolResult.Content)
			case p.ToolCall != nil:
				chars += len(p.ToolCall.Name)
				if b, err := json.Marshal(p.ToolCall.Args); err == nil {
					chars += len(b)
				} else {
					chars += len(fmt.Sprintf("%v", p.ToolCall.Args))
				}
			case p.Thinking != nil:
				chars += len(p.Thinking.Text)
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
