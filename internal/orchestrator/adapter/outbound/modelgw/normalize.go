// Package modelgw is the orchestrator's outbound adapter for the model-gateway
// gRPC service. It implements [app.ModelGatewayPort] over the generated
// [genproto.ModelGatewayServiceClient], mapping gen/ wire types to and from the
// canonical [llm] kernel types at this transport edge only (architecture §5.1,
// §12.3). No provider-specific logic lives here — all stream normalization is
// already done by the gateway; this adapter is a thin mapping layer.
package modelgw

import (
	genproto "github.com/boltrope/boltrope/gen/boltrope/v1"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// normalizeEvent maps a single gen.StreamEvent oneof to a llm.StreamEvent.
// Exactly one field in the returned llm.StreamEvent is set. A nil or
// unrecognized event returns a zero llm.StreamEvent (no panic).
func normalizeEvent(ev *genproto.StreamEvent) llm.StreamEvent {
	if ev == nil {
		return llm.StreamEvent{}
	}
	switch v := ev.GetEvent().(type) {
	case *genproto.StreamEvent_TextDelta:
		if v == nil || v.TextDelta == nil {
			return llm.StreamEvent{}
		}
		return llm.StreamEvent{
			TextDelta: &llm.TextDelta{Text: v.TextDelta.GetText()},
		}

	case *genproto.StreamEvent_ThinkingDelta:
		if v == nil || v.ThinkingDelta == nil {
			return llm.StreamEvent{}
		}
		return llm.StreamEvent{
			ThinkingDelta: &llm.ThinkingDelta{
				Text:      v.ThinkingDelta.GetText(),
				Signature: v.ThinkingDelta.GetSignature(),
			},
		}

	case *genproto.StreamEvent_ToolCallDelta:
		if v == nil || v.ToolCallDelta == nil {
			return llm.StreamEvent{}
		}
		tcd := v.ToolCallDelta
		return llm.StreamEvent{
			ToolCallDelta: &llm.ToolCallDelta{
				CallID:       tcd.GetCallId(),
				Name:         tcd.GetName(),
				ArgsPath:     tcd.GetArgsPath(),
				ArgsFragment: tcd.GetArgsFragment(),
			},
		}

	case *genproto.StreamEvent_Done:
		if v == nil || v.Done == nil {
			return llm.StreamEvent{}
		}
		return llm.StreamEvent{
			Done: normalizeDone(v.Done),
		}

	default:
		// unrecognized or nil oneof variant
		return llm.StreamEvent{}
	}
}

// normalizeDone maps a gen.Done to a llm.Done, including usage and provider_raw.
func normalizeDone(d *genproto.Done) *llm.Done {
	if d == nil {
		return &llm.Done{}
	}
	return &llm.Done{
		StopReason:    normalizeStopReason(d.GetStopReason()),
		RawStopReason: d.GetRawStopReason(),
		Usage:         normalizeUsage(d.GetUsage()),
		ProviderRaw:   d.GetProviderRaw(),
	}
}

// normalizeStopReason maps the gen StopReason enum to the open-set
// llm.StopReason string. Any unrecognized proto value maps to llm.StopOther
// (open-set escape hatch, ADR-0004; architecture §11.3).
func normalizeStopReason(r genproto.StopReason) llm.StopReason {
	switch r {
	case genproto.StopReason_STOP_REASON_END:
		return llm.StopEnd
	case genproto.StopReason_STOP_REASON_MAX_TOKENS:
		return llm.StopMaxTokens
	case genproto.StopReason_STOP_REASON_TOOL_USE:
		return llm.StopToolUse
	case genproto.StopReason_STOP_REASON_STOP_SEQUENCE:
		return llm.StopStopSequence
	case genproto.StopReason_STOP_REASON_CONTENT_FILTER:
		return llm.StopContentFilter
	case genproto.StopReason_STOP_REASON_REFUSAL:
		return llm.StopRefusal
	case genproto.StopReason_STOP_REASON_CONTEXT_WINDOW_EXCEEDED:
		return llm.StopContextWindowExceeded
	case genproto.StopReason_STOP_REASON_PAUSE:
		return llm.Pause
	default:
		// STOP_REASON_UNSPECIFIED, STOP_REASON_OTHER, or any future value
		return llm.StopOther
	}
}

// normalizeUsage maps a gen.Usage to llm.Usage. A nil gen.Usage maps to a
// zero llm.Usage.
func normalizeUsage(u *genproto.Usage) llm.Usage {
	if u == nil {
		return llm.Usage{}
	}
	return llm.Usage{
		InputTokens:      int(u.GetInputTokens()),
		OutputTokens:     int(u.GetOutputTokens()),
		CacheReadTokens:  int(u.GetCacheReadTokens()),
		CacheWriteTokens: int(u.GetCacheWriteTokens()),
		ReasoningTokens:  int(u.GetReasoningTokens()),
	}
}

// normalizeCapabilities maps a gen.Capabilities to llm.Capabilities.
func normalizeCapabilities(c *genproto.Capabilities) llm.Capabilities {
	if c == nil {
		return llm.Capabilities{}
	}
	return llm.Capabilities{
		SupportsTools:              c.GetSupportsTools(),
		SupportsParallelToolCalls:  c.GetSupportsParallelToolCalls(),
		SupportsStreamingToolCalls: c.GetSupportsStreamingToolCalls(),
		SupportsVision:             c.GetSupportsVision(),
		SupportsSystemPrompt:       c.GetSupportsSystemPrompt(),
		SupportsThinking:           c.GetSupportsThinking(),
		SupportsTokenCounting:      c.GetSupportsTokenCounting(),
		SupportsJSONSchemaStrict:   c.GetSupportsJsonSchemaStrict(),
		MaxOutputTokens:            int(c.GetMaxOutputTokens()),
	}
}
