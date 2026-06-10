package anthropic

import (
	sdk "github.com/anthropics/anthropic-sdk-go"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// mapStopReason maps an Anthropic stop_reason string onto the normalized OPEN
// [llm.StopReason] set (architecture §11.3). Known reasons map to first-class
// constants; pause_turn maps to the non-terminal [llm.Pause]; any unrecognized
// reason maps to [llm.StopOther] so the raw provider string is passed through
// (preserved by the caller in RawStopReason), never silently dropped.
//
// The mapping intentionally accepts the raw string rather than the SDK's typed
// [sdk.StopReason] so an unknown value a future API revision introduces still
// round-trips through StopOther instead of being lost at the type boundary.
func mapStopReason(raw string) llm.StopReason {
	switch sdk.StopReason(raw) {
	case sdk.StopReasonEndTurn:
		return llm.StopEnd
	case sdk.StopReasonMaxTokens:
		return llm.StopMaxTokens
	case sdk.StopReasonToolUse:
		return llm.StopToolUse
	case sdk.StopReasonStopSequence:
		return llm.StopStopSequence
	case sdk.StopReasonRefusal:
		return llm.StopRefusal
	case sdk.StopReasonPauseTurn:
		return llm.Pause
	default:
		return llm.StopOther
	}
}
