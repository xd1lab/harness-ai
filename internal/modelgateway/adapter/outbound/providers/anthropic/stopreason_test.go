package anthropic

import (
	"testing"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// TestMapStopReason is the stop-reason mapping table (architecture §11.3): every
// known Anthropic reason maps to its first-class constant, pause_turn maps to the
// non-terminal Pause, and any unknown reason maps to StopOther.
func TestMapStopReason(t *testing.T) {
	cases := []struct {
		raw          string
		want         llm.StopReason
		wantTerminal bool
	}{
		{"end_turn", llm.StopEnd, true},
		{"max_tokens", llm.StopMaxTokens, true},
		{"tool_use", llm.StopToolUse, true},
		{"stop_sequence", llm.StopStopSequence, true},
		{"refusal", llm.StopRefusal, true},
		{"pause_turn", llm.Pause, false},
		{"some_unknown_reason", llm.StopOther, true},
		{"", llm.StopOther, true},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			got := mapStopReason(tc.raw)
			if got != tc.want {
				t.Errorf("mapStopReason(%q) = %q, want %q", tc.raw, got, tc.want)
			}
			if got.IsTerminal() != tc.wantTerminal {
				t.Errorf("mapStopReason(%q).IsTerminal() = %v, want %v", tc.raw, got.IsTerminal(), tc.wantTerminal)
			}
		})
	}
}
