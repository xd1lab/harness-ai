package anthropic

import (
	sdk "github.com/anthropics/anthropic-sdk-go"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// usageFromMessage converts the full [sdk.Usage] carried on message_start (and on
// a non-streaming [sdk.Message]) into the normalized [llm.Usage], reading each
// counter from its authoritative provider field (architecture §11.6). Cache reads
// and writes are kept distinct from standard input tokens; reasoning tokens come
// from the output-token breakdown.
func usageFromMessage(u sdk.Usage) llm.Usage {
	return llm.Usage{
		InputTokens:      int(u.InputTokens),
		OutputTokens:     int(u.OutputTokens),
		CacheReadTokens:  int(u.CacheReadInputTokens),
		CacheWriteTokens: int(u.CacheCreationInputTokens),
		ReasoningTokens:  int(u.OutputTokensDetails.ThinkingTokens),
	}
}

// mergeUsage folds a baseline usage (from message_start) into the running total.
// Anthropic's message_start carries the input/cache figures and message_delta
// carries the authoritative cumulative figures, so a per-field max keeps the
// largest (most complete) value seen for each counter without double-counting.
func mergeUsage(base, next llm.Usage) llm.Usage {
	return llm.Usage{
		InputTokens:      maxInt(base.InputTokens, next.InputTokens),
		OutputTokens:     maxInt(base.OutputTokens, next.OutputTokens),
		CacheReadTokens:  maxInt(base.CacheReadTokens, next.CacheReadTokens),
		CacheWriteTokens: maxInt(base.CacheWriteTokens, next.CacheWriteTokens),
		ReasoningTokens:  maxInt(base.ReasoningTokens, next.ReasoningTokens),
	}
}

// mergeDeltaUsage folds the cumulative usage carried on a message_delta event into
// the running total. The message_delta usage is the authoritative end-of-turn
// figure (cumulative input + output, with the cache split), so its values take a
// per-field max with the baseline captured at message_start (architecture §11.6).
func mergeDeltaUsage(base llm.Usage, u sdk.MessageDeltaUsage) llm.Usage {
	delta := llm.Usage{
		InputTokens:      int(u.InputTokens),
		OutputTokens:     int(u.OutputTokens),
		CacheReadTokens:  int(u.CacheReadInputTokens),
		CacheWriteTokens: int(u.CacheCreationInputTokens),
		ReasoningTokens:  int(u.OutputTokensDetails.ThinkingTokens),
	}
	return mergeUsage(base, delta)
}

// usageFromMessageStart is the message_start hook: it normalizes the embedded
// full usage as the baseline.
func usageFromMessageStart(m sdk.Message) llm.Usage {
	return usageFromMessage(m.Usage)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
