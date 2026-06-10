package anthropic

import (
	"strings"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// capsEntry is a static per-model capability record. Capability variability on
// Anthropic is per-model within the one endpoint (context window, max output,
// thinking) so the table is keyed by a model-id prefix (architecture §11.4).
type capsEntry struct {
	prefix string
	caps   llm.Capabilities
}

// anthropicCommon are the capabilities shared by all current first-class
// Anthropic models on the Messages API: tools with parallel and streamed
// tool-call arguments, vision, a top-level system prompt, extended thinking,
// the count_tokens endpoint, and strict tool-schema enforcement.
func anthropicCommon(maxOut int) llm.Capabilities {
	return llm.Capabilities{
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           true,
		SupportsTokenCounting:      true,
		SupportsJSONSchemaStrict:   true,
		MaxOutputTokens:            maxOut,
	}
}

// capsTable is the static capability table, matched by model-id prefix in order.
// More specific prefixes are listed first. Output-token ceilings follow the
// published model limits (architecture §11.4); an unknown Anthropic model falls
// back to defaultCaps.
var capsTable = []capsEntry{
	{prefix: "claude-opus-4", caps: anthropicCommon(128_000)},
	{prefix: "claude-sonnet-4", caps: anthropicCommon(64_000)},
	{prefix: "claude-haiku-4", caps: anthropicCommon(64_000)},
	{prefix: "claude-3-7-sonnet", caps: anthropicCommon(64_000)},
	{prefix: "claude-3-5-haiku", caps: anthropicCommon(8_192)},
	{prefix: "claude-3-5-sonnet", caps: anthropicCommon(8_192)},
	{prefix: "claude-3-opus", caps: anthropicCommon(4_096)},
	{prefix: "claude-3-haiku", caps: anthropicCommon(4_096)},
	{prefix: "claude-3", caps: anthropicCommon(4_096)},
}

// defaultCaps is returned for a model id that matches no table prefix. It enables
// the broadly-supported Messages API features with a conservative output ceiling
// so the orchestrator neither over-promises nor blocks a valid newer model.
var defaultCaps = anthropicCommon(8_192)

// resolveCapabilities returns the [llm.Capabilities] for an Anthropic model id,
// matching the longest applicable prefix and falling back to defaultCaps.
func resolveCapabilities(model string) llm.Capabilities {
	for _, e := range capsTable {
		if strings.HasPrefix(model, e.prefix) {
			return e.caps
		}
	}
	return defaultCaps
}
