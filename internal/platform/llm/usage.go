package llm

// Usage is the normalized token accounting for a turn. Provider usage semantics
// differ — Anthropic streams cumulative usage under message_delta with a
// cache-read/cache-write split, Gemini reports usageMetadata per chunk, and OpenAI
// Responses bills the full chained context as input each turn — so the
// model-gateway reads usage from the authoritative field per surface and
// normalizes it to these four counters (architecture §11.6).
//
// Cost is computed in the gateway (which holds model pricing) from this Usage and
// is not recomputed from token counts in the orchestrator. All counts are for the
// single turn unless a provider only reports cumulative figures, in which case the
// adapter normalizes to per-turn where possible.
type Usage struct {
	// InputTokens is the number of prompt/input tokens billed at the standard
	// input rate (excludes cache reads and cache writes, which are billed
	// separately).
	InputTokens int
	// OutputTokens is the number of generated/output tokens.
	OutputTokens int
	// CacheReadTokens is the number of input tokens served from a prompt cache
	// (typically billed at a reduced rate).
	CacheReadTokens int
	// CacheWriteTokens is the number of input tokens written to a prompt cache
	// (typically billed at a premium).
	CacheWriteTokens int
	// ReasoningTokens is the number of reasoning/thinking tokens the model
	// generated, carried separately for auditing. By convention it is a SUBSET of
	// OutputTokens (already counted there for billing) unless a provider bills
	// reasoning tokens separately, in which case the adapter reflects the
	// provider's accounting. Zero when the provider does not report it.
	ReasoningTokens int
}
