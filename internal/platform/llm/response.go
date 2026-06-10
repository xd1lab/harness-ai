package llm

// StopReason is the normalized reason a generation stopped. It is an OPEN set, not
// a closed enum: known reasons are first-class typed constants, and any reason a
// provider reports that does not map to one of them is normalized to [StopOther]
// with the original provider string preserved in [Response.RawStopReason] /
// [Done.RawStopReason]. An unrecognized reason is therefore passed through and
// logged, never silently dropped (ADR-0004; ADR-0016; architecture §11.3).
//
// [Pause] is special: it is a NON-TERMINAL continuation signal, distinct from all
// terminal reasons. The agent loop branches on three outcomes — final /
// needs-tool-execution ([StopToolUse]) / needs-continuation ([Pause]) — rather than
// two. Use [StopReason.IsTerminal] to distinguish.
type StopReason string

const (
	// StopEnd is a normal, complete end of turn (the model finished).
	StopEnd StopReason = "end"
	// StopMaxTokens means generation hit the requested output-token limit.
	StopMaxTokens StopReason = "max_tokens"
	// StopToolUse means the model is requesting one or more tool calls and is
	// waiting for their results before continuing. Terminal for the generation
	// step, but the loop continues by executing tools and sending results back.
	StopToolUse StopReason = "tool_use"
	// StopStopSequence means generation halted because a configured stop
	// sequence was produced.
	StopStopSequence StopReason = "stop_sequence"
	// StopContentFilter means generation was halted by a provider content
	// filter / safety system.
	StopContentFilter StopReason = "content_filter"
	// StopRefusal means the model declined to comply. It is a first-class
	// reason mapped to a distinct termination subtype (e.g. for a fallback-model
	// policy), NOT folded into an execution error (architecture §11.3).
	StopRefusal StopReason = "refusal"
	// StopContextWindowExceeded means the input plus generation exceeded the
	// model's context window. It is first-class so the loop can apply a
	// compact-and-retry policy, distinct from [StopMaxTokens].
	StopContextWindowExceeded StopReason = "context_window_exceeded"
	// Pause is a NON-TERMINAL outcome: the provider paused the turn and expects
	// the response to be echoed back (via [ProviderRaw]) to continue (e.g.
	// Anthropic pause_turn for server-side tools). It is distinct from [Done];
	// see [StopReason.IsTerminal].
	Pause StopReason = "pause"
	// StopOther is the open-set escape hatch: a provider reason that does not map
	// to any constant above. The original provider string is preserved in
	// [Response.RawStopReason] / [Done.RawStopReason].
	StopOther StopReason = "other"
)

// IsTerminal reports whether the stop reason ends the generation outcome. All
// reasons are terminal except [Pause], which signals that the turn must be
// continued by echoing the provider-raw continuation state back. [StopToolUse] is
// terminal for the generation step (the loop then executes tools and issues a new
// request), so it reports true.
func (r StopReason) IsTerminal() bool {
	return r != Pause
}

// Response is the normalized result of a non-streaming [Provider.Generate] call.
type Response struct {
	// Content is the ordered assistant content the model produced: text,
	// thinking, and tool-call parts.
	Content []ContentPart

	// StopReason is the normalized reason generation stopped. When it is
	// [StopOther], consult RawStopReason for the original provider value.
	StopReason StopReason

	// RawStopReason is the verbatim provider stop-reason string. It is always
	// populated by adapters for traceability and is the authoritative value when
	// StopReason is [StopOther].
	RawStopReason string

	// Usage is the normalized token usage for this turn, read from the
	// provider's authoritative usage field (architecture §11.6).
	Usage Usage

	// ProviderRaw is the opaque, provider-scoped continuation blob for this turn.
	// It carries provider-native state (Anthropic server_tool_use blocks /
	// thinking signatures, OpenAI Responses Items) that must be echoed back via
	// [Request.ProviderRaw] to continue (notably on [Pause]) or to replay
	// byte-faithfully. Nil when the provider needs no continuation state.
	ProviderRaw ProviderRaw
}
