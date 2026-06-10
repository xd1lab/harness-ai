package openai

import (
	"encoding/json"

	"github.com/openai/openai-go/v3/responses"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// Responses stream event type discriminators. The SDK models every streamed event
// as a single flat union ([responses.ResponseStreamEventUnion]) keyed by a Type
// string; these are the subset this normalizer acts on.
const (
	evtOutputTextDelta           = "response.output_text.delta"
	evtReasoningTextDelta        = "response.reasoning_text.delta"
	evtReasoningSummaryTextDelta = "response.reasoning_summary_text.delta"
	evtRefusalDelta              = "response.refusal.delta"
	evtFunctionCallArgsDelta     = "response.function_call_arguments.delta"
	evtCompleted                 = "response.completed"
	evtIncomplete                = "response.incomplete"
	evtFailed                    = "response.failed"
	evtError                     = "error"
)

// Responses output item / status discriminators.
const (
	itemTypeFunctionCall = "function_call"
	itemTypeMessage      = "message"
	itemTypeReasoning    = "reasoning"

	incompleteMaxOutputTokens = "max_output_tokens"
	incompleteContentFilter   = "content_filter"
)

// Normalizer converts a stream of OpenAI Responses typed events
// ([responses.ResponseStreamEventUnion]) into normalized [llm.StreamEvent] values.
// It is a pure, network-free accumulator: feed each event to [Normalizer.Next] in
// arrival order. The terminal response.completed / response.incomplete /
// response.failed / error event produces the tool-call deltas (when any) and the
// single [llm.Done]; there is no separate Finish call because Responses delivers an
// explicit terminal event.
//
// Text and reasoning content are emitted live. Function-call arguments arrive as
// item-scoped string deltas; rather than reassemble them fragment by fragment, the
// normalizer takes the authoritative, fully-formed function_call items from the
// Response carried on response.completed and emits one complete [llm.ToolCallDelta]
// per call (CallID + Name + the full arguments in ArgsFragment, ArgsPath empty)
// before [llm.Done]. This matches the contract that [llm.Done] never carries
// tool-call content and that the orchestrator assembles tool calls uniformly
// (architecture §11.2). The continuation blob in [llm.Done.ProviderRaw] carries the
// output Items for stateless replay (§11.1).
//
// A Normalizer is single-use and not safe for concurrent use.
type Normalizer struct {
	done bool // a terminal event has been emitted; further events are ignored
}

// NewNormalizer returns a fresh Responses [Normalizer].
func NewNormalizer() *Normalizer {
	return &Normalizer{}
}

// Next folds one Responses event into normalized [llm.StreamEvent] values. Live
// text/reasoning deltas are returned immediately; the terminal event returns the
// buffered tool-call deltas followed by the single [llm.Done]. Function-call
// argument deltas and lifecycle/bookkeeping events (output_item.added,
// content_part.*, *.done, in_progress, created, queued) produce no events here —
// the authoritative tool calls come from the terminal Response.
func (n *Normalizer) Next(ev responses.ResponseStreamEventUnion) []llm.StreamEvent {
	if n.done {
		return nil
	}
	switch ev.Type {
	case evtOutputTextDelta:
		if ev.Delta == "" {
			return nil
		}
		return []llm.StreamEvent{{TextDelta: &llm.TextDelta{Text: ev.Delta}}}

	case evtReasoningTextDelta, evtReasoningSummaryTextDelta:
		if ev.Delta == "" {
			return nil
		}
		return []llm.StreamEvent{{ThinkingDelta: &llm.ThinkingDelta{Text: ev.Delta}}}

	case evtRefusalDelta:
		// Surface refusal text as visible text; the normalized stop reason is
		// derived from the terminal Response status/output.
		if ev.Delta == "" {
			return nil
		}
		return []llm.StreamEvent{{TextDelta: &llm.TextDelta{Text: ev.Delta}}}

	case evtCompleted:
		n.done = true
		return n.terminalFromResponse(ev.Response)

	case evtIncomplete:
		n.done = true
		return n.terminalFromResponse(ev.Response)

	case evtFailed, evtError:
		n.done = true
		return []llm.StreamEvent{{Done: &llm.Done{
			StopReason:    llm.StopOther,
			RawStopReason: failedRawReason(ev),
		}}}

	default:
		// function_call_arguments.delta, output_item.added, content_part.*,
		// *.done, created, in_progress, queued, etc. — no normalized event.
		return nil
	}
}

// terminalFromResponse builds the terminal events from the authoritative Response
// payload on a completed/incomplete event: a complete [llm.ToolCallDelta] for each
// function_call output item (in output order) followed by the single [llm.Done]
// with normalized usage, the derived stop reason, and the stateless continuation
// blob.
func (n *Normalizer) terminalFromResponse(resp responses.Response) []llm.StreamEvent {
	var out []llm.StreamEvent
	for _, item := range resp.Output {
		if item.Type != itemTypeFunctionCall {
			continue
		}
		args := []byte(item.Arguments.OfString)
		if len(args) == 0 {
			args = []byte("{}")
		}
		out = append(out, llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{
			CallID:       item.CallID,
			Name:         item.Name,
			ArgsFragment: json.RawMessage(append([]byte(nil), args...)),
		}})
	}

	done := &llm.Done{
		StopReason:    stopReasonFromResponse(resp),
		RawStopReason: rawStopReason(resp),
		Usage:         normalizeResponsesUsage(resp.Usage),
	}
	if raw := continuationFromResponse(resp); raw != nil {
		done.ProviderRaw = raw
	}
	out = append(out, llm.StreamEvent{Done: done})
	return out
}

// hasFunctionCall reports whether any output item is a function call.
func hasFunctionCall(resp responses.Response) bool {
	for _, item := range resp.Output {
		if item.Type == itemTypeFunctionCall {
			return true
		}
	}
	return false
}

// stopReasonFromResponse derives the normalized [llm.StopReason] from the terminal
// Response. A function_call in the output means the model is requesting tools
// (StopToolUse). An incomplete response maps by its reason. A completed response
// without tool calls is a normal end.
func stopReasonFromResponse(resp responses.Response) llm.StopReason {
	if hasFunctionCall(resp) {
		return llm.StopToolUse
	}
	switch resp.Status {
	case responses.ResponseStatusIncomplete:
		switch resp.IncompleteDetails.Reason {
		case incompleteMaxOutputTokens:
			return llm.StopMaxTokens
		case incompleteContentFilter:
			return llm.StopContentFilter
		default:
			return llm.StopOther
		}
	case responses.ResponseStatusCompleted:
		return llm.StopEnd
	case responses.ResponseStatusFailed:
		return llm.StopOther
	default:
		// cancelled / queued / in_progress terminal-ish states.
		return llm.StopOther
	}
}

// rawStopReason returns a verbatim provider stop string for traceability: the
// incomplete reason when present, otherwise the response status.
func rawStopReason(resp responses.Response) string {
	if resp.Status == responses.ResponseStatusIncomplete && resp.IncompleteDetails.Reason != "" {
		return resp.IncompleteDetails.Reason
	}
	return string(resp.Status)
}

// failedRawReason extracts a human-readable raw reason from a failed/error event.
func failedRawReason(ev responses.ResponseStreamEventUnion) string {
	if ev.Type == evtError {
		if ev.Message != "" {
			return ev.Message
		}
		return "error"
	}
	if ev.Response.Error.Message != "" {
		return ev.Response.Error.Message
	}
	return string(responses.ResponseStatusFailed)
}

// normalizeResponsesUsage converts the Responses usage block into the normalized
// [llm.Usage]. Cached input tokens are reported separately as cache reads and
// excluded from InputTokens per the [llm.Usage] convention; reasoning tokens are a
// subset of output tokens (architecture §11.6).
func normalizeResponsesUsage(u responses.ResponseUsage) llm.Usage {
	cacheRead := int(u.InputTokensDetails.CachedTokens)
	input := int(u.InputTokens) - cacheRead
	if input < 0 {
		input = 0
	}
	return llm.Usage{
		InputTokens:     input,
		OutputTokens:    int(u.OutputTokens),
		CacheReadTokens: cacheRead,
		ReasoningTokens: int(u.OutputTokensDetails.ReasoningTokens),
	}
}
