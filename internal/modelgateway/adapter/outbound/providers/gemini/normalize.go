package gemini

import (
	"encoding/json"

	"google.golang.org/genai"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// mapStopReason normalizes a Gemini [genai.FinishReason] onto the OPEN
// [llm.StopReason] set and returns the verbatim provider string alongside it
// (architecture §11.3). The mapping the task fixes is:
//
//	STOP                 -> StopEnd
//	MAX_TOKENS           -> StopMaxTokens
//	SAFETY, RECITATION   -> StopContentFilter
//	everything else      -> StopOther (raw string preserved, never dropped)
//
// The raw return is always the provider's own string so an unrecognized reason is
// passed through for logging rather than silently collapsed.
func mapStopReason(fr genai.FinishReason) (llm.StopReason, string) {
	raw := string(fr)
	switch fr {
	case genai.FinishReasonStop:
		return llm.StopEnd, raw
	case genai.FinishReasonMaxTokens:
		return llm.StopMaxTokens, raw
	case genai.FinishReasonSafety, genai.FinishReasonRecitation:
		return llm.StopContentFilter, raw
	default:
		return llm.StopOther, raw
	}
}

// normalizeUsage maps Gemini's usageMetadata onto the normalized [llm.Usage] counters
// (architecture §11.6). Gemini's PromptTokenCount is the TOTAL effective prompt size —
// it already INCLUDES the cached-content tokens — so the cached count is subtracted
// out of InputTokens, which by contract excludes cache reads. ThoughtsTokenCount is
// carried as ReasoningTokens. A nil metadata yields the zero Usage.
func normalizeUsage(md *genai.GenerateContentResponseUsageMetadata) llm.Usage {
	if md == nil {
		return llm.Usage{}
	}
	input := int(md.PromptTokenCount)
	cacheRead := int(md.CachedContentTokenCount)
	// PromptTokenCount includes cached tokens; the standard-rate input excludes them.
	if cacheRead > 0 {
		input -= cacheRead
		if input < 0 {
			input = 0
		}
	}
	return llm.Usage{
		InputTokens:     input,
		OutputTokens:    int(md.CandidatesTokenCount),
		CacheReadTokens: cacheRead,
		ReasoningTokens: int(md.ThoughtsTokenCount),
	}
}

// streamNormalizer converts the sequence of streamed [genai.GenerateContentResponse]
// chunks into normalized [llm.StreamEvent]s. It is the provider-event -> StreamEvent
// normalizer the architecture mandates living in the gateway (ADR-0016; §11.2), and is
// deliberately isolated and network-free so it can be golden-tested directly against
// synthetic genai responses.
//
// Per-chunk it emits a [llm.TextDelta] for each text part and a single complete
// [llm.ToolCallDelta] for each functionCall part. The Gemini Developer API does NOT
// stream partial function-call arguments (the genai PartialArgs/WillContinue fields are
// Vertex-only), so each functionCall arrives whole and is emitted as one complete
// ToolCallDelta — CallID + Name + the full JSON args in ArgsFragment, ArgsPath empty —
// which is exactly the buffered shape the orchestrator assembles uniformly (§11.2).
//
// It tracks the latest finishReason, the latest (cumulative) usageMetadata, and the
// model content seen so the terminal [llm.Done] carries the normalized stop reason,
// normalized usage, and an opaque provider-raw continuation blob. Exactly one Done is
// produced: it is emitted as soon as a finishReason is observed; [streamNormalizer.finish]
// flushes a defensive Done if the stream ended without one, so the loop never hangs.
//
// A streamNormalizer is single-use and not safe for concurrent use.
type streamNormalizer struct {
	doneEmitted bool
	callSeq     int              // monotonically increasing, for synthesizing call ids
	content     []*genai.Content // accumulated model content, for the provider-raw blob
}

// newStreamNormalizer returns a fresh, single-use stream normalizer.
func newStreamNormalizer() *streamNormalizer {
	return &streamNormalizer{}
}

// next ingests one streamed response chunk and returns the events it produces, in
// order. It returns at most one terminal [llm.Done] (when this chunk carries a
// finishReason); subsequent chunks after a Done has been emitted produce no further
// terminal event. The returned error is always nil today (chunk-level provider errors
// surface from the stream iterator, not from a decoded chunk); it is part of the
// signature so future per-chunk validation can report failures without an API change.
func (n *streamNormalizer) next(chunk *genai.GenerateContentResponse) ([]llm.StreamEvent, error) {
	if chunk == nil {
		return nil, nil
	}

	var events []llm.StreamEvent
	var finish *genai.FinishReason

	for _, cand := range chunk.Candidates {
		if cand == nil {
			continue
		}
		if cand.FinishReason != "" && finish == nil {
			fr := cand.FinishReason
			finish = &fr
		}
		if cand.Content == nil {
			continue
		}
		// Retain content for the provider-raw continuation blob.
		n.content = append(n.content, cand.Content)
		for _, part := range cand.Content.Parts {
			if part == nil {
				continue
			}
			switch {
			case part.FunctionCall != nil:
				events = append(events, n.functionCallEvent(part.FunctionCall))
			case part.Text != "":
				// A thought (reasoning) part is surfaced as a ThinkingDelta; ordinary
				// text as a TextDelta.
				if part.Thought {
					events = append(events, llm.StreamEvent{
						ThinkingDelta: &llm.ThinkingDelta{Text: part.Text},
					})
				} else {
					events = append(events, llm.StreamEvent{
						TextDelta: &llm.TextDelta{Text: part.Text},
					})
				}
			}
		}
	}

	if finish != nil && !n.doneEmitted {
		events = append(events, n.doneEvent(*finish, chunk.UsageMetadata))
	}

	return events, nil
}

// finish flushes a terminal [llm.Done] if the stream ended without an explicit
// finishReason, so a truncated or finish-reason-less stream still terminates cleanly.
// The defensive Done is classified [llm.StopOther] with an empty raw reason. It returns
// no events if a Done was already emitted.
func (n *streamNormalizer) finish() []llm.StreamEvent {
	if n.doneEmitted {
		return nil
	}
	return []llm.StreamEvent{n.doneEvent("", nil)}
}

// functionCallEvent builds a complete (buffered) ToolCallDelta for one functionCall.
// Gemini function calls carry an object of args; they are marshaled to JSON and placed
// whole in ArgsFragment with ArgsPath empty. The CallID prefers the provider-supplied
// id and otherwise synthesizes a stable per-stream id from the name and a counter, so
// every emitted call has a non-empty opaque identifier.
func (n *streamNormalizer) functionCallEvent(fc *genai.FunctionCall) llm.StreamEvent {
	id := fc.ID
	if id == "" {
		id = fc.Name + "-" + itoa(n.callSeq)
	}
	n.callSeq++

	var frag json.RawMessage
	if fc.Args != nil {
		// Marshaling a map[string]any is deterministic (Go sorts map keys), so the
		// fragment is stable for golden tests.
		if b, err := json.Marshal(fc.Args); err == nil {
			frag = b
		}
	}
	return llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{
		CallID:       id,
		Name:         fc.Name,
		ArgsFragment: frag,
	}}
}

// doneEvent builds the single terminal Done event and marks the normalizer terminated.
func (n *streamNormalizer) doneEvent(fr genai.FinishReason, usage *genai.GenerateContentResponseUsageMetadata) llm.StreamEvent {
	n.doneEmitted = true
	reason, raw := mapStopReason(fr)
	return llm.StreamEvent{Done: &llm.Done{
		StopReason:    reason,
		RawStopReason: raw,
		Usage:         normalizeUsage(usage),
		ProviderRaw:   n.providerRaw(),
	}}
}

// providerRaw serializes the accumulated model content into the opaque, provider-scoped
// continuation blob carried on [llm.Done.ProviderRaw] (architecture §11.1). It returns
// nil when no content was produced, signaling "no continuation state". Marshaling the
// genai content is byte-faithful enough to echo back the model's parts (including any
// thoughtSignature) on a subsequent call.
func (n *streamNormalizer) providerRaw() llm.ProviderRaw {
	if len(n.content) == 0 {
		return nil
	}
	b, err := json.Marshal(providerRawBlob{Content: n.content})
	if err != nil {
		return nil
	}
	return b
}

// providerRawBlob is the envelope serialized into [llm.Done.ProviderRaw] /
// [llm.Response.ProviderRaw]. It carries the model's content parts so a paused or
// replayed turn can be continued byte-faithfully.
type providerRawBlob struct {
	// Content is the model content parts produced this turn, in order.
	Content []*genai.Content `json:"content"`
}

// itoa is a tiny non-allocating-friendly integer formatter for synthesizing call ids
// without pulling strconv into the hot path's import surface intent. It handles the
// non-negative counter values used here.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
