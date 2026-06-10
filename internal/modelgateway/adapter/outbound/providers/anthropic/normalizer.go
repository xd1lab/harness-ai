package anthropic

import (
	"encoding/json"
	"fmt"

	sdk "github.com/anthropics/anthropic-sdk-go"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// streamNormalizer converts the Anthropic Messages SSE event sequence into the
// normalized [llm.StreamEvent] stream. It is a deliberately isolated, pure state
// machine (no network, no SDK client) so it can be golden-tested directly against
// synthetic provider events (architecture §11.2; task TDD requirement).
//
// The Anthropic protocol the normalizer consumes:
//
//	message_start
//	  content_block_start   (text | thinking | tool_use | server_tool_use)
//	    content_block_delta (text_delta | input_json_delta | thinking_delta | signature_delta)*
//	  content_block_stop
//	  (repeated per content block)
//	message_delta           (stop_reason + cumulative usage)
//	message_stop
//
// Each call to [streamNormalizer.next] feeds one SDK event and returns zero or
// more normalized events:
//
//   - text_delta        -> [llm.TextDelta]
//   - thinking_delta    -> [llm.ThinkingDelta] (Text set)
//   - signature_delta   -> [llm.ThinkingDelta] (Signature set) and accumulated
//   - input_json_delta  -> [llm.ToolCallDelta] (append-style ArgsFragment), with
//     CallID resolved from the content-block index captured at
//     content_block_start
//   - message_delta     -> remembers stop_reason + usage (no event yet)
//   - message_stop      -> the single terminal [llm.Done] (or a [llm.Pause]
//     continuation), carrying the mapped stop reason, normalized usage, and the
//     ProviderRaw continuation blob
//
// content_block_start for a tool_use block also emits a name-only
// [llm.ToolCallDelta] so the assembler learns the tool name before any argument
// fragment arrives. message_start primes the cumulative usage baseline.
//
// The normalizer is NOT safe for concurrent use; a single goroutine drives one
// stream.
type streamNormalizer struct {
	// blockToCall maps a content-block index to the opaque tool_use id captured
	// at content_block_start, so input_json_delta fragments (which carry only the
	// index) can be attributed to the right call.
	blockToCall map[int64]string
	// blocks accumulates the assistant content blocks as they are built, so a
	// pause_turn (or signed thinking) can be serialized into ProviderRaw for
	// byte-faithful continuation/replay (architecture §11.1).
	blocks []rawBlock
	// stopReason / stopSequence hold the values carried on message_delta until
	// message_stop emits the terminal event.
	stopReason   string
	stopSequence string
	// usage is the cumulative usage carried on message_start (baseline) and
	// updated by message_delta (authoritative final figure, §11.6).
	usage llm.Usage
	// done guards against emitting more than one terminal event if a provider
	// sends a duplicate message_stop.
	done bool
}

// newStreamNormalizer returns a ready-to-use normalizer.
func newStreamNormalizer() *streamNormalizer {
	return &streamNormalizer{blockToCall: make(map[int64]string)}
}

// rawBlock is the minimal projection of an assistant content block the
// normalizer accumulates to rebuild the provider-raw continuation blob.
type rawBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	Thinking  string          `json:"thinking,omitempty"`
	Signature string          `json:"signature,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	InputJSON string          `json:"-"` // accumulated partial_json fragments
	Input     json.RawMessage `json:"input,omitempty"`
}

// next normalizes one SDK stream event into zero or more [llm.StreamEvent]s.
// It returns an error only on a malformed accumulated tool-call argument at the
// terminal event; all unrecognized event/delta variants are ignored so a future
// protocol addition is forward-compatible rather than fatal.
func (n *streamNormalizer) next(ev sdk.MessageStreamEventUnion) ([]llm.StreamEvent, error) {
	switch ev.Type {
	case "message_start":
		n.usage = mergeUsage(n.usage, usageFromMessageStart(ev.AsMessageStart().Message))
		return nil, nil
	case "content_block_start":
		return n.onBlockStart(ev.AsContentBlockStart()), nil
	case "content_block_delta":
		return n.onBlockDelta(ev.AsContentBlockDelta()), nil
	case "content_block_stop":
		n.onBlockStop(ev.AsContentBlockStop())
		return nil, nil
	case "message_delta":
		n.onMessageDelta(ev.AsMessageDelta())
		return nil, nil
	case "message_stop":
		return n.onMessageStop()
	default:
		// Unknown event type: ignore (forward-compatible).
		return nil, nil
	}
}

// onBlockStart records a new content block and, for tool calls, maps its index to
// the tool_use id and emits a name-only ToolCallDelta.
func (n *streamNormalizer) onBlockStart(ev sdk.ContentBlockStartEvent) []llm.StreamEvent {
	cb := ev.ContentBlock
	blk := rawBlock{Type: cb.Type, Text: cb.Text, Thinking: cb.Thinking, Signature: cb.Signature, ID: cb.ID, Name: cb.Name}
	n.setBlock(ev.Index, blk)

	switch cb.Type {
	case "tool_use", "server_tool_use":
		n.blockToCall[ev.Index] = cb.ID
		// Emit a name-only delta so the assembler learns the tool name up front.
		return []llm.StreamEvent{{ToolCallDelta: &llm.ToolCallDelta{CallID: cb.ID, Name: cb.Name}}}
	default:
		return nil
	}
}

// onBlockDelta normalizes a content_block_delta into the matching delta event and
// accumulates the fragment into the block being built.
func (n *streamNormalizer) onBlockDelta(ev sdk.ContentBlockDeltaEvent) []llm.StreamEvent {
	switch ev.Delta.Type {
	case "text_delta":
		n.appendText(ev.Index, ev.Delta.Text)
		return []llm.StreamEvent{{TextDelta: &llm.TextDelta{Text: ev.Delta.Text}}}
	case "thinking_delta":
		n.appendThinking(ev.Index, ev.Delta.Thinking)
		return []llm.StreamEvent{{ThinkingDelta: &llm.ThinkingDelta{Text: ev.Delta.Thinking}}}
	case "signature_delta":
		n.appendSignature(ev.Index, ev.Delta.Signature)
		return []llm.StreamEvent{{ThinkingDelta: &llm.ThinkingDelta{Signature: ev.Delta.Signature}}}
	case "input_json_delta":
		n.appendInputJSON(ev.Index, ev.Delta.PartialJSON)
		callID := n.blockToCall[ev.Index]
		return []llm.StreamEvent{{ToolCallDelta: &llm.ToolCallDelta{
			CallID:       callID,
			ArgsFragment: json.RawMessage(ev.Delta.PartialJSON),
		}}}
	default:
		// citations_delta and any future delta types are ignored.
		return nil
	}
}

// onBlockStop finalizes the tool-call argument JSON for the stopped block (so the
// provider-raw blob holds a parsed object, not a fragment string).
func (n *streamNormalizer) onBlockStop(ev sdk.ContentBlockStopEvent) {
	idx := ev.Index
	if idx < 0 || int(idx) >= len(n.blocks) {
		return
	}
	b := &n.blocks[idx]
	if b.InputJSON != "" {
		b.Input = json.RawMessage(b.InputJSON)
	} else if b.Type == "tool_use" || b.Type == "server_tool_use" {
		// A tool call with no streamed args is an empty object.
		b.Input = json.RawMessage("{}")
	}
}

// onMessageDelta captures the terminal stop_reason/stop_sequence and the
// authoritative cumulative usage.
func (n *streamNormalizer) onMessageDelta(ev sdk.MessageDeltaEvent) {
	n.stopReason = string(ev.Delta.StopReason)
	n.stopSequence = ev.Delta.StopSequence
	n.usage = mergeDeltaUsage(n.usage, ev.Usage)
}

// onMessageStop emits the single terminal Done (or non-terminal Pause)
// event, attaching the mapped stop reason, normalized usage, and the
// continuation blob.
func (n *streamNormalizer) onMessageStop() ([]llm.StreamEvent, error) {
	if n.done {
		// Duplicate terminal event: ignore.
		return nil, nil
	}
	n.done = true

	raw, err := n.providerRaw()
	if err != nil {
		return nil, err
	}
	return []llm.StreamEvent{{Done: &llm.Done{
		StopReason:    mapStopReason(n.stopReason),
		RawStopReason: n.stopReason,
		Usage:         n.usage,
		ProviderRaw:   raw,
	}}}, nil
}

// providerRaw serializes the accumulated assistant content blocks into the opaque
// continuation blob carried on Done/Pause. It is non-nil whenever any block was
// produced so a paused turn or signed thinking can be replayed byte-faithfully
// (architecture §11.1). On a turn with no content blocks it is nil.
func (n *streamNormalizer) providerRaw() (llm.ProviderRaw, error) {
	if len(n.blocks) == 0 {
		return nil, nil
	}
	payload := continuationBlob{Role: "assistant", Content: n.blocks}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("anthropic: marshal continuation blob: %w", err)
	}
	return llm.ProviderRaw(b), nil
}

// continuationBlob is the wrapper serialized into [llm.Done.ProviderRaw].
type continuationBlob struct {
	Role    string     `json:"role"`
	Content []rawBlock `json:"content"`
}

// --- block accumulation helpers -------------------------------------------

func (n *streamNormalizer) setBlock(idx int64, b rawBlock) {
	for int64(len(n.blocks)) <= idx {
		n.blocks = append(n.blocks, rawBlock{})
	}
	n.blocks[idx] = b
}

func (n *streamNormalizer) ensure(idx int64) *rawBlock {
	for int64(len(n.blocks)) <= idx {
		n.blocks = append(n.blocks, rawBlock{})
	}
	return &n.blocks[idx]
}

func (n *streamNormalizer) appendText(idx int64, s string)      { n.ensure(idx).Text += s }
func (n *streamNormalizer) appendThinking(idx int64, s string)  { n.ensure(idx).Thinking += s }
func (n *streamNormalizer) appendSignature(idx int64, s string) { n.ensure(idx).Signature += s }
func (n *streamNormalizer) appendInputJSON(idx int64, s string) { n.ensure(idx).InputJSON += s }
