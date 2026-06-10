// Package agent holds the orchestrator's pure agent-loop building blocks. This
// file contributes the stream assembler (T-LOOP-01); the loop itself is added in
// a later task (T-LOOP-05).
//
// The assembler is the most defect-prone piece of the loop — turning a delta
// stream into a complete [llm.Message] — isolated as a PURE app-layer function so
// it is unit-testable with a hand-written fake [llm.StreamReader] feeding
// adversarial delta sequences (split mid-UTF-8, out-of-order CallIDs,
// Pause-before-Done, duplicate Done). All provider stream normalization already
// happened in the model-gateway; this code is provider-agnostic and imports NO
// gen/ package and no provider SDK (FR-MODEL-02 AC-3; DOD-08; architecture §4.3,
// §11.2).
package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// Outcome is the three-way terminal classification of an assembled generation,
// computed from the terminal [llm.Done] (and whether any tool calls were
// assembled). The agent loop branches on these three outcomes rather than two
// (architecture §4.3, §11.3):
//
//   - [OutcomeFinal] — the turn is complete; no further provider call is needed
//     from assembly's perspective (StopEnd, StopMaxTokens, StopRefusal, etc.).
//   - [OutcomeNeedsToolExecution] — the model requested tool calls
//     ([llm.StopToolUse]) and at least one was assembled; the loop must execute
//     the tools and feed results back.
//   - [OutcomeNeedsContinuation] — a non-terminal [llm.Pause]; the loop must
//     re-issue the request echoing [Result.Done].ProviderRaw back to continue
//     (architecture §11.1).
type Outcome int

const (
	// OutcomeFinal indicates the generation ended and needs no continuation or
	// tool execution. It is the classification for every terminal stop reason
	// except a [llm.StopToolUse] that produced tool calls.
	OutcomeFinal Outcome = iota
	// OutcomeNeedsToolExecution indicates the model requested tool calls
	// ([llm.StopToolUse]) and the assembled message carries at least one
	// [llm.ToolCall]; the loop must execute them before continuing.
	OutcomeNeedsToolExecution
	// OutcomeNeedsContinuation indicates a non-terminal [llm.Pause]: the loop must
	// re-issue the request with the provider-raw continuation blob echoed back.
	OutcomeNeedsContinuation
)

// String renders the outcome for logs and test failure messages.
func (o Outcome) String() string {
	switch o {
	case OutcomeFinal:
		return "final"
	case OutcomeNeedsToolExecution:
		return "needs-tool-execution"
	case OutcomeNeedsContinuation:
		return "needs-continuation"
	default:
		return fmt.Sprintf("Outcome(%d)", int(o))
	}
}

// Result is the assembled output of a streamed generation: the model-visible
// [llm.Message], the classified terminal [Outcome], and the terminal [llm.Done]
// carrying the normalized stop reason, usage, and the opaque provider-raw
// continuation blob. The loop appends exactly one AssistantMessage event from
// this Result and, on [OutcomeNeedsContinuation], echoes Done.ProviderRaw back on
// the next request (architecture §4.3, §11.1).
type Result struct {
	// Message is the assembled assistant turn: ordered thinking, then text, then
	// tool-call content parts. Role is always [llm.RoleAssistant].
	Message llm.Message
	// Outcome is the three-way terminal classification (see [Outcome]).
	Outcome Outcome
	// Done is the terminal event of the stream: normalized stop reason, raw
	// provider stop string, usage, and the opaque provider-raw continuation blob.
	// On a stream that errored or truncated before a terminal event, Done is the
	// zero value and Assemble returns a non-nil error alongside the partial
	// Message.
	Done llm.Done
}

// Sentinel errors returned by [Assemble]. Callers branch on these with
// [errors.Is]; any mid-stream provider failure is returned verbatim (wrapped) so
// a [*llm.ProviderError] remains recoverable via [errors.As].
var (
	// ErrIncompleteStream is returned when the stream ends (io.EOF) without a
	// terminal [llm.Done] event — the provider truncated the stream. The partial
	// [Result.Message] is still returned so the loop can checkpoint usage_so_far.
	ErrIncompleteStream = errors.New("agent: stream ended without a terminal Done event")
	// ErrMalformedToolArgs is returned when a tool call's accumulated argument
	// fragments do not parse into a JSON object — a malformed provider stream is
	// never silently downgraded to a zero-argument call.
	ErrMalformedToolArgs = errors.New("agent: tool-call arguments did not parse as a JSON object")
)

// Assemble consumes reader to completion and assembles the final assistant
// [llm.Message] plus the terminal [Outcome] and [llm.Done]. It is the pure
// delta→Message boundary of the loop (architecture §4.3): it only calls Recv in a
// loop and Close when done, and applies no provider-specific handling.
//
// Accumulation rules:
//
//   - Text and thinking deltas are concatenated; the resulting message orders
//     thinking part(s) before text before tool calls (the order every provider
//     family requires for replay).
//   - Tool-call fragments are accumulated by their OPAQUE CallID — never by
//     arrival index — so interleaved, out-of-order fragments for distinct calls
//     reassemble correctly, and the calls are emitted in first-appearance order.
//     Three argument encodings are handled uniformly: append-style fragments
//     (concatenated), path-addressed fragments ([llm.ToolCallDelta.ArgsPath] set,
//     assembled into an object), and a single complete buffered fragment (the
//     SupportsStreamingToolCalls=false case). All three parse into the final
//     [llm.ToolCall.Args] map.
//   - The FIRST [llm.Done] is authoritative; any events after it (a duplicate or
//     late Done, or stray deltas) are ignored. [llm.Pause] arrives via Done and is
//     classified [OutcomeNeedsContinuation] — it is non-terminal but ends the
//     stream-assembly step.
//
// Error handling: a mid-stream Recv error (other than io.EOF) is returned wrapped,
// with the partial Message preserved. An io.EOF before any Done returns
// [ErrIncompleteStream]. Un-parseable tool arguments return [ErrMalformedToolArgs].
// reader.Close is always called exactly once before return.
func Assemble(reader llm.StreamReader) (Result, error) {
	defer func() { _ = reader.Close() }()

	acc := newAccumulator()

	var (
		done     llm.Done
		haveDone bool
	)

	for {
		ev, err := reader.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Mid-stream provider failure: return the partial message so the loop
			// can account usage_so_far, wrapping the cause for errors.As recovery.
			return Result{Message: acc.message()}, fmt.Errorf("agent: stream recv: %w", err)
		}

		switch {
		case ev.Done != nil:
			// First Done wins; stop consuming (ignore duplicate/late events).
			done = *ev.Done
			haveDone = true
		case ev.TextDelta != nil:
			acc.addText(ev.TextDelta.Text)
		case ev.ThinkingDelta != nil:
			acc.addThinking(ev.ThinkingDelta.Text, ev.ThinkingDelta.Signature)
		case ev.ToolCallDelta != nil:
			acc.addToolCall(ev.ToolCallDelta)
		default:
			// An empty StreamEvent carries no variant; skip it defensively.
		}

		if haveDone {
			break
		}
	}

	msg, parseErr := acc.finalize()
	if !haveDone {
		// Stream truncated before a terminal event. Return the partial message;
		// a parse error here is subsumed by the more fundamental truncation.
		return Result{Message: msg}, ErrIncompleteStream
	}
	if parseErr != nil {
		return Result{Message: msg, Done: done}, parseErr
	}

	return Result{
		Message: msg,
		Outcome: classify(done.StopReason, msg),
		Done:    done,
	}, nil
}

// classify maps the terminal stop reason (plus whether any tool calls were
// assembled) to the three-way [Outcome]. [llm.Pause] is the sole non-terminal
// reason → [OutcomeNeedsContinuation]. A [llm.StopToolUse] is
// [OutcomeNeedsToolExecution] only when at least one tool call was assembled;
// without calls there is nothing to execute, so it degrades to [OutcomeFinal] and
// the loop terminates rather than dispatching an empty batch. Every other terminal
// reason is [OutcomeFinal].
func classify(reason llm.StopReason, msg llm.Message) Outcome {
	if !reason.IsTerminal() {
		// Only Pause is non-terminal in the frozen contract.
		return OutcomeNeedsContinuation
	}
	if reason == llm.StopToolUse && hasToolCall(msg) {
		return OutcomeNeedsToolExecution
	}
	return OutcomeFinal
}

// hasToolCall reports whether the message carries at least one tool-call part.
func hasToolCall(msg llm.Message) bool {
	for _, p := range msg.Content {
		if p.ToolCall != nil {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// accumulator: the fold state for one streamed assistant turn.
// ---------------------------------------------------------------------------

// accumulator collects deltas for a single assistant turn. Tool calls are keyed
// by opaque CallID with first-appearance ordering preserved in callOrder, mirroring
// the pure fold style used by the recovery package.
type accumulator struct {
	thinkingText []byte
	thinkingSig  []byte
	text         []byte

	calls     map[string]*callAcc
	callOrder []string
}

// newAccumulator returns an empty accumulator ready to fold deltas.
func newAccumulator() *accumulator {
	return &accumulator{calls: make(map[string]*callAcc)}
}

// addText appends a text fragment. Fragments are concatenated at the byte level so
// a rune split across two TextDeltas reassembles byte-faithfully.
func (a *accumulator) addText(s string) {
	a.text = append(a.text, s...)
}

// addThinking appends thinking text and signature fragments. Both are concatenated
// byte-faithfully; either may be empty on a given delta.
func (a *accumulator) addThinking(text, sig string) {
	a.thinkingText = append(a.thinkingText, text...)
	a.thinkingSig = append(a.thinkingSig, sig...)
}

// addToolCall folds one tool-call fragment into the per-CallID accumulator,
// registering the CallID in first-appearance order on first sighting.
func (a *accumulator) addToolCall(d *llm.ToolCallDelta) {
	c, ok := a.calls[d.CallID]
	if !ok {
		c = &callAcc{id: d.CallID}
		a.calls[d.CallID] = c
		a.callOrder = append(a.callOrder, d.CallID)
	}
	if d.Name != "" && c.name == "" {
		c.name = d.Name
	}
	c.addArgs(d.ArgsPath, d.ArgsFragment)
}

// message builds the partial [llm.Message] WITHOUT parsing tool arguments. It is
// used to surface partial content alongside an error (mid-stream failure or
// truncation) where argument parsing is moot.
func (a *accumulator) message() llm.Message {
	parts := a.contentSansTools()
	for _, id := range a.callOrder {
		c := a.calls[id]
		parts = append(parts, llm.ContentPart{ToolCall: &llm.ToolCall{
			ID:   c.id,
			Name: c.name,
			Args: map[string]any{}, // unparsed; best-effort partial view
		}})
	}
	return llm.Message{Role: llm.RoleAssistant, Content: parts}
}

// finalize builds the fully assembled [llm.Message], parsing each tool call's
// accumulated arguments into its Args map. It returns [ErrMalformedToolArgs]
// (wrapped) if any call's arguments do not parse as a JSON object; the message is
// still returned for diagnostics.
func (a *accumulator) finalize() (llm.Message, error) {
	parts := a.contentSansTools()
	var parseErr error
	for _, id := range a.callOrder {
		c := a.calls[id]
		args, err := c.parseArgs()
		if err != nil && parseErr == nil {
			parseErr = fmt.Errorf("%w: call %q (%s): %v", ErrMalformedToolArgs, c.id, c.name, err)
		}
		parts = append(parts, llm.ContentPart{ToolCall: &llm.ToolCall{
			ID:   c.id,
			Name: c.name,
			Args: args,
		}})
	}
	return llm.Message{Role: llm.RoleAssistant, Content: parts}, parseErr
}

// contentSansTools returns the ordered thinking and text parts (thinking first,
// then text), omitting any that are empty. Tool-call parts are appended by the
// caller so the final ordering is thinking → text → tool calls.
func (a *accumulator) contentSansTools() []llm.ContentPart {
	var parts []llm.ContentPart
	if len(a.thinkingText) > 0 || len(a.thinkingSig) > 0 {
		parts = append(parts, llm.ContentPart{Thinking: &llm.ThinkingPart{
			Text:      string(a.thinkingText),
			Signature: string(a.thinkingSig),
		}})
	}
	if len(a.text) > 0 {
		parts = append(parts, llm.ContentPart{Text: &llm.TextPart{Text: string(a.text)}})
	}
	return parts
}

// ---------------------------------------------------------------------------
// callAcc: argument accumulation for a single tool call.
// ---------------------------------------------------------------------------

// callAcc accumulates the fragments of one tool call. It supports the three
// provider argument encodings the gateway can produce (architecture §11.2):
//
//   - append-style: fragments with empty ArgsPath are concatenated into appendBuf
//     (OpenAI Chat-Completions concatenable JSON string, or a single complete
//     buffered call);
//   - path-addressed: fragments with a non-empty ArgsPath set that key in
//     pathArgs (Gemini jsonPath set-at-path).
//
// A given call uses one encoding; mixing is tolerated but the path encoding takes
// precedence when any path fragment was seen.
type callAcc struct {
	id        string
	name      string
	appendBuf []byte
	pathArgs  map[string]json.RawMessage
	pathOrder []string
}

// addArgs folds one argument fragment. An empty path means append-style; a
// non-empty path sets (last write wins) that key.
func (c *callAcc) addArgs(path string, frag json.RawMessage) {
	if path == "" {
		if len(frag) > 0 {
			c.appendBuf = append(c.appendBuf, frag...)
		}
		return
	}
	if c.pathArgs == nil {
		c.pathArgs = make(map[string]json.RawMessage)
	}
	if _, seen := c.pathArgs[path]; !seen {
		c.pathOrder = append(c.pathOrder, path)
	}
	c.pathArgs[path] = frag
}

// parseArgs assembles and parses the accumulated arguments into a JSON object. It
// always returns a non-nil map; a call with no argument fragments parses to an
// empty map (a zero-argument call is callable). Path-addressed fragments are
// assembled into an object first. A non-empty buffer that does not parse as a JSON
// object returns an error so a malformed stream is never mistaken for empty args.
func (c *callAcc) parseArgs() (map[string]any, error) {
	// Path-addressed encoding: build {path: rawValue, ...} then decode.
	if len(c.pathArgs) > 0 {
		obj := make(map[string]json.RawMessage, len(c.pathArgs))
		for _, k := range c.pathOrder {
			obj[k] = c.pathArgs[k]
		}
		buf, err := json.Marshal(obj)
		if err != nil {
			return map[string]any{}, err
		}
		return decodeObject(buf)
	}

	// Append/buffered encoding.
	if len(c.appendBuf) == 0 {
		return map[string]any{}, nil
	}
	return decodeObject(c.appendBuf)
}

// decodeObject decodes raw JSON bytes into a map, requiring a JSON object. It
// returns a non-nil (possibly empty) map even on error so callers never propagate
// a nil Args.
func decodeObject(buf []byte) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal(buf, &m); err != nil {
		return map[string]any{}, err
	}
	if m == nil {
		// Valid JSON null decodes to a nil map; treat as empty args.
		return map[string]any{}, nil
	}
	return m, nil
}
