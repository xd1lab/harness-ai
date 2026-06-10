package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/boltrope/boltrope/internal/orchestrator/app/agentctx"
	"github.com/boltrope/boltrope/internal/orchestrator/domain"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// maxPauseContinuations bounds the in-turn Pause→continue loop so a provider
// that pauses forever cannot spin the turn indefinitely (architecture §11.1). A
// Pause is a continuation of the SAME turn (FR-MODEL-04 AC-1: Pause then Done →
// one TurnFinished), so it does not consume a max-turns budget.
const maxPauseContinuations = 16

// runTurn performs exactly one model round-trip for the run: it appends a
// TurnStarted, streams the model (forwarding deltas to the client and feeding
// the pure assembler), appends one AssistantMessage, and branches:
//
//   - on a tool-use outcome it runs the permission pipeline and dispatches the
//     tools, feeds results back as a tool-role message, and returns turnContinue;
//   - on a final outcome it classifies the terminal reason (success / refusal /
//     structured-output retry-or-exhaust) and returns turnTerminal (or
//     turnContinue to retry structured output);
//   - on an unrecoverable stream error it appends a TurnAborted and returns
//     turnAborted.
//
// It updates the run's cumulative usage/cost and turn count.
func (l *Loop) runTurn(ctx context.Context, st *runState) (turnOutcome, domain.TerminationReason, error) {
	// Honor cancellation up front so an interrupt delivered between turns exits
	// promptly (FR-LOOP-03).
	if err := ctx.Err(); err != nil {
		return turnAborted, domain.ErrorDuringExecution, nil
	}

	// Build the model-visible window from the folded log.
	req, err := l.buildRequest(ctx, st)
	if err != nil {
		return 0, "", err
	}

	// Append the durable TurnStarted BEFORE Generate (FR-LOOP-05).
	turnID := l.deps.IDs.NewID().String()
	st.currentTurnID = turnID
	st.numTurns++
	if err := l.append(ctx, st, domain.ActorSystem, domain.TurnStarted{TurnID: turnID, Model: l.cfg.Model}); err != nil {
		return 0, "", err
	}

	// Stream + assemble, handling in-turn Pause continuations.
	res, perr := l.streamWithContinuations(ctx, st, turnID, req)
	if perr != nil {
		// Unrecoverable stream/assembly failure: append a TurnAborted carrying
		// usage_so_far (zero here — no usage was observed) and surface
		// error_during_execution (FR-LOOP-02 AC-2). A refusal is NOT an error
		// and never reaches this branch (it is a normal terminal Done).
		if err := l.append(ctx, st, domain.ActorSystem, domain.TurnAborted{
			TurnID:     turnID,
			Reason:     domain.ErrorDuringExecution,
			UsageSoFar: res.partialUsage,
			CostUSD:    l.computeCost(res.partialUsage),
		}); err != nil {
			return 0, "", err
		}
		st.usage = addUsage(st.usage, res.partialUsage)
		st.cost += l.computeCost(res.partialUsage)
		l.metrics.RecordRunError(string(domain.ErrorDuringExecution))
		return turnAborted, domain.ErrorDuringExecution, nil
	}

	// Append the single assembled AssistantMessage for the turn (assembled
	// message + usage/cost/provider_raw; architecture §4.3).
	cost := l.computeCost(res.assembled.Done.Usage)
	st.usage = addUsage(st.usage, res.assembled.Done.Usage)
	st.cost += cost
	if err := l.append(ctx, st, domain.ActorAssistant, domain.AssistantMessage{
		TurnID:        turnID,
		Message:       res.assembled.Message,
		StopReason:    res.assembled.Done.StopReason,
		RawStopReason: res.assembled.Done.RawStopReason,
		Usage:         res.assembled.Done.Usage,
		CostUSD:       cost,
		ProviderRaw:   res.assembled.Done.ProviderRaw,
	}); err != nil {
		return 0, "", err
	}
	// The AssistantMessage is a single-event append, so its seq is now the head.
	// It is the seq each tool call's idempotency key is derived from.
	st.lastAssistantSeq = st.headSeq

	switch res.assembled.Outcome {
	case OutcomeNeedsToolExecution:
		return l.handleToolCalls(ctx, st, res.assembled.Message)

	case OutcomeFinal:
		return l.classifyFinal(ctx, st, res.assembled)

	default:
		// OutcomeNeedsContinuation should have been resolved by
		// streamWithContinuations; reaching here means the Pause budget was
		// exhausted. Treat as an execution error.
		if err := l.append(ctx, st, domain.ActorSystem, domain.TurnAborted{
			TurnID: turnID, Reason: domain.ErrorDuringExecution, UsageSoFar: res.assembled.Done.Usage,
		}); err != nil {
			return 0, "", err
		}
		l.metrics.RecordRunError(string(domain.ErrorDuringExecution))
		return turnAborted, domain.ErrorDuringExecution, nil
	}
}

// streamResult bundles the assembled output of a (possibly multi-Pause) turn and
// the partial usage observed if the stream failed.
type streamResult struct {
	assembled    Result
	partialUsage llm.Usage
}

// streamWithContinuations runs Stream → Assemble, transparently re-issuing on a
// non-terminal Pause (echoing the provider-raw continuation blob) until a
// terminal Done or the Pause budget is exhausted. A Pause continuation is part
// of the SAME turn, so only the final assembled Result is returned (the loop
// appends one AssistantMessage per turn). It forwards text/thinking deltas to
// the client sink as they arrive (architecture §4.3).
func (l *Loop) streamWithContinuations(ctx context.Context, st *runState, turnID string, req llm.Request) (streamResult, error) {
	for i := 0; i < maxPauseContinuations; i++ {
		reader, err := l.deps.Model.Stream(ctx, req)
		if err != nil {
			// Failure to start the stream (e.g. a ProviderError). With no retry
			// budget the loop surfaces this as an execution error.
			return streamResult{}, fmt.Errorf("agent: stream: %w", err)
		}

		// Wrap the reader so deltas are forwarded live while the pure assembler
		// consumes the same events.
		fwd := &forwardingReader{
			inner:     reader,
			sink:      l.sink,
			sessionID: st.sessionID,
			turnID:    turnID,
		}
		assembled, aerr := Assemble(fwd)
		if aerr != nil {
			// A mid-stream provider error or truncation. Surface the partial
			// usage so the TurnAborted accounts what was observed.
			return streamResult{partialUsage: assembled.Done.Usage}, fmt.Errorf("%w: %w", errAssemble, aerr)
		}

		if assembled.Outcome != OutcomeNeedsContinuation {
			return streamResult{assembled: assembled}, nil
		}

		// Pause: re-issue echoing the provider-raw continuation blob. The
		// assistant content produced so far rides in ProviderRaw byte-faithfully
		// (architecture §11.1); we do not append a per-Pause AssistantMessage.
		req.ProviderRaw = assembled.Done.ProviderRaw
	}
	// Pause budget exhausted: return the last assembled result so runTurn can
	// abort the turn deterministically.
	return streamResult{}, fmt.Errorf("%w: pause continuation budget exhausted", errAssemble)
}

// classifyFinal classifies a terminal (final-outcome) assistant turn into the
// run's termination reason. A refusal is its own subtype (architecture §11.3).
// When structured output is configured the assembled text is validated against
// the schema; an invalid response retries (up to the cap) or, on exhaustion,
// terminates with error_max_structured_output_retries (FR-LOOP-02).
func (l *Loop) classifyFinal(ctx context.Context, st *runState, res Result) (turnOutcome, domain.TerminationReason, error) {
	if res.Done.StopReason == llm.StopRefusal {
		return turnTerminal, domain.Refusal, nil
	}

	compiled, ok, err := l.compileOutputSchema()
	if err != nil {
		return 0, "", err
	}
	if !ok {
		// Free-form output: a terminal text turn is a success.
		return turnTerminal, domain.Success, nil
	}

	// Structured output: validate the assembled assistant text against the
	// schema.
	if l.validateStructured(compiled, res.Message) {
		return turnTerminal, domain.Success, nil
	}

	// Invalid. Retry up to the cap.
	limit := l.cfg.MaxStructuredOutputRetries
	if limit <= 0 {
		limit = DefaultStructuredOutputRetries
	}
	if st.structuredRetries >= limit {
		return turnTerminal, domain.ErrorMaxStructuredOutputRetries, nil
	}
	st.structuredRetries++

	// Feed a corrective instruction back as a user message and let the loop run
	// another turn (FR-LOOP-02 / structured-output retry).
	if err := l.append(ctx, st, domain.ActorSystem, domain.MessageAppended{Message: structuredRetryMessage()}); err != nil {
		return 0, "", err
	}
	return turnContinue, "", nil
}

// validateStructured reports whether the assistant message's concatenated text
// parses as JSON and satisfies the compiled output schema.
func (l *Loop) validateStructured(compiled interface {
	ValidateRaw(json.RawMessage) error
}, msg llm.Message,
) bool {
	text := concatText(msg)
	if text == "" {
		return false
	}
	return compiled.ValidateRaw(json.RawMessage(text)) == nil
}

// structuredRetryMessage is the corrective user turn appended when a structured
// response failed validation, instructing the model to return schema-valid JSON.
func structuredRetryMessage() llm.Message {
	return llm.Message{
		Role: llm.RoleUser,
		Content: []llm.ContentPart{{Text: &llm.TextPart{
			Text: "Your previous response did not match the required JSON schema. Respond again with valid JSON that conforms to the schema.",
		}}},
	}
}

// buildRequest folds the session log into the model-visible window (via the
// context manager when present, else directly) and assembles the llm.Request for
// the next Generate, including the configured tool defs (or those advertised by
// the runtime) and the optional output schema.
func (l *Loop) buildRequest(ctx context.Context, st *runState) (llm.Request, error) {
	events, err := l.deps.EventLog.Load(ctx, st.sessionID, 0)
	if err != nil {
		return llm.Request{}, fmt.Errorf("agent: load window: %w", err)
	}
	win, err := agentctx.BuildWindow(events, agentctx.WindowOptions{})
	if err != nil {
		return llm.Request{}, fmt.Errorf("agent: build window: %w", err)
	}

	tools := l.cfg.ToolDefs
	if len(tools) == 0 {
		tools = l.toolDefsFromRuntime(ctx, st.sessionID)
	}

	return llm.Request{
		Model:        l.cfg.Model,
		System:       win.System,
		Messages:     win.Messages,
		Tools:        tools,
		Stream:       true,
		OutputSchema: append([]byte(nil), l.cfg.OutputSchema...),
	}, nil
}

// toolDefsFromRuntime derives llm.ToolDef tool definitions from the runtime's
// advertised descriptors so the model is told which tools it may call. On a
// listing error it returns nil (no tools) rather than failing the turn.
func (l *Loop) toolDefsFromRuntime(ctx context.Context, sessionID string) []llm.ToolDef {
	descs, err := l.deps.Tools.ListTools(ctx, sessionID)
	if err != nil || len(descs) == 0 {
		return nil
	}
	out := make([]llm.ToolDef, 0, len(descs))
	for _, d := range descs {
		out = append(out, llm.ToolDef{Name: d.Name, Description: d.Description, JSONSchema: d.JSONSchema})
	}
	return out
}

// concatText concatenates all text content parts of a message in order.
func concatText(m llm.Message) string {
	var s string
	for _, p := range m.Content {
		if p.Text != nil {
			s += p.Text.Text
		}
	}
	return s
}

// forwardingReader is a llm.StreamReader decorator that forwards each text and
// thinking delta to the client sink as Recv yields it, while passing the event
// through unchanged to the pure assembler. It owns nothing: Close delegates to
// the inner reader (which Assemble calls exactly once).
type forwardingReader struct {
	inner     llm.StreamReader
	sink      ClientSink
	sessionID string
	turnID    string
}

// Recv returns the next event from the inner reader, forwarding text/thinking
// fragments to the sink before returning.
func (r *forwardingReader) Recv() (llm.StreamEvent, error) {
	ev, err := r.inner.Recv()
	if err != nil {
		return ev, err
	}
	switch {
	case ev.TextDelta != nil:
		r.sink.OnTextDelta(r.sessionID, r.turnID, ev.TextDelta.Text)
	case ev.ThinkingDelta != nil && ev.ThinkingDelta.Text != "":
		r.sink.OnThinkingDelta(r.sessionID, r.turnID, ev.ThinkingDelta.Text)
	}
	return ev, nil
}

// Close delegates to the inner reader.
func (r *forwardingReader) Close() error { return r.inner.Close() }

// compile-time guard: forwardingReader is a llm.StreamReader.
var _ llm.StreamReader = (*forwardingReader)(nil)

// errToolStream is returned when a tool stream itself errors at the transport
// level (distinct from a tool reporting is_error in its result).
var errToolStream = errors.New("agent: tool stream")
