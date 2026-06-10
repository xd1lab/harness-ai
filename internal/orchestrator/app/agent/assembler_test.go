package agent_test

import (
	"encoding/json"
	"errors"
	"go/parser"
	"go/token"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/orchestrator/app/agent"
	"github.com/boltrope/boltrope/internal/platform/llm"
	"github.com/boltrope/boltrope/internal/platform/llm/llmtest"
)

// textParts extracts the concatenated text from all TextPart content parts of a
// message, in order, so tests can assert reassembled text without caring how the
// assembler chunked the parts.
func textOf(t *testing.T, m llm.Message) string {
	t.Helper()
	var s string
	for _, p := range m.Content {
		if p.Text != nil {
			s += p.Text.Text
		}
	}
	return s
}

// thinkingOf concatenates all thinking-part text in order.
func thinkingOf(t *testing.T, m llm.Message) string {
	t.Helper()
	var s string
	for _, p := range m.Content {
		if p.Thinking != nil {
			s += p.Thinking.Text
		}
	}
	return s
}

// toolCallsOf returns the ToolCall content parts in message order.
func toolCallsOf(t *testing.T, m llm.Message) []*llm.ToolCall {
	t.Helper()
	var calls []*llm.ToolCall
	for _, p := range m.Content {
		if p.ToolCall != nil {
			calls = append(calls, p.ToolCall)
		}
	}
	return calls
}

// ---------------------------------------------------------------------------
// Adversarial sequence: split mid-UTF-8 TextDelta.
// ---------------------------------------------------------------------------

// TestAssemble_SplitMidUTF8Rune asserts that a multi-byte rune ("世") split
// across two TextDelta fragments reassembles into the correct string and is not
// corrupted. This is the canonical adversarial delta from FR-MODEL-02 / §4.3.
func TestAssemble_SplitMidUTF8Rune(t *testing.T) {
	full := "héllo 世界" // contains multi-byte runes
	b := []byte(full)
	// Split at a byte boundary that lands in the middle of a rune.
	split := 8 // mid "世" (世 is 3 bytes starting at byte 7)
	first := string(b[:split])
	second := string(b[split:])

	reader := llmtest.NewFakeStreamReader(
		llm.StreamEvent{TextDelta: &llm.TextDelta{Text: first}},
		llm.StreamEvent{TextDelta: &llm.TextDelta{Text: second}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopEnd}},
	)

	res, err := agent.Assemble(reader)
	require.NoError(t, err)

	assert.Equal(t, llm.RoleAssistant, res.Message.Role)
	assert.Equal(t, full, textOf(t, res.Message), "split-rune text must reassemble byte-faithfully")
	assert.Equal(t, agent.OutcomeFinal, res.Outcome)
	assert.Equal(t, llm.StopEnd, res.Done.StopReason)
}

// ---------------------------------------------------------------------------
// Adversarial sequence: interleaved, out-of-order ToolCallDelta CallIDs
// accumulated by opaque CallID (NOT index).
// ---------------------------------------------------------------------------

// TestAssemble_InterleavedOutOfOrderToolCalls feeds append-style argument
// fragments for two distinct CallIDs interleaved out of order and asserts each
// call's arguments are concatenated correctly and ordered by FIRST appearance of
// the CallID (not by arrival order of the final fragment).
func TestAssemble_InterleavedOutOfOrderToolCalls(t *testing.T) {
	// callB appears first (so it must order first), but its fragments interleave
	// with callA's and arrive out of order.
	reader := llmtest.NewFakeStreamReader(
		// First sighting: B (name only).
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "B", Name: "write"}},
		// Then A (name only) — A is seen second.
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "A", Name: "bash"}},
		// Interleaved append fragments, out of order across calls.
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "A", ArgsFragment: json.RawMessage(`{"cmd":"l`)}},
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "B", ArgsFragment: json.RawMessage(`{"path":"/tmp`)}},
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "A", ArgsFragment: json.RawMessage(`s -la"}`)}},
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "B", ArgsFragment: json.RawMessage(`/x"}`)}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopToolUse}},
	)

	res, err := agent.Assemble(reader)
	require.NoError(t, err)

	assert.Equal(t, agent.OutcomeNeedsToolExecution, res.Outcome)
	assert.Equal(t, llm.StopToolUse, res.Done.StopReason)

	calls := toolCallsOf(t, res.Message)
	require.Len(t, calls, 2, "two distinct CallIDs -> two tool calls")

	// Ordered by first appearance: B then A.
	assert.Equal(t, "B", calls[0].ID)
	assert.Equal(t, "write", calls[0].Name)
	assert.Equal(t, map[string]any{"path": "/tmp/x"}, calls[0].Args)

	assert.Equal(t, "A", calls[1].ID)
	assert.Equal(t, "bash", calls[1].Name)
	assert.Equal(t, map[string]any{"cmd": "ls -la"}, calls[1].Args)
}

// ---------------------------------------------------------------------------
// Adversarial sequence: complete buffered ToolCallDelta (full args in one frag).
// ---------------------------------------------------------------------------

// TestAssemble_BufferedCompleteToolCall covers the buffered case
// (SupportsStreamingToolCalls=false): the whole call arrives as ONE ToolCallDelta
// with CallID + Name + full args in ArgsFragment, ArgsPath empty. It must parse
// into a single complete ToolCall.
func TestAssemble_BufferedCompleteToolCall(t *testing.T) {
	reader := llmtest.NewFakeStreamReader(
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{
			CallID:       "only",
			Name:         "search",
			ArgsFragment: json.RawMessage(`{"q":"golang","n":3}`),
		}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopToolUse}},
	)

	res, err := agent.Assemble(reader)
	require.NoError(t, err)

	calls := toolCallsOf(t, res.Message)
	require.Len(t, calls, 1)
	assert.Equal(t, "only", calls[0].ID)
	assert.Equal(t, "search", calls[0].Name)
	assert.Equal(t, map[string]any{"q": "golang", "n": float64(3)}, calls[0].Args)
	assert.Equal(t, agent.OutcomeNeedsToolExecution, res.Outcome)
}

// ---------------------------------------------------------------------------
// Adversarial sequence: path-addressed (Gemini-style) ToolCallDelta fragments.
// ---------------------------------------------------------------------------

// TestAssemble_PathAddressedFragments covers ArgsPath set-at-path fragments
// (Gemini jsonPath). Although the gateway often buffers these, the assembler must
// still handle path fragments arriving for the same call and produce the correct
// parsed object.
func TestAssemble_PathAddressedFragments(t *testing.T) {
	reader := llmtest.NewFakeStreamReader(
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "g1", Name: "edit"}},
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "g1", ArgsPath: "file", ArgsFragment: json.RawMessage(`"main.go"`)}},
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "g1", ArgsPath: "line", ArgsFragment: json.RawMessage(`42`)}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopToolUse}},
	)

	res, err := agent.Assemble(reader)
	require.NoError(t, err)

	calls := toolCallsOf(t, res.Message)
	require.Len(t, calls, 1)
	assert.Equal(t, "g1", calls[0].ID)
	assert.Equal(t, "edit", calls[0].Name)
	assert.Equal(t, map[string]any{"file": "main.go", "line": float64(42)}, calls[0].Args)
}

// ---------------------------------------------------------------------------
// Adversarial sequence: Pause reported via Done (NON-terminal) -> continuation.
// ---------------------------------------------------------------------------

// TestAssemble_PauseIsNeedsContinuation asserts a Pause stop reason (carried on
// Done with the continuation blob in ProviderRaw) classifies as
// OutcomeNeedsContinuation, never final, and surfaces ProviderRaw byte-faithfully.
func TestAssemble_PauseIsNeedsContinuation(t *testing.T) {
	raw := json.RawMessage(`{"server_tool_use":"opaque-state"}`)
	reader := llmtest.NewFakeStreamReader(
		llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "partial"}},
		llm.StreamEvent{Done: &llm.Done{
			StopReason:  llm.Pause,
			Usage:       llm.Usage{InputTokens: 10, OutputTokens: 5},
			ProviderRaw: raw,
		}},
	)

	res, err := agent.Assemble(reader)
	require.NoError(t, err)

	assert.Equal(t, agent.OutcomeNeedsContinuation, res.Outcome)
	assert.False(t, res.Done.StopReason.IsTerminal(), "Pause must be non-terminal")
	assert.Equal(t, "partial", textOf(t, res.Message))
	assert.JSONEq(t, string(raw), string(res.Done.ProviderRaw), "ProviderRaw surfaced byte-faithfully")
	assert.Equal(t, llm.Usage{InputTokens: 10, OutputTokens: 5}, res.Done.Usage)
}

// ---------------------------------------------------------------------------
// Adversarial sequence: duplicate / late Done after the first terminal Done.
// ---------------------------------------------------------------------------

// TestAssemble_DuplicateLateDoneIgnored asserts the FIRST Done is authoritative;
// a second/late Done (and any events after it) do not corrupt the assembled
// outcome. The assembler stops at the first terminal event.
func TestAssemble_DuplicateLateDoneIgnored(t *testing.T) {
	reader := llmtest.NewFakeStreamReader(
		llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "hi"}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopEnd, Usage: llm.Usage{OutputTokens: 1}}},
		// Late/duplicate events that must be ignored:
		llm.StreamEvent{TextDelta: &llm.TextDelta{Text: " IGNORED"}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopMaxTokens, Usage: llm.Usage{OutputTokens: 999}}},
	)

	res, err := agent.Assemble(reader)
	require.NoError(t, err)

	assert.Equal(t, "hi", textOf(t, res.Message), "text after first Done must be ignored")
	assert.Equal(t, agent.OutcomeFinal, res.Outcome)
	assert.Equal(t, llm.StopEnd, res.Done.StopReason, "first Done is authoritative")
	assert.Equal(t, 1, res.Done.Usage.OutputTokens)
}

// ---------------------------------------------------------------------------
// Thinking + text + tool-call ordering in one stream (golden ordering).
// ---------------------------------------------------------------------------

// TestAssemble_ThinkingTextToolCallOrdering asserts the assembled message keeps
// the natural ordering thinking -> text -> tool calls, with each kind collapsed
// into part(s) in arrival order.
func TestAssemble_ThinkingTextToolCallOrdering(t *testing.T) {
	reader := llmtest.NewFakeStreamReader(
		llm.StreamEvent{ThinkingDelta: &llm.ThinkingDelta{Text: "let me "}},
		llm.StreamEvent{ThinkingDelta: &llm.ThinkingDelta{Text: "think", Signature: "sig-"}},
		llm.StreamEvent{ThinkingDelta: &llm.ThinkingDelta{Signature: "xyz"}},
		llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "Running a command."}},
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "t1", Name: "bash", ArgsFragment: json.RawMessage(`{}`)}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopToolUse}},
	)

	res, err := agent.Assemble(reader)
	require.NoError(t, err)

	m := res.Message
	require.NotEmpty(t, m.Content)

	// Ordering: thinking part(s) first, then text, then tool call.
	assert.NotNil(t, m.Content[0].Thinking, "first part is thinking")
	assert.Equal(t, "let me think", thinkingOf(t, m))
	assert.Equal(t, "sig-xyz", m.Content[0].Thinking.Signature, "thinking signature fragments concatenate")

	assert.Equal(t, "Running a command.", textOf(t, m))

	calls := toolCallsOf(t, m)
	require.Len(t, calls, 1)
	assert.Equal(t, "bash", calls[0].Name)

	// thinking index < text index < tool-call index.
	var iThink, iText, iCall = -1, -1, -1
	for i, p := range m.Content {
		switch {
		case p.Thinking != nil && iThink == -1:
			iThink = i
		case p.Text != nil && iText == -1:
			iText = i
		case p.ToolCall != nil && iCall == -1:
			iCall = i
		}
	}
	assert.Less(t, iThink, iText)
	assert.Less(t, iText, iCall)
}

// ---------------------------------------------------------------------------
// Empty-args tool call: a ToolCallDelta with name only, no args fragment.
// ---------------------------------------------------------------------------

// TestAssemble_ToolCallNoArgs asserts a tool call with no argument fragments
// yields a non-nil empty Args map (callable with zero arguments), not a nil map.
func TestAssemble_ToolCallNoArgs(t *testing.T) {
	reader := llmtest.NewFakeStreamReader(
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "n1", Name: "now"}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopToolUse}},
	)

	res, err := agent.Assemble(reader)
	require.NoError(t, err)

	calls := toolCallsOf(t, res.Message)
	require.Len(t, calls, 1)
	assert.Equal(t, "now", calls[0].Name)
	require.NotNil(t, calls[0].Args, "empty args must be a non-nil map")
	assert.Empty(t, calls[0].Args)
}

// ---------------------------------------------------------------------------
// Stop-reason classification table.
// ---------------------------------------------------------------------------

// TestAssemble_OutcomeClassification is a table over stop reasons asserting the
// three-way outcome classification: Pause -> needs-continuation; StopToolUse with
// tool calls -> needs-tool-execution; everything else terminal -> final.
func TestAssemble_OutcomeClassification(t *testing.T) {
	tests := []struct {
		name     string
		stop     llm.StopReason
		withCall bool
		want     agent.Outcome
	}{
		{"end", llm.StopEnd, false, agent.OutcomeFinal},
		{"max_tokens", llm.StopMaxTokens, false, agent.OutcomeFinal},
		{"refusal", llm.StopRefusal, false, agent.OutcomeFinal},
		{"content_filter", llm.StopContentFilter, false, agent.OutcomeFinal},
		{"context_window", llm.StopContextWindowExceeded, false, agent.OutcomeFinal},
		{"stop_sequence", llm.StopStopSequence, false, agent.OutcomeFinal},
		{"other", llm.StopOther, false, agent.OutcomeFinal},
		{"pause", llm.Pause, false, agent.OutcomeNeedsContinuation},
		{"tool_use_with_call", llm.StopToolUse, true, agent.OutcomeNeedsToolExecution},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var events []llm.StreamEvent
			if tc.withCall {
				events = append(events, llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "c", Name: "x", ArgsFragment: json.RawMessage(`{}`)}})
			}
			events = append(events, llm.StreamEvent{Done: &llm.Done{StopReason: tc.stop}})
			reader := llmtest.NewFakeStreamReader(events...)

			res, err := agent.Assemble(reader)
			require.NoError(t, err)
			assert.Equal(t, tc.want, res.Outcome)
		})
	}
}

// TestAssemble_ToolUseStopWithoutCalls asserts that a StopToolUse reason that
// arrives with NO accumulated tool calls does not falsely report
// needs-tool-execution (there is nothing to execute) — it falls back to final so
// the loop terminates rather than dispatching an empty tool batch.
func TestAssemble_ToolUseStopWithoutCalls(t *testing.T) {
	reader := llmtest.NewFakeStreamReader(
		llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "no tools here"}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopToolUse}},
	)
	res, err := agent.Assemble(reader)
	require.NoError(t, err)
	assert.Equal(t, agent.OutcomeFinal, res.Outcome)
}

// ---------------------------------------------------------------------------
// Error propagation and stream-end handling.
// ---------------------------------------------------------------------------

// TestAssemble_MidStreamErrorPropagates asserts a non-EOF error from Recv is
// returned to the caller (wrapped), and the partial message accumulated so far is
// still returned so the loop can checkpoint usage_so_far.
func TestAssemble_MidStreamErrorPropagates(t *testing.T) {
	boom := &llm.ProviderError{Kind: llm.ErrServer, Raw: errors.New("boom")}
	reader := &erroringReader{
		events: []llm.StreamEvent{
			{TextDelta: &llm.TextDelta{Text: "before"}},
		},
		err: boom,
	}

	res, err := agent.Assemble(reader)
	require.Error(t, err)
	var pe *llm.ProviderError
	require.ErrorAs(t, err, &pe, "underlying ProviderError must be unwrappable")
	assert.Equal(t, "before", textOf(t, res.Message), "partial text returned alongside error")
}

// TestAssemble_EOFWithoutDone asserts that a stream that ends with io.EOF but no
// Done event returns a typed error (the provider truncated the stream), with the
// partial message preserved.
func TestAssemble_EOFWithoutDone(t *testing.T) {
	reader := llmtest.NewFakeStreamReader(
		llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "truncated"}},
		// no Done
	)
	res, err := agent.Assemble(reader)
	require.Error(t, err)
	assert.ErrorIs(t, err, agent.ErrIncompleteStream)
	assert.Equal(t, "truncated", textOf(t, res.Message))
}

// TestAssemble_ClosesReader asserts the assembler closes the reader exactly once
// when it owns the stream lifecycle (architecture §4.3: "Recv in a loop and Close
// when done").
func TestAssemble_ClosesReader(t *testing.T) {
	reader := &countingReader{
		inner: llmtest.NewFakeStreamReader(
			llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopEnd}},
		),
	}
	_, err := agent.Assemble(reader)
	require.NoError(t, err)
	assert.Equal(t, 1, reader.closeCount, "reader closed exactly once")
}

// TestAssemble_InvalidArgsJSONErrors asserts that an un-parseable accumulated
// argument buffer surfaces a typed error rather than a silently-empty Args map,
// so a malformed provider stream is never mistaken for a zero-arg call.
func TestAssemble_InvalidArgsJSONErrors(t *testing.T) {
	reader := llmtest.NewFakeStreamReader(
		llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{CallID: "bad", Name: "x", ArgsFragment: json.RawMessage(`{"k":`)}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopToolUse}},
	)
	_, err := agent.Assemble(reader)
	require.Error(t, err)
	assert.ErrorIs(t, err, agent.ErrMalformedToolArgs)
}

// ---------------------------------------------------------------------------
// Test doubles for error / lifecycle behavior the scripted reader can't express.
// ---------------------------------------------------------------------------

// erroringReader yields its events, then returns err (a non-EOF error).
type erroringReader struct {
	events []llm.StreamEvent
	pos    int
	err    error
	closed bool
}

func (r *erroringReader) Recv() (llm.StreamEvent, error) {
	if r.pos < len(r.events) {
		ev := r.events[r.pos]
		r.pos++
		return ev, nil
	}
	return llm.StreamEvent{}, r.err
}

func (r *erroringReader) Close() error { r.closed = true; return nil }

// countingReader counts Close calls and delegates Recv to an inner reader.
type countingReader struct {
	inner      llm.StreamReader
	closeCount int
}

func (r *countingReader) Recv() (llm.StreamEvent, error) { return r.inner.Recv() }
func (r *countingReader) Close() error {
	r.closeCount++
	return r.inner.Close()
}

// sanity: io is used (EOF reference in helper docs); keep import meaningful.
var _ = io.EOF
var _ = errors.Is

// ---------------------------------------------------------------------------
// Purity assertion: assembler.go imports NO gen/ package and no provider SDK.
// ---------------------------------------------------------------------------

// TestAssembler_NoGenOrSDKImports parses assembler.go's import block and asserts
// it imports nothing from gen/ and no provider SDK (FR-MODEL-02 AC-3, DOD-08).
// This is the in-package complement to the repo-wide depguard rule: the assembler
// is the pure delta->Message boundary and must never reach into wire types.
func TestAssembler_NoGenOrSDKImports(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "assembler.go", nil, parser.ImportsOnly)
	require.NoError(t, err)

	forbidden := []string{
		"github.com/boltrope/boltrope/gen",
		"google.golang.org/grpc",
		"github.com/anthropics/anthropic-sdk-go",
		"google.golang.org/genai",
		"github.com/openai/openai-go",
	}

	for _, imp := range f.Imports {
		path, perr := strconv.Unquote(imp.Path.Value)
		require.NoError(t, perr)
		for _, bad := range forbidden {
			assert.False(t,
				path == bad || strings.HasPrefix(path, bad+"/"),
				"assembler.go must not import %q (got %q): FR-MODEL-02 AC-3 / DOD-08", bad, path)
		}
	}
}
