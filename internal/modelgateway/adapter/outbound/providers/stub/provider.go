package stub

import (
	"context"
	"io"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// Compile-time assertion that Provider satisfies the kernel contract.
var _ llm.Provider = (*Provider)(nil)

// fixedUsage is the believable-but-fake [llm.Usage] every stub response reports.
// The numbers are large enough to exercise cost-reporting code paths without
// triggering zero-value edge cases.
var fixedUsage = llm.Usage{
	InputTokens:  512,
	OutputTokens: 64,
}

// fixedCountTokens is the token count returned by [Provider.CountTokens].
const fixedCountTokens = 512

// stubText is the single deterministic text turn the stub replies with. It is a
// terminal (StopEnd) text response — the stub NEVER requests a tool. This is a
// deliberate design choice for the keyless demo path (DOD-05): the orchestrator
// always advertises the tool set to the model, but a tool call in an unattended
// `harnessctl run` would block forever on the human approval gate (the default
// permission mode asks for every tool and a headless run has no approver). A
// clean text-only terminal turn lets the deployed stack run a task end-to-end
// keyless and deterministically reach termination Success — proving the full
// distributed pipeline (mTLS, event log, model-gateway round-trip, the
// orchestrator↔tool-runtime ListTools advertisement, streaming, and terminal
// RunResult delivery). The tool EXECUTION path is proven separately and
// exhaustively by the tool-runtime integration suite (sandbox + dedup), not by
// this network-free demo provider.
const stubText = "I received your task and I am working on it."

// Provider is the built-in deterministic test/demo [llm.Provider]. It streams a
// scripted response without contacting any external API. See package doc for full
// behavior description.
//
// Provider is safe for concurrent use: it carries no mutable state.
//
// WARNING: stub provider — NOT for production. For local demo, CI smoke, DOD-05.
type Provider struct{}

// New returns a stub [llm.Provider]. No configuration is required or accepted.
//
// WARNING: stub provider — NOT for production.
func New() *Provider { return &Provider{} }

// Stream returns a [llm.StreamReader] that replays the deterministic stub script.
// The script is always the same terminal, text-only turn (it never requests a
// tool — see [stubText] for why):
//
//  1. TextDelta: the [stubText] acknowledgement.
//  2. Done: StopEnd + fixedUsage.
//
// The context is not used after Stream returns; the reader is synchronous.
func (p *Provider) Stream(_ context.Context, req llm.Request) (llm.StreamReader, error) {
	return newScriptReader(req), nil
}

// Generate returns a non-streaming [llm.Response] built from the same script as
// [Provider.Stream].
func (p *Provider) Generate(_ context.Context, req llm.Request) (*llm.Response, error) {
	return buildResponse(req), nil
}

// CountTokens returns a fixed estimate (fixedCountTokens = 512). The stub does
// not have a real tokenizer; the value is large enough to exercise cost and
// capability paths.
func (p *Provider) CountTokens(_ context.Context, _ llm.Request) (int, error) {
	return fixedCountTokens, nil
}

// Capabilities returns a conservative capability set suitable for smoke tests.
// SupportsTools stays true so a tool-equipped request is accepted and the loop
// still advertises the runtime's tools (exercising the orchestrator↔tool-runtime
// ListTools path); the stub simply elects not to CALL any (see [stubText]).
func (p *Provider) Capabilities(_ context.Context, _ string) (llm.Capabilities, error) {
	return llm.Capabilities{
		SupportsTools:              true,
		SupportsParallelToolCalls:  false,
		SupportsStreamingToolCalls: true,
		SupportsVision:             false,
		SupportsSystemPrompt:       true,
		SupportsThinking:           false,
		SupportsTokenCounting:      true,
		SupportsJSONSchemaStrict:   false,
		MaxOutputTokens:            4096,
	}, nil
}

// buildScript constructs the ordered []llm.StreamEvent sequence for req. The
// script is invariant of req (in particular it ignores req.Tools): a single
// text delta followed by a terminal StopEnd Done. See [stubText] for the
// rationale (a tool call would deadlock an unattended, approver-less run).
func buildScript(_ llm.Request) []llm.StreamEvent {
	return []llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: stubText}},
		{Done: &llm.Done{
			StopReason:    llm.StopEnd,
			RawStopReason: string(llm.StopEnd),
			Usage:         fixedUsage,
		}},
	}
}

// buildResponse builds a non-streaming [llm.Response] from the stub script: a
// single text part with a terminal StopEnd (no tool call; see [stubText]).
func buildResponse(_ llm.Request) *llm.Response {
	return &llm.Response{
		Content:       []llm.ContentPart{{Text: &llm.TextPart{Text: stubText}}},
		StopReason:    llm.StopEnd,
		RawStopReason: string(llm.StopEnd),
		Usage:         fixedUsage,
	}
}

// scriptReader implements [llm.StreamReader] over a pre-built event slice.
type scriptReader struct {
	events []llm.StreamEvent
	pos    int
}

// newScriptReader constructs a scriptReader for req using [buildScript].
func newScriptReader(req llm.Request) *scriptReader {
	return &scriptReader{events: buildScript(req)}
}

// Recv returns the next event in the script. After the last event it returns
// [io.EOF] on every subsequent call.
func (r *scriptReader) Recv() (llm.StreamEvent, error) {
	if r.pos >= len(r.events) {
		return llm.StreamEvent{}, io.EOF
	}
	ev := r.events[r.pos]
	r.pos++
	return ev, nil
}

// Close is a no-op for the stub; there are no resources to release.
func (r *scriptReader) Close() error { return nil }

// Ensure scriptReader satisfies [llm.StreamReader] at compile time.
var _ llm.StreamReader = (*scriptReader)(nil)
