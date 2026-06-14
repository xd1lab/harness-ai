package gemini

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"testing"

	"google.golang.org/genai"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// compile-time assertion that Provider implements llm.Provider.
var _ llm.Provider = (*Provider)(nil)

// fakeModels is an in-memory stand-in for the genai Models service implementing the
// internal modelsAPI seam, so Generate / CountTokens are tested network-free.
type fakeModels struct {
	genResp    *genai.GenerateContentResponse
	genErr     error
	streamSeq  []*genai.GenerateContentResponse
	streamErr  error
	countResp  *genai.CountTokensResponse
	countErr   error
	lastModel  string
	lastConfig *genai.GenerateContentConfig
	lastConts  []*genai.Content
}

func (f *fakeModels) GenerateContent(_ context.Context, model string, contents []*genai.Content, config *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
	f.lastModel, f.lastConts, f.lastConfig = model, contents, config
	if f.genErr != nil {
		return nil, f.genErr
	}
	return f.genResp, nil
}

func (f *fakeModels) GenerateContentStream(_ context.Context, model string, contents []*genai.Content, config *genai.GenerateContentConfig) iter.Seq2[*genai.GenerateContentResponse, error] {
	f.lastModel, f.lastConts, f.lastConfig = model, contents, config
	return func(yield func(*genai.GenerateContentResponse, error) bool) {
		if f.streamErr != nil {
			yield(nil, f.streamErr)
			return
		}
		for _, c := range f.streamSeq {
			if !yield(c, nil) {
				return
			}
		}
	}
}

func (f *fakeModels) CountTokens(_ context.Context, model string, contents []*genai.Content, _ *genai.CountTokensConfig) (*genai.CountTokensResponse, error) {
	f.lastModel, f.lastConts = model, contents
	if f.countErr != nil {
		return nil, f.countErr
	}
	return f.countResp, nil
}

// newTestProvider builds a Provider backed by the supplied fake models seam.
func newTestProvider(m modelsAPI) *Provider {
	return &Provider{models: m}
}

func userMsg(text string) llm.Message {
	return llm.Message{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: text}}}}
}

// TestBuildContentsRoles asserts normalized roles map onto Gemini's user/model roles
// and that tool results are folded into a user-turn functionResponse part.
func TestBuildContentsRoles(t *testing.T) {
	t.Parallel()
	msgs := []llm.Message{
		userMsg("hello"),
		{Role: llm.RoleAssistant, Content: []llm.ContentPart{
			{Text: &llm.TextPart{Text: "thinking..."}},
			{ToolCall: &llm.ToolCall{ID: "c1", Name: "search", Args: map[string]any{"q": "go"}}},
		}},
		{Role: llm.RoleTool, Content: []llm.ContentPart{
			{ToolResult: &llm.ToolResult{CallID: "c1", Content: "result text"}},
		}},
	}
	conts, err := buildContents(msgs)
	if err != nil {
		t.Fatalf("buildContents error: %v", err)
	}
	if len(conts) != 3 {
		t.Fatalf("want 3 contents, got %d", len(conts))
	}
	if conts[0].Role != genai.RoleUser {
		t.Errorf("msg0 role = %q, want user", conts[0].Role)
	}
	if conts[1].Role != genai.RoleModel {
		t.Errorf("msg1 role = %q, want model", conts[1].Role)
	}
	// Assistant turn carries a text part and a functionCall part.
	if len(conts[1].Parts) != 2 || conts[1].Parts[1].FunctionCall == nil {
		t.Fatalf("assistant parts wrong: %+v", conts[1].Parts)
	}
	if conts[1].Parts[1].FunctionCall.Name != "search" {
		t.Errorf("functionCall name = %q, want search", conts[1].Parts[1].FunctionCall.Name)
	}
	// Tool result becomes a user-role functionResponse.
	if conts[2].Role != genai.RoleUser {
		t.Errorf("tool-result role = %q, want user (Gemini folds tool results into user)", conts[2].Role)
	}
	if len(conts[2].Parts) != 1 || conts[2].Parts[0].FunctionResponse == nil {
		t.Fatalf("tool-result part wrong: %+v", conts[2].Parts)
	}
	if conts[2].Parts[0].FunctionResponse.Name != "search" {
		t.Errorf("functionResponse name = %q, want search (matched by call id)", conts[2].Parts[0].FunctionResponse.Name)
	}
}

// TestBuildConfigSystemAndTools asserts System goes to SystemInstruction and that
// ToolDefs become functionDeclarations carrying the raw JSON schema.
func TestBuildConfigSystemAndTools(t *testing.T) {
	t.Parallel()
	temp := 0.5
	req := llm.Request{
		Model:       "gemini-3-pro",
		System:      "You are helpful.",
		MaxTokens:   256,
		Temperature: &temp,
		Tools: []llm.ToolDef{{
			Name:        "get_weather",
			Description: "Get the weather",
			JSONSchema:  json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`),
		}},
	}
	cfg, err := buildConfig(req, llm.Capabilities{})
	if err != nil {
		t.Fatalf("buildConfig error: %v", err)
	}
	if cfg.SystemInstruction == nil || len(cfg.SystemInstruction.Parts) == 0 ||
		cfg.SystemInstruction.Parts[0].Text != "You are helpful." {
		t.Errorf("SystemInstruction not set correctly: %+v", cfg.SystemInstruction)
	}
	if cfg.MaxOutputTokens != 256 {
		t.Errorf("MaxOutputTokens = %d, want 256", cfg.MaxOutputTokens)
	}
	if cfg.Temperature == nil || *cfg.Temperature != float32(0.5) {
		t.Errorf("Temperature = %v, want 0.5", cfg.Temperature)
	}
	if len(cfg.Tools) != 1 || len(cfg.Tools[0].FunctionDeclarations) != 1 {
		t.Fatalf("tools not mapped: %+v", cfg.Tools)
	}
	fd := cfg.Tools[0].FunctionDeclarations[0]
	if fd.Name != "get_weather" || fd.Description != "Get the weather" {
		t.Errorf("function declaration name/desc wrong: %+v", fd)
	}
	if fd.ParametersJsonSchema == nil {
		t.Errorf("ParametersJsonSchema is nil; raw JSON schema must be carried through")
	}
}

// TestBuildConfigNoSystemNoTools asserts an empty System / no Tools yields a config
// without a SystemInstruction or Tools (so we don't send empty envelopes).
func TestBuildConfigNoSystemNoTools(t *testing.T) {
	t.Parallel()
	cfg, err := buildConfig(llm.Request{Model: "gemini-3-pro"}, llm.Capabilities{})
	if err != nil {
		t.Fatalf("buildConfig error: %v", err)
	}
	if cfg.SystemInstruction != nil {
		t.Errorf("SystemInstruction should be nil when System is empty")
	}
	if len(cfg.Tools) != 0 {
		t.Errorf("Tools should be empty when no ToolDefs supplied")
	}
}

// TestBuildConfigToolChoice asserts ToolChoice maps onto a Gemini ToolConfig
// FunctionCallingConfig mode.
func TestBuildConfigToolChoice(t *testing.T) {
	t.Parallel()
	cases := []struct {
		choice llm.ToolChoice
		want   genai.FunctionCallingConfigMode
	}{
		{llm.ToolChoiceAuto, genai.FunctionCallingConfigModeAuto},
		{llm.ToolChoiceAny, genai.FunctionCallingConfigModeAny},
		{llm.ToolChoiceRequired, genai.FunctionCallingConfigModeAny},
		{llm.ToolChoiceNone, genai.FunctionCallingConfigModeNone},
	}
	for _, tc := range cases {
		req := llm.Request{
			Model:      "gemini-3-pro",
			ToolChoice: tc.choice,
			Tools:      []llm.ToolDef{{Name: "t", Description: "d", JSONSchema: json.RawMessage(`{"type":"object"}`)}},
		}
		cfg, err := buildConfig(req, llm.Capabilities{})
		if err != nil {
			t.Fatalf("buildConfig(%q) error: %v", tc.choice, err)
		}
		if cfg.ToolConfig == nil || cfg.ToolConfig.FunctionCallingConfig == nil {
			t.Fatalf("ToolConfig not set for choice %q", tc.choice)
		}
		if cfg.ToolConfig.FunctionCallingConfig.Mode != tc.want {
			t.Errorf("choice %q -> mode %q, want %q", tc.choice, cfg.ToolConfig.FunctionCallingConfig.Mode, tc.want)
		}
	}
}

// TestGenerate asserts a non-streaming Generate aggregates a genai response into the
// normalized llm.Response (content + stop reason + usage).
func TestGenerate(t *testing.T) {
	t.Parallel()
	fm := &fakeModels{
		genResp: &genai.GenerateContentResponse{
			Candidates: []*genai.Candidate{{
				Content: &genai.Content{Role: genai.RoleModel, Parts: []*genai.Part{
					{Text: "the answer"},
					{FunctionCall: &genai.FunctionCall{Name: "fin", Args: map[string]any{"ok": true}}},
				}},
				FinishReason: genai.FinishReasonStop,
			}},
			UsageMetadata: &genai.GenerateContentResponseUsageMetadata{
				PromptTokenCount:     5,
				CandidatesTokenCount: 9,
			},
		},
	}
	p := newTestProvider(fm)
	resp, err := p.Generate(context.Background(), llm.Request{Model: "gemini-3-pro", Messages: []llm.Message{userMsg("q")}})
	if err != nil {
		t.Fatalf("Generate error: %v", err)
	}
	if resp.StopReason != llm.StopEnd || resp.RawStopReason != "STOP" {
		t.Errorf("stop = %q/%q, want StopEnd/STOP", resp.StopReason, resp.RawStopReason)
	}
	if resp.Usage.InputTokens != 5 || resp.Usage.OutputTokens != 9 {
		t.Errorf("usage = %+v, want in=5 out=9", resp.Usage)
	}
	if len(resp.Content) != 2 {
		t.Fatalf("want 2 content parts, got %d: %+v", len(resp.Content), resp.Content)
	}
	if resp.Content[0].Text == nil || resp.Content[0].Text.Text != "the answer" {
		t.Errorf("content[0] = %+v, want text 'the answer'", resp.Content[0])
	}
	if resp.Content[1].ToolCall == nil || resp.Content[1].ToolCall.Name != "fin" {
		t.Errorf("content[1] = %+v, want tool call 'fin'", resp.Content[1])
	}
	if fm.lastModel != "gemini-3-pro" {
		t.Errorf("model passed to SDK = %q, want gemini-3-pro", fm.lastModel)
	}
}

// TestGenerateError asserts a provider APIError surfaces as a normalized
// *llm.ProviderError.
func TestGenerateError(t *testing.T) {
	t.Parallel()
	fm := &fakeModels{genErr: genai.APIError{Code: 429, Message: "slow down"}}
	p := newTestProvider(fm)
	_, err := p.Generate(context.Background(), llm.Request{Model: "gemini-3-pro"})
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("Generate err = %T, want *llm.ProviderError", err)
	}
	if pe.Kind != llm.ErrRateLimited {
		t.Errorf("kind = %q, want rate_limited", pe.Kind)
	}
}

// TestStream asserts Stream returns a StreamReader that yields normalized events
// terminated by io.EOF after the Done event.
func TestStream(t *testing.T) {
	t.Parallel()
	fm := &fakeModels{
		streamSeq: []*genai.GenerateContentResponse{
			textResp("Hi"),
			finalResp(genai.FinishReasonStop, &genai.GenerateContentResponseUsageMetadata{PromptTokenCount: 3, CandidatesTokenCount: 1}),
		},
	}
	p := newTestProvider(fm)
	sr, err := p.Stream(context.Background(), llm.Request{Model: "gemini-3-pro", Messages: []llm.Message{userMsg("q")}})
	if err != nil {
		t.Fatalf("Stream error: %v", err)
	}
	defer func() { _ = sr.Close() }()

	var events []llm.StreamEvent
	for {
		ev, rerr := sr.Recv()
		if errors.Is(rerr, errEOF) {
			break
		}
		if rerr != nil {
			t.Fatalf("Recv error: %v", rerr)
		}
		events = append(events, ev)
	}
	if len(events) != 2 {
		t.Fatalf("want 2 events (text+done), got %d: %+v", len(events), events)
	}
	if events[0].TextDelta == nil || events[0].TextDelta.Text != "Hi" {
		t.Errorf("event[0] = %+v, want text 'Hi'", events[0])
	}
	if events[1].Done == nil || events[1].Done.StopReason != llm.StopEnd {
		t.Errorf("event[1] = %+v, want Done StopEnd", events[1])
	}
	if events[1].Done.Usage.InputTokens != 3 {
		t.Errorf("done usage = %+v, want input 3", events[1].Done.Usage)
	}
}

// TestStreamMidStreamError asserts a mid-stream provider error surfaces from Recv as
// a normalized *llm.ProviderError.
func TestStreamMidStreamError(t *testing.T) {
	t.Parallel()
	fm := &fakeModels{streamErr: genai.APIError{Code: 503, Message: "unavailable"}}
	p := newTestProvider(fm)
	sr, err := p.Stream(context.Background(), llm.Request{Model: "gemini-3-pro"})
	if err != nil {
		t.Fatalf("Stream start error: %v", err)
	}
	defer func() { _ = sr.Close() }()
	_, rerr := sr.Recv()
	var pe *llm.ProviderError
	if !errors.As(rerr, &pe) {
		t.Fatalf("Recv err = %T, want *llm.ProviderError", rerr)
	}
	if pe.Kind != llm.ErrServer {
		t.Errorf("kind = %q, want server", pe.Kind)
	}
}

// TestCountTokens asserts CountTokens returns the SDK total and that token counting
// is reported as supported.
func TestCountTokens(t *testing.T) {
	t.Parallel()
	fm := &fakeModels{countResp: &genai.CountTokensResponse{TotalTokens: 42}}
	p := newTestProvider(fm)
	n, err := p.CountTokens(context.Background(), llm.Request{Model: "gemini-3-pro", Messages: []llm.Message{userMsg("count me")}})
	if err != nil {
		t.Fatalf("CountTokens error: %v", err)
	}
	if n != 42 {
		t.Errorf("CountTokens = %d, want 42", n)
	}
	caps, err := p.Capabilities(context.Background(), "gemini-3-pro")
	if err != nil {
		t.Fatalf("Capabilities error: %v", err)
	}
	if !caps.SupportsTokenCounting {
		t.Errorf("SupportsTokenCounting = false, want true")
	}
}

// TestCapabilities asserts the per-model capability flags are populated and that
// tool support is reported.
func TestCapabilities(t *testing.T) {
	t.Parallel()
	p := newTestProvider(&fakeModels{})
	caps, err := p.Capabilities(context.Background(), "gemini-3-pro")
	if err != nil {
		t.Fatalf("Capabilities error: %v", err)
	}
	if !caps.SupportsTools {
		t.Errorf("SupportsTools = false, want true")
	}
	if !caps.SupportsSystemPrompt {
		t.Errorf("SupportsSystemPrompt = false, want true")
	}
}
