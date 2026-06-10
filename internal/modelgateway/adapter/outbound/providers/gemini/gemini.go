// Package gemini implements the Boltrope [llm.Provider] abstraction over Google's
// google.golang.org/genai SDK (the Gemini Developer API backend). It is one of the
// model-gateway's outbound provider adapters: it maps the normalized request/response
// model onto the genai wire types, normalizes streamed responses into
// [llm.StreamEvent]s, classifies failures into [llm.ProviderError], and resolves
// per-model capabilities (ADR-0004; ADR-0016; architecture §5.2, §11).
//
// All provider-specific behavior lives here so the orchestrator stays
// provider-agnostic: stop-reason normalization, usage extraction, tool-call delta
// shaping, and error classification never leak past this package. The stream
// normalizer (see normalize.go) is isolated as a network-free, golden-testable
// function from genai response chunks to [llm.StreamEvent].
package gemini

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"math"

	"google.golang.org/genai"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// errEOF is the sentinel [llm.StreamReader.Recv] returns once a stream is exhausted
// after its terminal [llm.Done]. It aliases [io.EOF] so callers may compare with
// either errors.Is(err, io.EOF) or errors.Is(err, errEOF).
var errEOF = io.EOF

// modelsAPI is the narrow seam over the genai Models service the adapter depends on.
// Defining it here (rather than depending on the concrete *genai.Models) lets the
// adapter be unit-tested with an in-memory fake, network-free. The concrete
// *genai.Models satisfies it.
type modelsAPI interface {
	GenerateContent(ctx context.Context, model string, contents []*genai.Content, config *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error)
	GenerateContentStream(ctx context.Context, model string, contents []*genai.Content, config *genai.GenerateContentConfig) iter.Seq2[*genai.GenerateContentResponse, error]
	CountTokens(ctx context.Context, model string, contents []*genai.Content, config *genai.CountTokensConfig) (*genai.CountTokensResponse, error)
}

// Provider is the Gemini implementation of [llm.Provider]. It is stateless apart from
// the underlying genai client and is safe for concurrent use.
type Provider struct {
	models modelsAPI
}

// Config configures a Gemini [Provider].
type Config struct {
	// APIKey is the Gemini Developer API key. When empty, the SDK falls back to the
	// GEMINI_API_KEY / GOOGLE_API_KEY environment variables.
	APIKey string
}

// New constructs a Gemini [Provider] backed by a genai client on the Gemini Developer
// API backend (genai.BackendGeminiAPI). It returns a normalized [llm.ProviderError] if
// the client cannot be created.
func New(ctx context.Context, cfg Config) (*Provider, error) {
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		Backend: genai.BackendGeminiAPI,
		APIKey:  cfg.APIKey,
	})
	if err != nil {
		return nil, normalizeError(err)
	}
	return &Provider{models: client.Models}, nil
}

// Generate runs a single non-streaming generation and returns the aggregated
// normalized [llm.Response]. On failure it returns a [*llm.ProviderError].
func (p *Provider) Generate(ctx context.Context, req llm.Request) (*llm.Response, error) {
	contents, err := buildContents(req.Messages)
	if err != nil {
		return nil, &llm.ProviderError{Kind: llm.ErrInvalidRequest, Raw: err}
	}
	cfg, err := buildConfig(req)
	if err != nil {
		return nil, &llm.ProviderError{Kind: llm.ErrInvalidRequest, Raw: err}
	}

	resp, err := p.models.GenerateContent(ctx, req.Model, contents, cfg)
	if err != nil {
		return nil, normalizeError(err)
	}
	return aggregateResponse(resp), nil
}

// Stream runs a streaming generation and returns a [llm.StreamReader] of normalized
// [llm.StreamEvent]s terminated by a [llm.Done]. On failure to build the request it
// returns a [*llm.ProviderError]; mid-stream failures surface from Recv.
func (p *Provider) Stream(ctx context.Context, req llm.Request) (llm.StreamReader, error) {
	contents, err := buildContents(req.Messages)
	if err != nil {
		return nil, &llm.ProviderError{Kind: llm.ErrInvalidRequest, Raw: err}
	}
	cfg, err := buildConfig(req)
	if err != nil {
		return nil, &llm.ProviderError{Kind: llm.ErrInvalidRequest, Raw: err}
	}

	seq := p.models.GenerateContentStream(ctx, req.Model, contents, cfg)
	return newStreamReader(seq), nil
}

// CountTokens returns the input token count for req under its target model via the
// genai countTokens endpoint. On failure it returns a [*llm.ProviderError].
func (p *Provider) CountTokens(ctx context.Context, req llm.Request) (int, error) {
	contents, err := buildContents(req.Messages)
	if err != nil {
		return 0, &llm.ProviderError{Kind: llm.ErrInvalidRequest, Raw: err}
	}
	resp, err := p.models.CountTokens(ctx, req.Model, contents, nil)
	if err != nil {
		return 0, normalizeError(err)
	}
	return int(resp.TotalTokens), nil
}

// Capabilities returns the [llm.Capabilities] for the given Gemini model. Gemini
// models support tools, vision, a system instruction, streaming, thinking, and the
// countTokens endpoint; argument streaming for tool calls is NOT supported on the
// Gemini Developer API, so SupportsStreamingToolCalls is false and the gateway emits
// complete (buffered) tool calls (architecture §11.2, §11.4).
func (p *Provider) Capabilities(_ context.Context, _ string) (llm.Capabilities, error) {
	return llm.Capabilities{
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: false,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           true,
		SupportsTokenCounting:      true,
		SupportsJSONSchemaStrict:   false,
		MaxOutputTokens:            0,
	}, nil
}

// ---------------------------------------------------------------------------
// Request mapping
// ---------------------------------------------------------------------------

// buildContents maps the normalized conversation history onto Gemini's []*Content.
// Roles map as: RoleUser/RoleTool -> "user", RoleAssistant -> "model" (Gemini has only
// user/model roles, so tool results are folded into a user turn as functionResponse
// parts). Tool-call ids are tracked so each tool result's functionResponse is matched
// to the name of the call it answers.
func buildContents(msgs []llm.Message) ([]*genai.Content, error) {
	callNames := map[string]string{} // call id -> tool name, for matching tool results
	out := make([]*genai.Content, 0, len(msgs))

	for _, msg := range msgs {
		role := genai.RoleUser
		if msg.Role == llm.RoleAssistant {
			role = genai.RoleModel
		}
		parts, err := buildParts(msg, callNames)
		if err != nil {
			return nil, err
		}
		if len(parts) == 0 {
			continue
		}
		out = append(out, &genai.Content{Role: role, Parts: parts})
	}
	return out, nil
}

// buildParts maps one message's content parts onto Gemini []*Part, recording tool-call
// names in callNames so a later tool result can be matched by call id.
func buildParts(msg llm.Message, callNames map[string]string) ([]*genai.Part, error) {
	parts := make([]*genai.Part, 0, len(msg.Content))
	for _, cp := range msg.Content {
		switch {
		case cp.Text != nil:
			parts = append(parts, &genai.Part{Text: cp.Text.Text})

		case cp.Thinking != nil:
			parts = append(parts, &genai.Part{
				Text:             cp.Thinking.Text,
				Thought:          true,
				ThoughtSignature: signatureBytes(cp.Thinking.Signature),
			})

		case cp.Image != nil:
			part, err := imagePart(cp.Image)
			if err != nil {
				return nil, err
			}
			parts = append(parts, part)

		case cp.ToolCall != nil:
			callNames[cp.ToolCall.ID] = cp.ToolCall.Name
			parts = append(parts, &genai.Part{FunctionCall: &genai.FunctionCall{
				ID:   cp.ToolCall.ID,
				Name: cp.ToolCall.Name,
				Args: cp.ToolCall.Args,
			}})

		case cp.ToolResult != nil:
			parts = append(parts, toolResultPart(cp.ToolResult, callNames))
		}
	}
	return parts, nil
}

// toolResultPart maps a normalized [llm.ToolResult] onto a Gemini functionResponse
// part. The result text is placed under the "output" key (or "error" when the tool
// reported a failure) per the Gemini functionResponse convention. The function name is
// recovered from callNames by the matching call id.
func toolResultPart(tr *llm.ToolResult, callNames map[string]string) *genai.Part {
	key := "output"
	if tr.IsError {
		key = "error"
	}
	return &genai.Part{FunctionResponse: &genai.FunctionResponse{
		ID:       tr.CallID,
		Name:     callNames[tr.CallID],
		Response: map[string]any{key: tr.Content},
	}}
}

// imagePart maps a normalized [llm.ImagePart] onto a Gemini Part. Inline bytes become
// an inlineData Blob; a remote URL or provider file ref becomes a fileData reference.
func imagePart(img *llm.ImagePart) (*genai.Part, error) {
	switch {
	case len(img.Data) > 0:
		return &genai.Part{InlineData: &genai.Blob{
			Data:     img.Data,
			MIMEType: img.MediaType,
		}}, nil
	case img.URL != "":
		return &genai.Part{FileData: &genai.FileData{
			FileURI:  img.URL,
			MIMEType: img.MediaType,
		}}, nil
	case img.FileRef != "":
		return &genai.Part{FileData: &genai.FileData{
			FileURI:  img.FileRef,
			MIMEType: img.MediaType,
		}}, nil
	default:
		return nil, fmt.Errorf("gemini: image part has no data, URL, or file ref")
	}
}

// signatureBytes converts an opaque thinking-signature string back to the byte form
// the genai SDK carries on a Part. Empty in -> nil.
func signatureBytes(sig string) []byte {
	if sig == "" {
		return nil
	}
	return []byte(sig)
}

// buildConfig maps the normalized [llm.Request] fields (System, Tools, ToolChoice,
// MaxTokens, Temperature) onto a *genai.GenerateContentConfig.
func buildConfig(req llm.Request) (*genai.GenerateContentConfig, error) {
	cfg := &genai.GenerateContentConfig{}

	if req.System != "" {
		cfg.SystemInstruction = &genai.Content{
			Role:  genai.RoleUser,
			Parts: []*genai.Part{{Text: req.System}},
		}
	}
	if req.MaxTokens > 0 {
		mt := req.MaxTokens
		if mt > math.MaxInt32 {
			mt = math.MaxInt32
		}
		cfg.MaxOutputTokens = int32(mt)
	}
	if req.Temperature != nil {
		t := float32(*req.Temperature)
		cfg.Temperature = &t
	}

	tools, err := buildTools(req.Tools)
	if err != nil {
		return nil, err
	}
	cfg.Tools = tools

	if tc := toolConfig(req.ToolChoice); tc != nil {
		cfg.ToolConfig = tc
	}

	return cfg, nil
}

// buildTools maps normalized [llm.ToolDef]s onto a single Gemini Tool carrying the
// functionDeclarations. The raw JSON schema is carried verbatim via ParametersJsonSchema
// (genai accepts JSON Schema there directly), so the schema is not re-encoded. An empty
// tool set returns nil so no tools envelope is sent.
func buildTools(defs []llm.ToolDef) ([]*genai.Tool, error) {
	if len(defs) == 0 {
		return nil, nil
	}
	decls := make([]*genai.FunctionDeclaration, 0, len(defs))
	for _, d := range defs {
		fd := &genai.FunctionDeclaration{
			Name:        d.Name,
			Description: d.Description,
		}
		if len(d.JSONSchema) > 0 {
			var schema any
			if err := json.Unmarshal(d.JSONSchema, &schema); err != nil {
				return nil, fmt.Errorf("gemini: tool %q has invalid JSON schema: %w", d.Name, err)
			}
			fd.ParametersJsonSchema = schema
		}
		decls = append(decls, fd)
	}
	return []*genai.Tool{{FunctionDeclarations: decls}}, nil
}

// toolConfig maps a normalized [llm.ToolChoice] onto a Gemini ToolConfig. An unset
// choice returns nil (provider default). A specific tool name constrains the model to
// ANY mode with that single allowed function name.
func toolConfig(choice llm.ToolChoice) *genai.ToolConfig {
	var mode genai.FunctionCallingConfigMode
	var allowed []string

	switch choice {
	case "":
		return nil
	case llm.ToolChoiceAuto:
		mode = genai.FunctionCallingConfigModeAuto
	case llm.ToolChoiceAny, llm.ToolChoiceRequired:
		mode = genai.FunctionCallingConfigModeAny
	case llm.ToolChoiceNone:
		mode = genai.FunctionCallingConfigModeNone
	default:
		// A specific tool name: require a call constrained to that function.
		mode = genai.FunctionCallingConfigModeAny
		allowed = []string{string(choice)}
	}

	return &genai.ToolConfig{FunctionCallingConfig: &genai.FunctionCallingConfig{
		Mode:                 mode,
		AllowedFunctionNames: allowed,
	}}
}

// ---------------------------------------------------------------------------
// Non-streaming response aggregation
// ---------------------------------------------------------------------------

// aggregateResponse maps a complete (non-streaming) [genai.GenerateContentResponse]
// onto the normalized [llm.Response]: ordered content parts, the normalized stop
// reason, normalized usage, and the opaque provider-raw continuation blob.
func aggregateResponse(resp *genai.GenerateContentResponse) *llm.Response {
	out := &llm.Response{
		Usage: normalizeUsage(resp.UsageMetadata),
	}

	var cand *genai.Candidate
	for _, c := range resp.Candidates {
		if c != nil {
			cand = c
			break
		}
	}
	if cand == nil {
		// No candidate (e.g. blocked prompt): classify as other and carry nothing.
		out.StopReason = llm.StopOther
		return out
	}

	out.StopReason, out.RawStopReason = mapStopReason(cand.FinishReason)
	if cand.Content != nil {
		out.Content = contentParts(cand.Content.Parts)
		out.ProviderRaw = marshalProviderRaw([]*genai.Content{cand.Content})
	}
	return out
}

// contentParts maps Gemini response parts onto normalized [llm.ContentPart]s,
// preserving order: text -> TextPart, thought text -> ThinkingPart, functionCall ->
// ToolCall (with parsed args).
func contentParts(parts []*genai.Part) []llm.ContentPart {
	out := make([]llm.ContentPart, 0, len(parts))
	for _, part := range parts {
		if part == nil {
			continue
		}
		switch {
		case part.FunctionCall != nil:
			out = append(out, llm.ContentPart{ToolCall: &llm.ToolCall{
				ID:   part.FunctionCall.ID,
				Name: part.FunctionCall.Name,
				Args: part.FunctionCall.Args,
			}})
		case part.Text != "" && part.Thought:
			out = append(out, llm.ContentPart{Thinking: &llm.ThinkingPart{
				Text:      part.Text,
				Signature: string(part.ThoughtSignature),
			}})
		case part.Text != "":
			out = append(out, llm.ContentPart{Text: &llm.TextPart{Text: part.Text}})
		}
	}
	return out
}

// marshalProviderRaw serializes content into the opaque provider-raw blob; nil on
// empty content or marshal error.
func marshalProviderRaw(content []*genai.Content) llm.ProviderRaw {
	if len(content) == 0 {
		return nil
	}
	b, err := json.Marshal(providerRawBlob{Content: content})
	if err != nil {
		return nil
	}
	return b
}

// ---------------------------------------------------------------------------
// StreamReader
// ---------------------------------------------------------------------------

// streamReader adapts the genai GenerateContentStream iter.Seq2 to a
// [llm.StreamReader]. It pulls chunks from the iterator on demand, runs each through
// the [streamNormalizer], and buffers the emitted [llm.StreamEvent]s so Recv returns
// one event per call. A mid-stream provider error is normalized and returned from Recv;
// io.EOF is returned once the iterator and the buffer are exhausted (after the terminal
// Done). It is not safe for concurrent use.
type streamReader struct {
	next  func() (*genai.GenerateContentResponse, error, bool) // pull from iter.Seq2
	stop  func()                                               // releases the iterator
	norm  *streamNormalizer
	buf   []llm.StreamEvent
	err   error // sticky terminal error (normalized) or io.EOF
	ended bool  // iterator drained; flush the normalizer's trailing Done once
}

// newStreamReader builds a streamReader over the given genai response iterator using
// iter.Pull2 so the adapter can pull chunks lazily and synchronously per Recv call.
func newStreamReader(seq iter.Seq2[*genai.GenerateContentResponse, error]) *streamReader {
	next, stop := iter.Pull2(seq)
	return &streamReader{
		next: next,
		stop: stop,
		norm: newStreamNormalizer(),
	}
}

// Recv returns the next normalized [llm.StreamEvent]. It returns [io.EOF] when the
// stream is exhausted after the terminal [llm.Done], or a normalized
// [*llm.ProviderError] on a mid-stream failure.
func (r *streamReader) Recv() (llm.StreamEvent, error) {
	for {
		// Drain any buffered events first.
		if len(r.buf) > 0 {
			ev := r.buf[0]
			r.buf = r.buf[1:]
			return ev, nil
		}
		if r.err != nil {
			return llm.StreamEvent{}, r.err
		}
		if r.ended {
			// Iterator drained: flush a defensive trailing Done if needed, then EOF.
			r.buf = r.norm.finish()
			r.err = errEOF
			if len(r.buf) == 0 {
				return llm.StreamEvent{}, r.err
			}
			continue
		}

		chunk, err, ok := r.next()
		if !ok {
			// Iterator finished without error; mark ended so the trailing Done flushes.
			r.ended = true
			continue
		}
		if err != nil {
			// Mid-stream provider error: surface it normalized, but still flush any
			// events the normalizer had already produced (none pending here).
			r.err = normalizeError(err)
			continue
		}

		events, nerr := r.norm.next(chunk)
		if nerr != nil {
			r.err = normalizeError(nerr)
			continue
		}
		r.buf = append(r.buf, events...)
		// Loop: a chunk may yield zero events (e.g. empty candidate); keep pulling.
	}
}

// Close releases the underlying iterator. It is safe to call multiple times and after
// Recv has returned an error or io.EOF.
func (r *streamReader) Close() error {
	if r.stop != nil {
		r.stop()
		r.stop = nil
	}
	return nil
}

// compile-time guards.
var (
	_ llm.StreamReader = (*streamReader)(nil)
	_ modelsAPI        = (*genai.Models)(nil)
)
