package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/ssestream"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// defaultMaxOutputTokens is the max_tokens used when a [llm.Request] leaves it
// zero. The Messages API requires a positive max_tokens, so a default is always
// supplied; the orchestrator typically sets an explicit value.
const defaultMaxOutputTokens int64 = 4096

// Compile-time assertion that Provider satisfies the kernel contract.
var _ llm.Provider = (*Provider)(nil)

// Provider implements [llm.Provider] over the Anthropic Messages API. It is
// stateless beyond the configured SDK client and is safe for concurrent use by
// multiple goroutines (the SDK client is concurrency-safe; the per-stream
// normalizer is created fresh per call).
type Provider struct {
	client          sdk.Client
	defaultMaxToken int64
}

// Option configures a [Provider].
type Option func(*config)

type config struct {
	apiKey          string
	baseURL         string
	requestOptions  []option.RequestOption
	defaultMaxToken int64
}

// WithAPIKey sets the Anthropic API key (maps to option.WithAPIKey).
func WithAPIKey(key string) Option {
	return func(c *config) { c.apiKey = key }
}

// WithBaseURL overrides the API base URL (maps to option.WithBaseURL), for
// proxies or Anthropic-compatible gateways.
func WithBaseURL(url string) Option {
	return func(c *config) { c.baseURL = url }
}

// WithDefaultMaxTokens sets the max_tokens applied when a request leaves it zero.
func WithDefaultMaxTokens(n int64) Option {
	return func(c *config) {
		if n > 0 {
			c.defaultMaxToken = n
		}
	}
}

// WithRequestOptions appends raw SDK request options (escape hatch for settings
// not covered by a typed Option, e.g. a custom HTTP client).
func WithRequestOptions(opts ...option.RequestOption) Option {
	return func(c *config) { c.requestOptions = append(c.requestOptions, opts...) }
}

// New constructs a [Provider]. The SDK's own retry layer is disabled
// (option.WithMaxRetries(0)) because retries are the model-gateway's
// responsibility via the single harness-level retry policy keyed on
// [llm.ProviderError] (architecture §4.4); letting the SDK also retry would
// double-count attempts and defeat the deterministic, injected-clock backoff.
func New(opts ...Option) *Provider {
	cfg := &config{defaultMaxToken: defaultMaxOutputTokens}
	for _, o := range opts {
		o(cfg)
	}

	reqOpts := []option.RequestOption{option.WithMaxRetries(0)}
	if cfg.apiKey != "" {
		reqOpts = append(reqOpts, option.WithAPIKey(cfg.apiKey))
	}
	if cfg.baseURL != "" {
		reqOpts = append(reqOpts, option.WithBaseURL(cfg.baseURL))
	}
	reqOpts = append(reqOpts, cfg.requestOptions...)

	return &Provider{
		client:          sdk.NewClient(reqOpts...),
		defaultMaxToken: cfg.defaultMaxToken,
	}
}

// Generate runs a single non-streaming generation and returns the aggregated
// normalized [llm.Response]. On failure it returns a [*llm.ProviderError].
func (p *Provider) Generate(ctx context.Context, req llm.Request) (*llm.Response, error) {
	params, err := buildMessageParams(req, p.defaultMaxToken)
	if err != nil {
		return nil, &llm.ProviderError{Kind: llm.ErrInvalidRequest, Raw: err}
	}
	msg, err := p.client.Messages.New(ctx, params)
	if err != nil {
		return nil, mapError(err)
	}
	return responseFromMessage(msg)
}

// Stream runs a streaming generation and returns a [llm.StreamReader] of
// normalized events terminated by a [llm.Done]. A failure to construct the
// request is returned immediately as a [*llm.ProviderError]; transport failures
// surface from [llm.StreamReader.Recv].
func (p *Provider) Stream(ctx context.Context, req llm.Request) (llm.StreamReader, error) {
	params, err := buildMessageParams(req, p.defaultMaxToken)
	if err != nil {
		return nil, &llm.ProviderError{Kind: llm.ErrInvalidRequest, Raw: err}
	}
	stream := p.client.Messages.NewStreaming(ctx, params)
	return newStreamReader(stream), nil
}

// CountTokens returns the input token count for req via the count_tokens
// endpoint. Anthropic models support token counting
// ([llm.Capabilities.SupportsTokenCounting] is true), so this is not
// capability-gated off; it returns a [*llm.ProviderError] on failure.
func (p *Provider) CountTokens(ctx context.Context, req llm.Request) (int, error) {
	if !resolveCapabilities(req.Model).SupportsTokenCounting {
		return 0, &llm.ProviderError{Kind: llm.ErrUnsupported}
	}
	params, err := buildCountTokensParams(req)
	if err != nil {
		return 0, &llm.ProviderError{Kind: llm.ErrInvalidRequest, Raw: err}
	}
	res, err := p.client.Messages.CountTokens(ctx, params)
	if err != nil {
		return 0, mapError(err)
	}
	return int(res.InputTokens), nil
}

// Capabilities returns the [llm.Capabilities] for the given Anthropic model. It
// resolves from a static per-model table and never performs I/O, so it ignores
// the context and returns no error.
func (p *Provider) Capabilities(_ context.Context, model string) (llm.Capabilities, error) {
	return resolveCapabilities(model), nil
}

// responseFromMessage aggregates a non-streaming [sdk.Message] into the
// normalized [llm.Response], mapping content blocks to content parts, the stop
// reason to the open [llm.StopReason] set, usage to [llm.Usage], and preserving
// the raw assistant content blocks in ProviderRaw for byte-faithful continuation
// (architecture §11.1).
func responseFromMessage(msg *sdk.Message) (*llm.Response, error) {
	parts, raw, err := contentFromBlocks(msg.Content)
	if err != nil {
		return nil, &llm.ProviderError{Kind: llm.ErrServer, Raw: err}
	}
	return &llm.Response{
		Content:       parts,
		StopReason:    mapStopReason(string(msg.StopReason)),
		RawStopReason: string(msg.StopReason),
		Usage:         usageFromMessage(msg.Usage),
		ProviderRaw:   raw,
	}, nil
}

// contentFromBlocks maps non-streaming content blocks to normalized content parts
// and builds the provider-raw continuation blob from the same blocks.
func contentFromBlocks(blocks []sdk.ContentBlockUnion) ([]llm.ContentPart, llm.ProviderRaw, error) {
	parts := make([]llm.ContentPart, 0, len(blocks))
	rawBlocks := make([]rawBlock, 0, len(blocks))
	for i := range blocks {
		b := blocks[i]
		switch b.Type {
		case "text":
			parts = append(parts, llm.ContentPart{Text: &llm.TextPart{Text: b.Text}})
			rawBlocks = append(rawBlocks, rawBlock{Type: "text", Text: b.Text})
		case "thinking":
			parts = append(parts, llm.ContentPart{Thinking: &llm.ThinkingPart{Text: b.Thinking, Signature: b.Signature}})
			rawBlocks = append(rawBlocks, rawBlock{Type: "thinking", Thinking: b.Thinking, Signature: b.Signature})
		case "tool_use", "server_tool_use":
			args, err := decodeArgs(b.Input)
			if err != nil {
				return nil, nil, err
			}
			parts = append(parts, llm.ContentPart{ToolCall: &llm.ToolCall{ID: b.ID, Name: b.Name, Args: args}})
			rawBlocks = append(rawBlocks, rawBlock{Type: b.Type, ID: b.ID, Name: b.Name, Input: cloneRaw(b.Input)})
		default:
			// Preserve any other block (e.g. redacted_thinking, tool results) in
			// the raw continuation blob so a pause continuation is faithful, but
			// do not surface it as a normalized part.
			rawBlocks = append(rawBlocks, rawBlock{Type: b.Type, Input: cloneRaw(b.Input)})
		}
	}
	if len(rawBlocks) == 0 {
		return parts, nil, nil
	}
	blob, err := json.Marshal(continuationBlob{Role: "assistant", Content: rawBlocks})
	if err != nil {
		return nil, nil, err
	}
	return parts, llm.ProviderRaw(blob), nil
}

// decodeArgs parses a tool-call input JSON object into the normalized args map.
// A nil/empty input decodes to an empty map.
func decodeArgs(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

// cloneRaw returns a copy of a raw JSON message so the continuation blob does not
// alias SDK-owned memory. Nil in, nil out.
func cloneRaw(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	out := make(json.RawMessage, len(raw))
	copy(out, raw)
	return out
}

// streamReader adapts the SDK SSE stream to [llm.StreamReader]. It pulls SDK
// events, runs them through the [streamNormalizer], and buffers the resulting
// normalized events so Recv returns one at a time. After the terminal Done it
// returns [io.EOF]; a transport error from the SDK stream is normalized to a
// [*llm.ProviderError].
type streamReader struct {
	stream *ssestream.Stream[sdk.MessageStreamEventUnion]
	norm   *streamNormalizer
	buf    []llm.StreamEvent
	err    error
	done   bool
}

// newStreamReader wraps an SDK SSE stream.
func newStreamReader(stream *ssestream.Stream[sdk.MessageStreamEventUnion]) *streamReader {
	return &streamReader{stream: stream, norm: newStreamNormalizer()}
}

// Recv returns the next normalized [llm.StreamEvent], pumping the SDK stream as
// needed. It returns [io.EOF] after the terminal Done has been delivered.
func (r *streamReader) Recv() (llm.StreamEvent, error) {
	for len(r.buf) == 0 {
		if r.err != nil {
			return llm.StreamEvent{}, r.err
		}
		if r.done {
			return llm.StreamEvent{}, io.EOF
		}
		if !r.stream.Next() {
			// Stream ended. A non-nil Err is a transport/API failure; a nil Err
			// means the SSE body closed.
			if err := r.stream.Err(); err != nil && !errors.Is(err, io.EOF) {
				r.err = mapError(err)
				return llm.StreamEvent{}, r.err
			}
			r.done = true
			return llm.StreamEvent{}, io.EOF
		}
		events, err := r.norm.next(r.stream.Current())
		if err != nil {
			r.err = &llm.ProviderError{Kind: llm.ErrServer, Raw: err}
			return llm.StreamEvent{}, r.err
		}
		r.buf = append(r.buf, events...)
	}
	ev := r.buf[0]
	r.buf = r.buf[1:]
	return ev, nil
}

// Close releases the underlying SSE stream. It is safe to call more than once and
// after Recv has returned an error or io.EOF.
func (r *streamReader) Close() error {
	if r.stream == nil {
		return nil
	}
	return r.stream.Close()
}
