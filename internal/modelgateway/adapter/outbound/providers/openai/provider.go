package openai

import (
	"context"
	"errors"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/xd1lab/harness-ai/internal/modelgateway/adapter/outbound/providers/openaicompat"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// Config configures a native OpenAI [Provider].
type Config struct {
	// APIKey is the OpenAI API key. When empty, the SDK falls back to the
	// OPENAI_API_KEY environment variable.
	APIKey string
	// BaseURL overrides the default OpenAI API base URL (e.g. for Azure OpenAI or a
	// gateway). Optional.
	BaseURL string
	// UseChatCompletions selects the Chat Completions API instead of the default
	// Responses API. When true, this adapter delegates to the OpenAI-compatible
	// Chat-Completions path so both surfaces share one stream normalizer
	// (architecture §11.5). The Responses API is the default (UseChatCompletions
	// false).
	UseChatCompletions bool
	// Capabilities is the per-model capability set this endpoint advertises. The
	// zero value yields a sensible default for a tool-capable Responses model via
	// [DefaultCapabilities].
	Capabilities llm.Capabilities
	// Options are extra SDK request options appended after the key and base URL;
	// optional.
	Options []option.RequestOption
}

// Provider is the native OpenAI [llm.Provider] implementation. By default it uses
// the Responses API; with [Config.UseChatCompletions] it delegates to the
// OpenAI-compatible Chat-Completions adapter so the Chat surface shares a single
// normalizer. It is safe for concurrent use.
type Provider struct {
	client   openai.Client
	caps     llm.Capabilities
	chatImpl *openaicompat.Provider // non-nil only in Chat-Completions mode
}

// New constructs a native OpenAI [Provider] for the given [Config].
func New(cfg Config) (*Provider, error) {
	caps := cfg.Capabilities
	if caps == (llm.Capabilities{}) {
		caps = DefaultCapabilities()
	}

	opts := make([]option.RequestOption, 0, len(cfg.Options)+2)
	if cfg.APIKey != "" {
		opts = append(opts, option.WithAPIKey(cfg.APIKey))
	}
	if cfg.BaseURL != "" {
		opts = append(opts, option.WithBaseURL(cfg.BaseURL))
	}
	opts = append(opts, cfg.Options...)

	p := &Provider{
		client: openai.NewClient(opts...),
		caps:   caps,
	}

	if cfg.UseChatCompletions {
		// Delegate the Chat surface to the shared OpenAI-compatible adapter,
		// pointed at OpenAI's (or the override) base URL. This is the single
		// Chat-Completions normalizer shared by both surfaces (§11.5).
		base := cfg.BaseURL
		if base == "" {
			base = "https://api.openai.com/v1"
		}
		chat, err := openaicompat.New(openaicompat.Config{
			BaseURL: base,
			APIKey:  cfg.APIKey,
			Profile: chatProfileFromCaps(caps),
			Options: cfg.Options,
		})
		if err != nil {
			return nil, err
		}
		p.chatImpl = chat
	}

	return p, nil
}

// Generate runs a single non-streaming generation and returns the aggregated
// normalized [llm.Response]. In Chat-Completions mode it delegates to the shared
// adapter; otherwise it uses the Responses API. Failures are normalized to a
// [*llm.ProviderError].
func (p *Provider) Generate(ctx context.Context, req llm.Request) (*llm.Response, error) {
	if p.chatImpl != nil {
		return p.chatImpl.Generate(ctx, req)
	}
	params, err := buildParams(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.client.Responses.New(ctx, params)
	if err != nil {
		return nil, normalizeError(err)
	}
	return assembleResponse(resp)
}

// Stream runs a streaming generation and returns an [llm.StreamReader] of normalized
// events terminated by a single [llm.Done]. In Chat-Completions mode it delegates to
// the shared adapter; otherwise it streams the Responses API.
func (p *Provider) Stream(ctx context.Context, req llm.Request) (llm.StreamReader, error) {
	if p.chatImpl != nil {
		return p.chatImpl.Stream(ctx, req)
	}
	params, err := buildParams(req)
	if err != nil {
		return nil, err
	}
	stream := p.client.Responses.NewStreaming(ctx, params)
	if err := stream.Err(); err != nil {
		return nil, normalizeError(err)
	}
	return newResponsesStreamReader(stream), nil
}

// CountTokens is unsupported on both OpenAI surfaces in v1: the Responses API has no
// count-tokens endpoint and the harness bills from authoritative usage rather than
// estimating with a local tokenizer (architecture §11.6). It always returns a
// [*llm.ProviderError] of kind [llm.ErrUnsupported], consistent with
// [llm.Capabilities.SupportsTokenCounting] being false.
func (p *Provider) CountTokens(_ context.Context, _ llm.Request) (int, error) {
	return 0, newUnsupported(errors.New("openai: token counting is not supported (SupportsTokenCounting=false)"))
}

// Capabilities returns the per-(endpoint, model) [llm.Capabilities] configured for
// this provider. The model id is accepted for interface conformance and forward
// compatibility with per-model resolution.
func (p *Provider) Capabilities(_ context.Context, _ string) (llm.Capabilities, error) {
	return p.caps, nil
}

// Ensure Provider satisfies llm.Provider at compile time.
var _ llm.Provider = (*Provider)(nil)
