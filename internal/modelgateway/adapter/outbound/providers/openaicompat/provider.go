package openaicompat

import (
	"context"
	"errors"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// Config configures an OpenAI-compatible [Provider].
type Config struct {
	// BaseURL is the endpoint base URL, e.g. "http://localhost:11434/v1" for
	// Ollama or "http://localhost:8000/v1" for vLLM. It is required.
	BaseURL string
	// APIKey is the bearer key sent to the endpoint. Most local servers ignore it
	// but the SDK requires a non-empty value; a placeholder such as "sk-local" is
	// substituted when this is empty.
	APIKey string
	// Profile is the per-endpoint capability profile. The zero value yields a
	// conservative generic profile via [GenericProfile]; use [LMStudioProfile] /
	// [OllamaProfile] for those servers.
	Profile EndpointProfile
	// Options are extra SDK request options appended after the base URL and key
	// (e.g. custom headers); optional.
	Options []option.RequestOption
}

// placeholderAPIKey is substituted when [Config.APIKey] is empty so the SDK, which
// requires a non-empty key, can target a self-hosted server that ignores auth. It is
// a deliberate non-secret placeholder, not a credential.
//
//nolint:gosec // G101: not a credential — a placeholder for keyless self-hosted endpoints.
const placeholderAPIKey = "sk-noauth-openaicompat"

// Provider is the [llm.Provider] implementation for OpenAI-compatible Chat
// Completions endpoints. It is safe for concurrent use; the underlying SDK client
// is concurrency-safe.
type Provider struct {
	client  openai.Client
	profile EndpointProfile
}

// New constructs an OpenAI-compatible [Provider] for the given [Config]. It returns
// a [*llm.ProviderError] of kind [llm.ErrInvalidRequest] if the base URL is empty.
func New(cfg Config) (*Provider, error) {
	if cfg.BaseURL == "" {
		return nil, newInvalidRequest(errors.New("openaicompat: BaseURL is required"))
	}
	key := cfg.APIKey
	if key == "" {
		key = placeholderAPIKey
	}
	profile := cfg.Profile
	if profile == (EndpointProfile{}) {
		profile = GenericProfile()
	}

	opts := append([]option.RequestOption{
		option.WithBaseURL(cfg.BaseURL),
		option.WithAPIKey(key),
	}, cfg.Options...)

	return &Provider{
		client:  openai.NewClient(opts...),
		profile: profile,
	}, nil
}

// Generate runs a single non-streaming Chat Completions request and returns the
// aggregated normalized [llm.Response]. Failures are normalized to a
// [*llm.ProviderError].
func (p *Provider) Generate(ctx context.Context, req llm.Request) (*llm.Response, error) {
	params, err := buildParams(req)
	if err != nil {
		return nil, err
	}
	completion, err := p.client.Chat.Completions.New(ctx, params)
	if err != nil {
		return nil, normalizeError(err)
	}
	return assembleResponse(completion)
}

// Stream runs a streaming Chat Completions request and returns an [llm.StreamReader]
// of normalized events terminated by a single [llm.Done]. A failure to construct
// the request is returned immediately as a [*llm.ProviderError]; mid-stream failures
// surface from [llm.StreamReader.Recv].
func (p *Provider) Stream(ctx context.Context, req llm.Request) (llm.StreamReader, error) {
	params, err := buildParams(req)
	if err != nil {
		return nil, err
	}
	// Ask the server to report usage on the trailing chunk when it supports
	// stream_options; servers that ignore it simply omit usage.
	params.StreamOptions = openai.ChatCompletionStreamOptionsParam{
		IncludeUsage: openai.Bool(true),
	}
	stream := p.client.Chat.Completions.NewStreaming(ctx, params)
	if err := stream.Err(); err != nil {
		return nil, normalizeError(err)
	}
	return newChatStreamReader(stream), nil
}

// CountTokens is unsupported on the OpenAI-compatible path: self-hosted endpoints
// expose no portable count-tokens API. It always returns a [*llm.ProviderError] of
// kind [llm.ErrUnsupported], consistent with
// [llm.Capabilities.SupportsTokenCounting] being false (architecture §11.6).
func (p *Provider) CountTokens(_ context.Context, _ llm.Request) (int, error) {
	return 0, newUnsupported(errors.New("openaicompat: token counting is not supported for OpenAI-compatible endpoints"))
}

// Capabilities returns the per-(endpoint, model) [llm.Capabilities] from the
// configured endpoint profile. The model id is accepted for interface conformance
// and forward compatibility with per-model overrides; the v1 profile is per
// endpoint.
func (p *Provider) Capabilities(_ context.Context, _ string) (llm.Capabilities, error) {
	return p.profile.capabilities(), nil
}

// Ensure Provider satisfies llm.Provider at compile time.
var _ llm.Provider = (*Provider)(nil)
