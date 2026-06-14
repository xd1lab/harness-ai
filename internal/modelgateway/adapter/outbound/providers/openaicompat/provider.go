package openaicompat

import (
	"context"
	"errors"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
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
	// Capabilities, when non-nil, resolves per-(endpoint, model) capabilities from
	// the central registry at request-build time. It is the ONLY path by which native
	// structured output can be enabled for a self-hosted endpoint: the operator opts
	// in via capabilities.Registry.SetEndpointOverride. A nil value (or no override)
	// keeps the conservative default (no native; loop validate-retry).
	Capabilities capabilityResolver
	// Endpoint is the registry endpoint name this provider is bound to (e.g.
	// "openaicompat"); used with Capabilities to resolve per-model caps.
	Endpoint string
	// Options are extra SDK request options appended after the base URL and key
	// (e.g. custom headers); optional.
	Options []option.RequestOption
}

// capabilityResolver is the narrow consumer-side port the provider uses to obtain
// per-(endpoint, model) capabilities. The central *capabilities.Registry satisfies
// it; the provider depends only on this interface so the concrete registry is
// injected from wiring and tests can supply a fake.
type capabilityResolver interface {
	Resolve(endpoint, model string) llm.Capabilities
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
	client   openai.Client
	profile  EndpointProfile
	resolver capabilityResolver // non-nil => resolve per-model caps centrally
	endpoint string             // registry endpoint name (used with resolver)
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
		client:   openai.NewClient(opts...),
		profile:  profile,
		resolver: cfg.Capabilities,
		endpoint: cfg.Endpoint,
	}, nil
}

// resolveCaps returns the capabilities used to gate native structured output for
// model. With a central resolver injected it is authoritative (an operator can opt
// in via SetEndpointOverride); otherwise it falls back to the endpoint profile,
// which always reports SupportsJSONSchemaStrict=false — so native is never
// blind-sent to an unknown self-hosted server.
func (p *Provider) resolveCaps(model string) llm.Capabilities {
	if p.resolver != nil {
		return p.resolver.Resolve(p.endpoint, model)
	}
	return p.profile.capabilities()
}

// Generate runs a single non-streaming Chat Completions request and returns the
// aggregated normalized [llm.Response]. Failures are normalized to a
// [*llm.ProviderError].
func (p *Provider) Generate(ctx context.Context, req llm.Request) (*llm.Response, error) {
	params, err := buildParams(req, p.resolveCaps(req.Model))
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
	params, err := buildParams(req, p.resolveCaps(req.Model))
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
