// Command boltrope-modelgwd is the model-gateway daemon: a stateless service that
// fronts a configured LLM provider behind the normalized boltrope.v1
// ModelGatewayService (T-CMD-02; architecture §4.3, §11). It wires a provider
// adapter (Anthropic / Gemini / OpenAI / OpenAI-compatible) decorated with the
// harness retry policy, the per-(endpoint,model) capability resolver, and
// cost-on-Done, then serves Generate/CountTokens/GetCapabilities over mTLS with
// the shared [github.com/boltrope/boltrope/internal/platform/daemon] harness
// (health, readiness, graceful shutdown).
//
// Provider selection is a deployment concern, not part of the frozen shared
// [config.Config], so it is read from the environment in [loadGatewaySettings];
// the credential VALUE is never stored — only the NAME of the env var holding it,
// resolved through the [secret.SecretsPort] at this trusted boundary (ADR-0013).
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"

	mgwgrpc "github.com/boltrope/boltrope/internal/modelgateway/adapter/inbound/grpc"
	"github.com/boltrope/boltrope/internal/modelgateway/adapter/outbound/providers/anthropic"
	"github.com/boltrope/boltrope/internal/modelgateway/adapter/outbound/providers/gemini"
	"github.com/boltrope/boltrope/internal/modelgateway/adapter/outbound/providers/openai"
	"github.com/boltrope/boltrope/internal/modelgateway/adapter/outbound/providers/openaicompat"
	"github.com/boltrope/boltrope/internal/modelgateway/adapter/outbound/providers/stub"
	mgwapp "github.com/boltrope/boltrope/internal/modelgateway/app"
	"github.com/boltrope/boltrope/internal/modelgateway/app/capabilities"
	"github.com/boltrope/boltrope/internal/modelgateway/app/retry"
	"github.com/boltrope/boltrope/internal/platform/config"
	"github.com/boltrope/boltrope/internal/platform/daemon"
	"github.com/boltrope/boltrope/internal/platform/llm"
	"github.com/boltrope/boltrope/internal/platform/pricing"
	"github.com/boltrope/boltrope/internal/platform/secret"
)

const serviceName = "model-gateway"

// gatewaySettings are the model-gateway-specific knobs read from the environment
// (the frozen shared [config.Config] is provider-agnostic). They select which
// provider the gateway fronts and how to reach it.
type gatewaySettings struct {
	// Provider selects the provider adapter: "anthropic", "gemini", "openai",
	// "openaicompat", "stub", or "" (defaults to "openaicompat", the keyless local
	// path). Use "stub" for local demo, CI smoke tests, and DOD-05 keyless E2E — it
	// needs no API key and streams a deterministic scripted response. NOT for
	// production.
	Provider string
	// APIKeyEnv is the NAME of the env var holding the upstream provider API key;
	// resolved via the secrets port (env-only; ADR-0013). Empty is allowed for
	// keyless self-hosted endpoints.
	APIKeyEnv string
	// OpenAIBaseURL is the base URL for the openai/openaicompat providers (e.g.
	// "http://localhost:11434/v1" for Ollama). Required for "openaicompat".
	OpenAIBaseURL string
	// TrustDomain is the SPIFFE trust domain for inter-service mTLS.
	TrustDomain string
}

// envOr returns the value of env var key, or def when it is unset/empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// loadGatewaySettings reads the gateway-specific environment. The orchestrator's
// view of the gateway lives in [config.Config]; these are the gateway's own
// provider settings, namespaced under BOLTROPE_MODELGW_*.
func loadGatewaySettings() gatewaySettings {
	return gatewaySettings{
		Provider:      os.Getenv("BOLTROPE_MODELGW_PROVIDER"),
		APIKeyEnv:     os.Getenv("BOLTROPE_MODELGW_API_KEY_ENV"),
		OpenAIBaseURL: envOr("BOLTROPE_MODELGW_OPENAI_BASE_URL", "http://localhost:11434/v1"),
		TrustDomain:   envOr("BOLTROPE_TRUST_DOMAIN", "boltrope.local"),
	}
}

// buildProvider constructs the configured raw [llm.Provider] and returns it
// alongside the logical endpoint name used as the capability-resolution key. The
// API key (when the provider needs one) is resolved via the secrets port from the
// configured env-var NAME, so a missing required credential fails fast (ADR-0013).
// An unknown provider kind is a fatal configuration error (NFR-OPS-04).
func buildProvider(ctx context.Context, gw gatewaySettings, secrets secret.SecretsPort) (llm.Provider, string, error) {
	switch gw.Provider {
	case "", "openaicompat":
		prov, err := openaicompat.New(openaicompat.Config{BaseURL: gw.OpenAIBaseURL})
		if err != nil {
			return nil, "", fmt.Errorf("modelgwd: build openaicompat provider: %w", err)
		}
		return prov, "openaicompat", nil

	case "anthropic":
		key, err := resolveKey(ctx, secrets, gw.APIKeyEnv)
		if err != nil {
			return nil, "", err
		}
		return anthropic.New(anthropic.WithAPIKey(key)), "anthropic", nil

	case "gemini":
		key, err := resolveKey(ctx, secrets, gw.APIKeyEnv)
		if err != nil {
			return nil, "", err
		}
		prov, err := gemini.New(ctx, gemini.Config{APIKey: key})
		if err != nil {
			return nil, "", fmt.Errorf("modelgwd: build gemini provider: %w", err)
		}
		return prov, "gemini", nil

	case "openai":
		key, err := resolveKey(ctx, secrets, gw.APIKeyEnv)
		if err != nil {
			return nil, "", err
		}
		prov, err := openai.New(openai.Config{APIKey: key, BaseURL: gw.OpenAIBaseURL})
		if err != nil {
			return nil, "", fmt.Errorf("modelgwd: build openai provider: %w", err)
		}
		return prov, "openai", nil

	case "stub":
		// Built-in deterministic test/demo provider — no API key or network required.
		// For local demo, CI smoke tests, and DOD-05 keyless E2E. NOT for production.
		return stub.New(), "stub", nil

	default:
		return nil, "", fmt.Errorf("modelgwd: unknown provider %q (want anthropic|gemini|openai|openaicompat|stub)", gw.Provider)
	}
}

// resolveKey resolves the API key named by envName via the secrets port. An empty
// envName is a fatal misconfiguration for a provider that requires a key.
func resolveKey(ctx context.Context, secrets secret.SecretsPort, envName string) (string, error) {
	if envName == "" {
		return "", fmt.Errorf("modelgwd: this provider requires an API key; set BOLTROPE_MODELGW_API_KEY_ENV to the env-var name holding it")
	}
	sec, err := secrets.Get(ctx, envName)
	if err != nil {
		return "", fmt.Errorf("modelgwd: resolving API key from %q: %w", envName, err)
	}
	return sec.Reveal(), nil
}

// Run wires the model-gateway and serves it until ctx is cancelled or a signal
// arrives, then shuts down gracefully. logw is the log sink (production:
// os.Stderr). It returns the first fatal error (telemetry/provider construction,
// credential selection, or a listener failure).
func Run(ctx context.Context, cfg *config.Config, logw io.Writer) error {
	tel, err := daemon.SetupTelemetry(ctx, serviceName, version, cfg, logw)
	if err != nil {
		return err
	}

	gw := loadGatewaySettings()
	secrets := secret.NewEnvSecrets()

	provider, endpoint, err := buildProvider(ctx, gw, secrets)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return err
	}

	// Decorate with the harness retry policy (deterministic clock/jitter in tests;
	// system clock + crypto-seeded jitter in production via newJitter).
	retrying := retry.New(provider, retry.Config{
		MaxAttempts: defaultRetryMaxAttempts,
		BaseDelay:   defaultRetryBaseDelay,
		MaxDelay:    defaultRetryMaxDelay,
	}, llm.SystemClock{}, daemon.NewJitter())

	svc, err := mgwapp.NewService(mgwapp.Config{
		Provider:     retrying,
		Endpoint:     endpoint,
		Capabilities: capabilities.NewRegistry(nil),
		Cost:         pricing.Cost,
	})
	if err != nil {
		_ = tel.Shutdown(ctx)
		return fmt.Errorf("modelgwd: build gateway service: %w", err)
	}
	server := mgwgrpc.NewServer(svc)

	credsCfg, err := serverCredsConfig(gw.TrustDomain, cfg, tel)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return err
	}
	creds, err := daemon.ServerCredentials(credsCfg)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return err
	}

	return daemon.Run(ctx, daemon.RunInput{
		GRPCAddr:    cfg.Server.GRPCAddr,
		HTTPAddr:    cfg.Server.HTTPAddr,
		Creds:       creds,
		Policy:      gatewayRBAC(gw.TrustDomain),
		Telemetry:   tel,
		HasIdentity: func() bool { return daemon.HasServerIdentity(credsCfg) },
		Service: daemon.Service{
			Register: func(srv *grpc.Server) { registerGatewayServer(srv, server) },
			// The gateway is stateless; readiness gates on SVID presence only (its
			// upstream provider is dialed lazily per request, architecture §10.1).
			Closers: []func() error{func() error { _ = tel.Shutdown(ctx); return nil }},
		},
	})
}

// serverCredsConfig assembles the [daemon.CredsConfig] for this service from the
// trust domain and the resolved dev-insecure decision. The live SPIFFE source is
// left nil here; the production build wires it under the `spire` build tag.
func serverCredsConfig(trustDomain string, cfg *config.Config, tel *daemon.Telemetry) (daemon.CredsConfig, error) {
	td, err := spiffeid.TrustDomainFromString(trustDomain)
	if err != nil {
		return daemon.CredsConfig{}, fmt.Errorf("modelgwd: invalid trust domain %q: %w", trustDomain, err)
	}
	id, err := spiffeid.FromSegments(td, serviceName)
	if err != nil {
		return daemon.CredsConfig{}, fmt.Errorf("modelgwd: build server SPIFFE id: %w", err)
	}
	return daemon.CredsConfig{
		TrustDomain: td,
		ServerID:    id,
		DevInsecure: cfg.DevInsecure,
		Source:      spiffeSource(),
		Logger:      tel.Logger,
	}, nil
}
