package secret

import (
	"context"
	"fmt"
	"os"
)

// EnvSecrets is the v1 [SecretsPort] backend that resolves named secrets from
// process environment variables (ADR-0013: provider credentials are env-only in
// v1). It keeps the consumer decoupled from the source, so a file or KMS backend
// can be substituted later without touching call sites.
//
// An unset name — or a name set to the empty string — resolves to [ErrNotFound],
// so a missing required credential fails fast and loudly rather than silently
// degrading to an empty key. EnvSecrets is safe for concurrent use.
type EnvSecrets struct {
	// prefix is prepended to the requested name before the environment lookup, so
	// callers can ask for a logical name (e.g. "ANTHROPIC_API_KEY") while the
	// deployment namespaces it (e.g. "BOLTROPE_SECRET_ANTHROPIC_API_KEY"). Empty
	// by default.
	prefix string
}

// EnvOption configures an [EnvSecrets] at construction.
type EnvOption func(*EnvSecrets)

// WithEnvPrefix sets a prefix prepended to every requested name before the
// environment lookup. For example WithEnvPrefix("BOLTROPE_SECRET_") makes a
// Get(ctx, "ANTHROPIC_API_KEY") read $BOLTROPE_SECRET_ANTHROPIC_API_KEY.
func WithEnvPrefix(prefix string) EnvOption {
	return func(e *EnvSecrets) { e.prefix = prefix }
}

// NewEnvSecrets returns an [EnvSecrets] reading from the process environment,
// configured by the given options.
func NewEnvSecrets(opts ...EnvOption) *EnvSecrets {
	e := &EnvSecrets{}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Get resolves the secret in the environment variable named prefix+name. It
// returns [ErrNotFound] when the variable is unset or empty, and otherwise a
// [Secret] wrapping the value (so the resolved credential still redacts itself in
// logs). The returned error wraps [ErrNotFound] with the requested name for
// [errors.Is] branching and human-readable diagnostics.
func (e *EnvSecrets) Get(_ context.Context, name string) (Secret, error) {
	envName := e.prefix + name
	v, ok := os.LookupEnv(envName)
	if !ok || v == "" {
		return Secret{}, fmt.Errorf("%w: %q (env %q)", ErrNotFound, name, envName)
	}
	return New(v), nil
}

// Compile-time assertion that EnvSecrets satisfies the SecretsPort port.
var _ SecretsPort = (*EnvSecrets)(nil)
