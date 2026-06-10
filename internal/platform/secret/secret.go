// Package secret defines the [SecretsPort] for resolving sensitive values, and the
// [Secret] masking type implementing the slog [slog.LogValuer] redaction pattern.
//
// # Two distinct concerns
//
//   - Resolution: where secrets come from. Provider API keys live ONLY in
//     model-gateway config, env-only, never in the orchestrator or in events
//     (ADR-0013 §"Output masking"; architecture §8.10). [SecretsPort] is the
//     consumer-side port a service uses to fetch a named secret without knowing the
//     backend (env, file, KMS).
//   - Masking: keeping resolved secrets out of logs and telemetry. [Secret] wraps a
//     sensitive value so it redacts itself when logged via slog, and never prints
//     in plain form through fmt.
//
// # Masking is defense-in-depth ONLY
//
// Per ADR-0013, output masking is explicitly NOT a lethal-trifecta containment
// boundary: it catches only known/registry/pattern secrets and is trivially
// defeated by an adversarial model (base64/hex, splitting across calls,
// paraphrase). The real exfiltration control is egress restriction
// ([github.com/boltrope/boltrope/internal/toolruntime/app.EgressBroker] plus the
// taint gate). [Secret] and [SecretsPort] exist for log/telemetry hygiene, not as a
// security guarantee on tool output. This is stated in the type docs so callers do
// not mistake redaction for containment.
//
// # Purity
//
// Contract-only: interfaces, the [Secret] value type with its LogValuer/Stringer
// behavior, and a [Redactor] hook type. It imports only the standard library
// (context, errors, log/slog).
package secret

import (
	"context"
	"errors"
	"log/slog"
)

// Redacted is the fixed placeholder substituted for a sensitive value in logs,
// telemetry, and string formatting. It carries no information about the underlying
// value (not even its length).
const Redacted = "[REDACTED]"

// ErrNotFound is returned by [SecretsPort.Get] when no secret is registered under
// the requested name. It is a sentinel so callers can branch with [errors.Is].
var ErrNotFound = errors.New("secret: not found")

// SecretsPort resolves named sensitive values from a configured backend without the
// consumer knowing the source. v1's backend is env-only for provider credentials
// (ADR-0013); the port keeps the consumer (e.g. a model-gateway adapter) decoupled
// so a file or KMS backend can be substituted later without touching call sites.
//
// Implementations must be safe for concurrent use. A returned [Secret] already
// carries redaction behavior, so callers may log the fact that a secret was
// resolved without leaking its value.
type SecretsPort interface {
	// Get resolves the secret registered under name. It returns [ErrNotFound] (via
	// the error) when the name is unknown, so a missing required credential fails
	// fast and loudly rather than silently degrading. The returned [Secret] never
	// reveals its value through logging or formatting; use [Secret.Reveal] at the
	// single trusted call site that needs the plaintext (e.g. constructing a
	// provider SDK client).
	Get(ctx context.Context, name string) (Secret, error)
}

// Redactor reports whether and how a string should be scrubbed before it is written
// to a log or sent to telemetry. It is the best-effort, pattern/registry-based
// hook used for log hygiene on tool output and free-form fields.
//
// IMPORTANT: a Redactor is defense-in-depth for log hygiene only, never a
// trifecta containment boundary (see the package doc and ADR-0013). It will miss
// transformed or split secrets by construction; do not rely on it to prevent
// exfiltration.
type Redactor interface {
	// Redact returns s with any recognized sensitive substrings replaced by
	// [Redacted]. It returns s unchanged when nothing is recognized. It must be
	// pure and safe for concurrent use.
	Redact(s string) string
}

// Secret wraps a sensitive value so that it is masked everywhere except at the one
// trusted call site that explicitly reveals it. It implements [slog.LogValuer] and
// [fmt.Stringer] to substitute [Redacted], so an accidental %v, %s, or
// slog.Any("key", secret) cannot leak the value.
//
// The zero Secret holds an empty value and is safe to log. Construct a populated
// Secret with [New]. Copying a Secret copies the wrapped value; it remains masked.
type Secret struct {
	// value is the unexported plaintext. It is never exported, so the only ways to
	// observe it are the deliberate [Secret.Reveal] / [Secret.RevealBytes] methods.
	value string
}

// New wraps value in a [Secret]. The plaintext is retained in unexported form and
// will be masked by [Secret.String] and [Secret.LogValue]; recover it only via
// [Secret.Reveal] at a trusted boundary.
func New(value string) Secret { return Secret{value: value} }

// Reveal returns the underlying plaintext. This is the single intended escape
// hatch: call it only at the trusted boundary that genuinely needs the value (for
// example, when constructing an authenticated provider SDK client), and never pass
// the result to a logger.
func (s Secret) Reveal() string { return s.value }

// RevealBytes returns the underlying plaintext as a byte slice. The returned slice
// is a fresh copy so mutating it does not affect the [Secret]. Same caution as
// [Secret.Reveal] applies.
func (s Secret) RevealBytes() []byte { return []byte(s.value) }

// IsZero reports whether the secret holds no value (the zero/unset [Secret]).
func (s Secret) IsZero() bool { return s.value == "" }

// String implements [fmt.Stringer], returning [Redacted] so a Secret never prints
// its value through fmt verbs.
func (s Secret) String() string { return Redacted }

// GoString implements [fmt.GoStringer] so even the %#v verb yields [Redacted]
// rather than the struct's unexported field.
func (s Secret) GoString() string { return Redacted }

// LogValue implements [slog.LogValuer], returning a string [slog.Value] of
// [Redacted] so structured logs record the placeholder, never the plaintext. This
// is the slog redaction pattern referenced by ADR-0013 §"Output masking" and
// architecture §8.10.
func (s Secret) LogValue() slog.Value { return slog.StringValue(Redacted) }

// Compile-time assertion that Secret satisfies slog.LogValuer.
var _ slog.LogValuer = Secret{}
