package secret_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/platform/secret"
)

func TestRegistryRedactor_RedactsRegisteredValue(t *testing.T) {
	r := secret.NewRegistryRedactor()
	r.Register("sk-live-abc123")

	out := r.Redact("Authorization: Bearer sk-live-abc123 trailing")
	assert.Contains(t, out, secret.Redacted)
	assert.NotContains(t, out, "sk-live-abc123")
}

func TestRegistryRedactor_RedactsRegisteredSecret(t *testing.T) {
	r := secret.NewRegistryRedactor()
	r.RegisterSecret(secret.New("top-secret-token"))

	out := r.Redact("token=top-secret-token;")
	assert.Contains(t, out, secret.Redacted)
	assert.NotContains(t, out, "top-secret-token")
}

func TestRegistryRedactor_NoMatchUnchanged(t *testing.T) {
	r := secret.NewRegistryRedactor()
	r.Register("sk-live-abc123")

	const in = "nothing sensitive here"
	assert.Equal(t, in, r.Redact(in))
}

func TestRegistryRedactor_MultipleOccurrences(t *testing.T) {
	r := secret.NewRegistryRedactor()
	r.Register("dup")

	out := r.Redact("dup and dup and dup")
	assert.NotContains(t, out, "dup")
	assert.Equal(t, 3, strings.Count(out, secret.Redacted))
}

func TestRegistryRedactor_MultipleRegisteredValues(t *testing.T) {
	r := secret.NewRegistryRedactor()
	r.Register("alpha-key")
	r.Register("beta-key")

	out := r.Redact("alpha-key then beta-key")
	assert.NotContains(t, out, "alpha-key")
	assert.NotContains(t, out, "beta-key")
}

// Empty registered values must never cause every position to be redacted.
func TestRegistryRedactor_EmptyValueIgnored(t *testing.T) {
	r := secret.NewRegistryRedactor()
	r.Register("")
	r.RegisterSecret(secret.Secret{}) // zero secret -> empty value

	const in = "unaffected text"
	assert.Equal(t, in, r.Redact(in))
}

func TestRegistryRedactor_RedactsPattern(t *testing.T) {
	r := secret.NewRegistryRedactor()
	// AWS-style access key id pattern.
	require.NoError(t, r.RegisterPattern(`AKIA[0-9A-Z]{16}`))

	out := r.Redact("key AKIA0123456789ABCDEF in logs")
	assert.Contains(t, out, secret.Redacted)
	assert.NotContains(t, out, "AKIA0123456789ABCDEF")
}

func TestRegistryRedactor_BadPatternErrors(t *testing.T) {
	r := secret.NewRegistryRedactor()
	require.Error(t, r.RegisterPattern(`(unclosed`))
}

// The Redactor must be safe for concurrent use (Redact only reads; registration
// happens at setup). Run the race detector job to validate; here we at least
// exercise concurrent Redact calls.
func TestRegistryRedactor_ConcurrentRedact(t *testing.T) {
	r := secret.NewRegistryRedactor()
	r.Register("concurrent-secret")
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				out := r.Redact("x concurrent-secret y")
				if strings.Contains(out, "concurrent-secret") {
					t.Errorf("secret leaked: %q", out)
				}
			}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
}
