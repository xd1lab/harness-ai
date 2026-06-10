package secrettest_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/platform/secret"
	"github.com/boltrope/boltrope/internal/platform/secret/secrettest"
)

func TestFakeSecrets_GetFound(t *testing.T) {
	fs := secrettest.NewFakeSecrets(map[string]string{"API_KEY": "top-secret"})
	s, err := fs.Get(context.Background(), "API_KEY")
	require.NoError(t, err)
	assert.Equal(t, "top-secret", s.Reveal())
	// Logging the secret must not reveal it.
	assert.Equal(t, secret.Redacted, s.String())
}

func TestFakeSecrets_GetNotFound(t *testing.T) {
	fs := secrettest.NewFakeSecrets(nil)
	_, err := fs.Get(context.Background(), "MISSING")
	require.Error(t, err)
	assert.ErrorIs(t, err, secret.ErrNotFound)
}

func TestFakeSecrets_Put(t *testing.T) {
	fs := secrettest.NewFakeSecrets(nil)
	fs.Put("NEW", "value")
	s, err := fs.Get(context.Background(), "NEW")
	require.NoError(t, err)
	assert.Equal(t, "value", s.Reveal())
}

func TestFakeRedactor_RedactsMatches(t *testing.T) {
	r := secrettest.NewFakeRedactor("sk-secret123")
	out := r.Redact("Authorization: Bearer sk-secret123 is the token")
	assert.Contains(t, out, secret.Redacted)
	assert.NotContains(t, out, "sk-secret123")
}

func TestFakeRedactor_NoMatchUnchanged(t *testing.T) {
	r := secrettest.NewFakeRedactor("sk-secret123")
	in := "nothing to redact here"
	assert.Equal(t, in, r.Redact(in))
}

func TestFakeRedactor_MultipleMatches(t *testing.T) {
	r := secrettest.NewFakeRedactor("abc")
	out := r.Redact("abc and abc again")
	assert.NotContains(t, out, "abc")
}
