package secret_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/platform/secret"
)

func TestEnvSecrets_GetUnsetReturnsNotFound(t *testing.T) {
	es := secret.NewEnvSecrets()
	_, err := es.Get(context.Background(), "BOLTROPE_TEST_DEFINITELY_UNSET_VAR")
	require.Error(t, err)
	assert.ErrorIs(t, err, secret.ErrNotFound)
}

func TestEnvSecrets_GetSetReturnsValue(t *testing.T) {
	const name = "BOLTROPE_TEST_SECRET_ENV"
	t.Setenv(name, "the-value")

	es := secret.NewEnvSecrets()
	s, err := es.Get(context.Background(), name)
	require.NoError(t, err)
	assert.Equal(t, "the-value", s.Reveal())
	// The resolved secret still redacts when logged/printed.
	assert.Equal(t, secret.Redacted, s.String())
}

// An env var that is set but empty is treated as not-found (an empty credential
// is never a valid secret), so a missing required credential fails fast.
func TestEnvSecrets_EmptyTreatedAsNotFound(t *testing.T) {
	const name = "BOLTROPE_TEST_EMPTY_SECRET_ENV"
	t.Setenv(name, "")

	es := secret.NewEnvSecrets()
	_, err := es.Get(context.Background(), name)
	require.Error(t, err)
	assert.ErrorIs(t, err, secret.ErrNotFound)
}

// A prefix scopes lookups so callers ask for a logical name and the backend maps
// it to PREFIX+NAME in the environment.
func TestEnvSecrets_WithPrefix(t *testing.T) {
	t.Setenv("BOLTROPE_SECRET_ANTHROPIC_API_KEY", "sk-xyz")

	es := secret.NewEnvSecrets(secret.WithEnvPrefix("BOLTROPE_SECRET_"))
	s, err := es.Get(context.Background(), "ANTHROPIC_API_KEY")
	require.NoError(t, err)
	assert.Equal(t, "sk-xyz", s.Reveal())
}

func TestEnvSecrets_SatisfiesPort(_ *testing.T) {
	var _ secret.SecretsPort = secret.NewEnvSecrets()
}
