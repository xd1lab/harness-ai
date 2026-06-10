package grpcx_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/platform/grpcx"
)

// testTrustDomain is the SPIFFE trust domain used across grpcx tests. It mirrors
// the architecture's example domain (architecture §8.1).
var testTrustDomain = spiffeid.RequireTrustDomainFromString("boltrope.local")

// mustID builds a SPIFFE ID under the test trust domain or fails the test.
func mustID(t *testing.T, path string) spiffeid.ID {
	t.Helper()
	id, err := spiffeid.FromPath(testTrustDomain, path)
	require.NoError(t, err)
	return id
}

// TestStaticDevCredentials_RefusesWithoutOptIn is the NFR-TEST-05(g) guard: the
// static-cert dev fallback fails closed. With BOLTROPE_DEV_INSECURE unset (or any
// value other than "1") the constructor MUST return ErrDevInsecureNotEnabled and
// MUST NOT return usable credentials, so a production deployment can never
// silently downgrade to ephemeral static certs (architecture §8.1, NFR-SEC-01).
func TestStaticDevCredentials_RefusesWithoutOptIn(t *testing.T) {
	for _, envVal := range []string{"", "0", "true", "yes", "2"} {
		t.Run("env="+envVal, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, nil))

			creds, err := grpcx.StaticDevCredentials(grpcx.StaticDevConfig{
				TrustDomain: testTrustDomain,
				ServerID:    mustID(t, "/ns/default/sa/orchestrator"),
				Logger:      logger,
				LookupEnv:   func(string) (string, bool) { return envVal, envVal != "" },
			})
			require.Error(t, err)
			assert.Nil(t, creds, "no credentials must be handed back when the dev fallback is refused")
			assert.ErrorIs(t, err, grpcx.ErrDevInsecureNotEnabled)
			// The error must name the gating variable so the operator can fix it.
			assert.Contains(t, err.Error(), "BOLTROPE_DEV_INSECURE")
		})
	}
}

// TestStaticDevCredentials_StartsWithOptInAndWarns asserts the converse: with
// BOLTROPE_DEV_INSECURE=1 the constructor returns usable transport credentials
// AND logs a loud, WARN-level warning so the insecure mode is never silent
// (architecture §8.1, NFR-SEC-01).
func TestStaticDevCredentials_StartsWithOptInAndWarns(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	creds, err := grpcx.StaticDevCredentials(grpcx.StaticDevConfig{
		TrustDomain: testTrustDomain,
		ServerID:    mustID(t, "/ns/default/sa/orchestrator"),
		Logger:      logger,
		LookupEnv:   func(string) (string, bool) { return "1", true },
	})
	require.NoError(t, err)
	require.NotNil(t, creds)
	// It must be a real gRPC TransportCredentials (TLS), not insecure passthrough.
	assert.NotEqual(t, "insecure", creds.Info().SecurityProtocol)

	// The warning must have been emitted at WARN level and must mention the
	// gating variable so it is unmistakable in logs.
	out := buf.String()
	require.NotEmpty(t, out, "a loud warning must be logged when the dev fallback engages")
	var rec map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.SplitN(out, "\n", 2)[0]), &rec))
	assert.Equal(t, "WARN", rec["level"])
	assert.Contains(t, strings.ToUpper(out), "BOLTROPE_DEV_INSECURE")
}

// TestStaticDevCredentials_DefaultLookupUsesProcessEnv asserts that when no
// LookupEnv is injected the constructor reads the real process environment, so
// production wiring (which does not inject a lookup) still fails closed.
func TestStaticDevCredentials_DefaultLookupUsesProcessEnv(t *testing.T) {
	// The test process does not set BOLTROPE_DEV_INSECURE, so the default lookup
	// must refuse. (We deliberately do not set it to keep the suite hermetic.)
	t.Setenv("BOLTROPE_DEV_INSECURE", "")
	creds, err := grpcx.StaticDevCredentials(grpcx.StaticDevConfig{
		TrustDomain: testTrustDomain,
		ServerID:    mustID(t, "/ns/default/sa/orchestrator"),
	})
	require.Error(t, err)
	assert.Nil(t, creds)
	assert.True(t, errors.Is(err, grpcx.ErrDevInsecureNotEnabled))
}
