package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// byNameForTest builds the name->spec lookup used by filterKnownFlags from the
// production flagSpecs table, so the test exercises the real flag surface.
func byNameForTest() map[string]flagSpec {
	m := make(map[string]flagSpec, len(flagSpecs))
	for _, s := range flagSpecs {
		m[s.name] = s
	}
	return m
}

func TestFilterKnownFlags(t *testing.T) {
	byName := byNameForTest()

	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "drops foreign test-runner flags",
			in:   []string{"-test.v", "-test.testlogfile=/tmp/x", "--log-level=info"},
			want: []string{"--log-level=info"},
		},
		{
			name: "keeps known equals form",
			in:   []string{"--postgres-version=15"},
			want: []string{"--postgres-version=15"},
		},
		{
			name: "keeps known space-separated value form",
			in:   []string{"--log-level", "warn"},
			want: []string{"--log-level", "warn"},
		},
		{
			name: "single-dash known flag is kept",
			in:   []string{"-grpc-addr", ":9000"},
			want: []string{"-grpc-addr", ":9000"},
		},
		{
			name: "bool flag does not swallow following token",
			in:   []string{"--dev-insecure", "positional"},
			want: []string{"--dev-insecure"},
		},
		{
			name: "double-dash terminator stops parsing",
			in:   []string{"--log-level=info", "--", "--postgres-dsn=leaked"},
			want: []string{"--log-level=info"},
		},
		{
			name: "foreign space-separated value is dropped with its flag",
			in:   []string{"--unknown-flag", "value", "--blob-dir=/b"},
			want: []string{"--blob-dir=/b"},
		},
		{
			name: "empty in empty out",
			in:   nil,
			want: []string{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := filterKnownFlags(tc.in, byName)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTransformEnvKey(t *testing.T) {
	cases := []struct {
		in      string
		wantKey string
	}{
		{"BOLTROPE_LOG_LEVEL", "log_level"},
		{"BOLTROPE_DEV_INSECURE", "dev_insecure"},
		{"BOLTROPE_POSTGRES__DSN", "postgres.dsn"},
		{"BOLTROPE_POSTGRES__VERSION", "postgres.version"},
		{"BOLTROPE_SERVER__GRPC_ADDR", "server.grpc_addr"},
		{"BOLTROPE_MODEL_GATEWAY__API_KEY_ENV", "model_gateway.api_key_env"},
		{"BOLTROPE_OTLP__INSECURE", "otlp.insecure"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			gotKey, gotVal := transformEnvKey(tc.in, "v")
			assert.Equal(t, tc.wantKey, gotKey)
			assert.Equal(t, "v", gotVal, "value must pass through unchanged for koanf weak-typing")
		})
	}
}

// TestMapProvider_UnflattensDottedKeys verifies the local confmap replacement
// turns dot-delimited keys into the nested shape koanf merges, matching confmap.
func TestMapProvider_UnflattensDottedKeys(t *testing.T) {
	p := mapProvider(map[string]any{
		"postgres.dsn":     "x",
		"postgres.version": 15,
		"log_level":        "info",
	}, keyDelim)

	got, err := p.Read()
	assert.NoError(t, err)

	pg, ok := got["postgres"].(map[string]any)
	if assert.True(t, ok, "postgres key must be a nested map after unflatten") {
		assert.Equal(t, "x", pg["dsn"])
		assert.Equal(t, 15, pg["version"])
	}
	assert.Equal(t, "info", got["log_level"])

	_, err = p.ReadBytes()
	assert.ErrorIs(t, err, errUnsupportedReadBytes)
}
