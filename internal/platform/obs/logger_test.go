package obs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/xd1lab/harness-ai/internal/platform/obs"
	"github.com/xd1lab/harness-ai/internal/platform/secret"
)

// providerConfig mirrors a realistic config struct that carries a secret field.
// Logging it must never reveal the wrapped value (FR-OBS-03 AC-1).
type providerConfig struct {
	Endpoint string
	APIKey   secret.Secret
}

// LogValue makes the whole struct log as a group, with the secret field still
// redacting itself.
func (p providerConfig) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("endpoint", p.Endpoint),
		slog.Any("api_key", p.APIKey),
	)
}

func TestLogger_RedactsSecretValue(t *testing.T) {
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo)

	cfg := providerConfig{Endpoint: "https://api.example.com", APIKey: secret.New("sk-super-secret-key")}
	logger.Info("provider configured", slog.Any("config", cfg))

	out := buf.String()
	assert.NotContains(t, out, "sk-super-secret-key", "raw secret leaked into log output")
	assert.Contains(t, out, secret.Redacted)

	// The output must be valid JSON (JSONHandler).
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	assert.Equal(t, "provider configured", rec["msg"])
}

func TestLogger_RedactsBareSecretAttr(t *testing.T) {
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo)

	logger.Info("resolved", slog.Any("api_key", secret.New("plaintext-token-123")))

	out := buf.String()
	assert.NotContains(t, out, "plaintext-token-123")
	assert.Contains(t, out, secret.Redacted)
}

func TestLogger_LevelFiltersBelowThreshold(t *testing.T) {
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelWarn)

	logger.Info("should be filtered")
	assert.Empty(t, strings.TrimSpace(buf.String()))

	logger.Warn("should appear")
	assert.Contains(t, buf.String(), "should appear")
}

func TestLogger_InjectsTraceAndSpanID(t *testing.T) {
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo)

	// Build a context carrying a valid, sampled span context.
	traceID, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0123456789abcdef")
	require.NoError(t, err)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	logger.InfoContext(ctx, "in a span")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	assert.Equal(t, "0123456789abcdef0123456789abcdef", rec["trace_id"])
	assert.Equal(t, "0123456789abcdef", rec["span_id"])
}

func TestLogger_NoSpanNoTraceFields(t *testing.T) {
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo)

	logger.InfoContext(context.Background(), "no span here")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	_, hasTrace := rec["trace_id"]
	_, hasSpan := rec["span_id"]
	assert.False(t, hasTrace, "trace_id must be absent without an active span")
	assert.False(t, hasSpan, "span_id must be absent without an active span")
}

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"INFO":  slog.LevelInfo,
		"Warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	for in, want := range cases {
		got, err := obs.ParseLevel(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}

	_, err := obs.ParseLevel("not-a-level")
	require.Error(t, err)
}
