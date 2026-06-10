package obs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// Log attribute keys for trace correlation. They match the conventional names a
// collector/Loki query joins on (NFR-OBS-02).
const (
	traceIDKey = "trace_id"
	spanIDKey  = "span_id"
)

// NewLogger returns a production [*slog.Logger] writing newline-delimited JSON to
// w at the given minimum level. The handler:
//
//   - uses [slog.JSONHandler] (FR-OBS-03: JSON in production);
//   - honors [slog.LogValuer], so a
//     [github.com/boltrope/boltrope/internal/platform/secret.Secret] (or any
//     value implementing LogValuer to mask itself) is recorded as "[REDACTED]",
//     never its plaintext;
//   - injects trace_id and span_id from the active OTel span carried on the
//     record's context, when one is present and valid (NFR-OBS-02).
//
// Pass the resolved level (see [ParseLevel]); the logger is safe for concurrent
// use.
func NewLogger(w io.Writer, level slog.Leveler) *slog.Logger {
	base := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(&traceHandler{Handler: base})
}

// NewHandler returns the same trace-injecting JSON [slog.Handler] that [NewLogger]
// builds, for callers that want to install it as the default handler via
// [slog.SetDefault] or compose it further.
func NewHandler(w io.Writer, level slog.Leveler) slog.Handler {
	return &traceHandler{Handler: slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})}
}

// traceHandler decorates an inner [slog.Handler] by adding trace_id/span_id
// attributes derived from the active span on the record's context. It delegates
// every other concern (level filtering, attribute encoding, LogValuer resolution)
// to the inner JSON handler, so secret redaction is handled by slog itself.
type traceHandler struct {
	slog.Handler
}

// Handle adds the trace_id/span_id attributes (when an active, valid span is on
// ctx) and forwards the record to the inner handler. The record is copied before
// mutation so a shared record is never altered for other handlers.
func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		// Clone to avoid mutating a record the caller may reuse.
		rc := r.Clone()
		rc.AddAttrs(
			slog.String(traceIDKey, sc.TraceID().String()),
			slog.String(spanIDKey, sc.SpanID().String()),
		)
		return h.Handler.Handle(ctx, rc)
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs returns a new traceHandler whose inner handler carries attrs, so
// trace injection is preserved across logger.With(...) chains.
func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithAttrs(attrs)}
}

// WithGroup returns a new traceHandler whose inner handler is grouped, preserving
// trace injection across logger.WithGroup(...) chains.
func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithGroup(name)}
}

// ParseLevel maps a case-insensitive level name ("debug", "info", "warn",
// "error") to a [slog.Level]. It returns an error for an unrecognized name so a
// misconfigured log level fails fast at startup rather than silently defaulting.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "": // empty defaults to info
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("obs: unknown log level %q (want debug|info|warn|error)", s)
	}
}

// Compile-time assertion that traceHandler satisfies slog.Handler.
var _ slog.Handler = (*traceHandler)(nil)
