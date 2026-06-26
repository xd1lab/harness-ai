// SPDX-License-Identifier: Apache-2.0

package projection

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/xd1lab/harness-ai/internal/platform/secret"
)

// DefaultSIEMSubscription is the default event_subscriptions row name the SIEM
// exporter owns (AC-13). It is DISTINCT from the cost-rollup and audit-checkpoint
// subscriptions so the exporter's cursor advances independently — a failing SIEM
// sink stalls only its own subscription, never cost-rollup (AC-17).
const DefaultSIEMSubscription = "siem-export"

// defaultSIEMHTTPTimeout bounds an HTTP-sink POST so a slow/hung SIEM endpoint
// cannot wedge the exporter's catch-up loop. The exporter uses a plain
// net/http.Client (operator-tier, NOT the egress broker; AC-14).
const defaultSIEMHTTPTimeout = 10 * time.Second

// SIEMConfig configures a [SIEMExporter]. Each sink is active ONLY when its field
// is set, so projectord can construct the exporter unconditionally and attach it
// only when a sink is configured (AC-13). Neither sink set => the exporter is a
// no-op.
type SIEMConfig struct {
	// FilePath, when set (BOLTROPE_SIEM_FILE), enables the FILE/NDJSON sink:
	// each frame is appended as one JSON object per line.
	FilePath string
	// HTTPURL, when set (BOLTROPE_SIEM_HTTP_URL), enables the HTTP sink: each
	// batch is POSTed as an NDJSON body. A plain net/http client is used.
	HTTPURL string
	// HTTPBearer is the optional bearer token (BOLTROPE_SIEM_HTTP_BEARER) sent as
	// `Authorization: Bearer <token>` on the HTTP sink. It is wrapped in a
	// [secret.Secret] internally so it redacts in logs (AC-13). The wiring layer
	// resolves it via [secret.SecretsPort] and passes the revealed value here.
	HTTPBearer string
}

// siemFrame is the audit FRAME emitted per event (AC-13/AC-15). It carries
// DESCRIPTORS + HASHES ONLY — there is DELIBERATELY no Payload / payload_canonical
// field, so a raw payload (which may carry secrets) can NEVER be serialized into a
// SIEM frame. content_hash / chain_hash are hex strings; global_id is in every
// frame so the SIEM dedups under the at-least-once cursor.
type siemFrame struct {
	TenantID    string `json:"tenant_id"`
	SessionID   string `json:"session_id"`
	Seq         int64  `json:"seq"`
	GlobalID    int64  `json:"global_id"`
	EventType   string `json:"event_type"`
	Actor       string `json:"actor"`
	CreatedAt   string `json:"created_at"`
	ContentHash string `json:"content_hash"`
	ChainHash   string `json:"chain_hash"`
}

// SIEMExporter is the operator-tier projection sink that ships an audit FRAME per
// event to an external, independent sink so evidence survives a full-DB compromise
// (Batch-5B, ADR-0034). It tails the GLOBAL event feed through the projection
// [Runner] on its OWN subscription (default "siem-export"), and for each event
// emits a descriptors+hashes-only JSON frame to the configured file and/or HTTP
// sink.
//
// Like [CostProjector] it NEVER touches the append/hot path and errors BEFORE the
// runner advances the cursor (a sink failure returns from [SIEMExporter.Project],
// so the batch is re-read next poll; the SIEM dedups on global_id — at-least-once).
//
// # Trust boundary (AC-14)
//
// The HTTP sink uses a plain net/http.Client, NOT the toolruntime EgressBroker. The
// SIEM exporter is OPERATOR-TIER infrastructure (like OTLP/metrics export), and the
// egress broker (ADR-0013) governs only MODEL-INFLUENCED channels. This file does
// NOT import any egress package.
//
// # Payload secrecy (AC-15)
//
// The emitted [siemFrame] has NO payload field. The raw event payload (which may
// carry secrets) is never serialized into a frame — only content/chain hashes and
// non-sensitive descriptors. The optional bearer is held as a [secret.Secret] so
// it redacts in logs.
type SIEMExporter struct {
	filePath   string
	httpURL    string
	httpBearer secret.Secret
	httpClient *http.Client
}

// NewSIEMExporter constructs a [SIEMExporter] for cfg. When neither sink is
// configured the exporter is inert ([SIEMExporter.Project] is a no-op), so
// projectord can construct it unconditionally. It never returns an error today,
// but the signature reserves room for future validation (e.g. URL parsing).
func NewSIEMExporter(cfg SIEMConfig) (*SIEMExporter, error) {
	e := &SIEMExporter{
		filePath: cfg.FilePath,
		httpURL:  cfg.HTTPURL,
	}
	if cfg.HTTPBearer != "" {
		e.httpBearer = secret.New(cfg.HTTPBearer)
	}
	if cfg.HTTPURL != "" {
		e.httpClient = &http.Client{Timeout: defaultSIEMHTTPTimeout}
	}
	return e, nil
}

// enabled reports whether at least one sink is configured.
func (e *SIEMExporter) enabled() bool {
	return e.filePath != "" || e.httpURL != ""
}

// Project emits one SIEM frame per event in rows to every configured sink, in
// order. It is a no-op when no sink is configured (so an unconfigured exporter can
// be attached harmlessly). It errors BEFORE the runner advances the cursor: a sink
// failure returns here, so the batch is re-read next poll and the SIEM dedups on
// global_id (at-least-once, never skip; NFR-REL-04).
func (e *SIEMExporter) Project(ctx context.Context, rows []EventRow) error {
	if !e.enabled() || len(rows) == 0 {
		return nil
	}

	frames := make([]siemFrame, 0, len(rows))
	for _, r := range rows {
		frames = append(frames, frameOf(r))
	}

	// Render the NDJSON body once; both sinks consume the same payload-free bytes.
	var ndjson bytes.Buffer
	enc := json.NewEncoder(&ndjson)
	for i := range frames {
		if err := enc.Encode(frames[i]); err != nil {
			return fmt.Errorf("projection: encoding SIEM frame (global_id=%d): %w", frames[i].GlobalID, err)
		}
	}

	if e.filePath != "" {
		if err := e.writeFile(ndjson.Bytes()); err != nil {
			return err
		}
	}
	if e.httpURL != "" {
		if err := e.postHTTP(ctx, ndjson.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

// frameOf builds the descriptors+hashes-only frame for one event row. It NEVER
// reads r.Payload — the raw payload is deliberately excluded from the frame (AC-15).
func frameOf(r EventRow) siemFrame {
	var createdAt string
	if !r.CreatedAt.IsZero() {
		createdAt = r.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	return siemFrame{
		TenantID:    r.TenantID,
		SessionID:   r.SessionID,
		Seq:         r.Seq,
		GlobalID:    r.GlobalID,
		EventType:   string(r.Type),
		Actor:       r.Actor,
		CreatedAt:   createdAt,
		ContentHash: hex.EncodeToString(r.ContentHash),
		ChainHash:   hex.EncodeToString(r.ChainHash),
	}
}

// writeFile appends the NDJSON batch to the file sink (one JSON object per line).
// It opens append-only so concurrent restarts never truncate prior evidence.
func (e *SIEMExporter) writeFile(ndjson []byte) error {
	f, err := os.OpenFile(e.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("projection: opening SIEM file %q: %w", e.filePath, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(ndjson); err != nil {
		return fmt.Errorf("projection: writing SIEM file %q: %w", e.filePath, err)
	}
	return nil
}

// postHTTP POSTs the NDJSON batch to the HTTP sink with the optional bearer. It
// uses a plain net/http.Client (operator-tier, NOT the egress broker; AC-14). A
// non-2xx response is an error so the runner re-reads the batch (the SIEM dedups
// on global_id).
func (e *SIEMExporter) postHTTP(ctx context.Context, ndjson []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.httpURL, bytes.NewReader(ndjson))
	if err != nil {
		return fmt.Errorf("projection: building SIEM HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if !e.httpBearer.IsZero() {
		req.Header.Set("Authorization", "Bearer "+e.httpBearer.Reveal())
	}
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("projection: POSTing SIEM batch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	// Drain so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("projection: SIEM HTTP sink returned status %d", resp.StatusCode)
	}
	return nil
}
