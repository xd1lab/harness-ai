// SPDX-License-Identifier: Apache-2.0

package projection

// RED (test-first) unit tests for Batch-5B's SIEM export sink (AC-13, AC-14,
// AC-15). Authored BEFORE the implementation; they reference symbols that do
// NOT exist yet — SIEMExporter, NewSIEMExporter, the sink types, the frame
// shape — so the package does NOT compile. That absence is the RED proof.
//
// Pinned (SPEC AC-13/AC-14/AC-15):
//   - per event a JSON FRAME = { tenant_id, session_id, seq, global_id,
//     event_type, actor, created_at, content_hash (hex), chain_hash (hex) } —
//     DESCRIPTORS + HASHES ONLY, never raw payload bytes.
//   - sinks: a FILE/NDJSON sink (one JSON object per line) and an HTTP sink
//     (POST NDJSON batch, optional bearer). Operator-tier net/http, NOT the
//     egress broker.
//   - global_id is in every frame so the SIEM dedups (at-least-once cursor).
//   - KEY SECRECY / payload secrecy: a sentinel secret placed in the source
//     payload must be ABSENT from the emitted frame (file + HTTP body).

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

const siemSentinel = "SUPER-SECRET-PAYLOAD-abc123"

// siemRow builds an EventRow whose PAYLOAD carries the sentinel secret, plus a
// content_hash/chain_hash, so the no-payload assertions are meaningful.
func siemRow(gid, seq int64, tenant, session, etype string) EventRow {
	payload := []byte(`{"TurnID":"t","Secret":"` + siemSentinel + `"}`)
	return EventRow{
		GlobalID:    gid,
		Seq:         seq,
		TenantID:    tenant,
		SessionID:   session,
		Type:        domain.EventType(etype),
		Payload:     payload,
		ContentHash: domain.ContentHash(payload),
		ChainHash:   domain.ContentHash([]byte("chain-" + etype)),
	}
}

// TestSIEMExporter_FileSink_NDJSON (AC-13): the file sink appends one JSON
// object per line carrying the descriptors + hex hashes, and global_id is
// present for SIEM dedup.
func TestSIEMExporter_FileSink_NDJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "siem.ndjson")

	exp, err := NewSIEMExporter(SIEMConfig{FilePath: path})
	if err != nil {
		t.Fatalf("NewSIEMExporter: %v", err)
	}

	rows := []EventRow{
		siemRow(10, 1, "ten", "sess", string(domain.EventTurnStarted)),
		siemRow(11, 2, "ten", "sess", string(domain.EventTurnFinished)),
	}
	if err := exp.Project(context.Background(), rows); err != nil {
		t.Fatalf("Project: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open ndjson: %v", err)
	}
	defer func() { _ = f.Close() }()

	var frames []map[string]any
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("frame line is not valid JSON (%q): %v", line, err)
		}
		frames = append(frames, m)
	}
	if len(frames) != 2 {
		t.Fatalf("got %d NDJSON frames, want 2 (one per event)", len(frames))
	}

	// global_id present for dedup.
	if _, ok := frames[0]["global_id"]; !ok {
		t.Fatalf("frame missing global_id (SIEM cannot dedup): %v", frames[0])
	}
	// content_hash present as hex.
	wantHex := hex.EncodeToString(rows[0].ContentHash)
	if frames[0]["content_hash"] != wantHex {
		t.Fatalf("content_hash = %v, want hex %q", frames[0]["content_hash"], wantHex)
	}
	if frames[0]["event_type"] != string(domain.EventTurnStarted) {
		t.Fatalf("event_type = %v, want %q", frames[0]["event_type"], domain.EventTurnStarted)
	}
}

// TestSIEMExporter_FileSink_NoRawPayload (AC-15): the serialized frame bytes do
// NOT contain the sentinel secret from the source payload (descriptors + hashes
// only). The whole file content is searched.
func TestSIEMExporter_FileSink_NoRawPayload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "siem.ndjson")

	exp, err := NewSIEMExporter(SIEMConfig{FilePath: path})
	if err != nil {
		t.Fatalf("NewSIEMExporter: %v", err)
	}
	if err := exp.Project(context.Background(), []EventRow{siemRow(1, 1, "ten", "sess", string(domain.EventTurnStarted))}); err != nil {
		t.Fatalf("Project: %v", err)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read ndjson: %v", err)
	}
	if bytes.Contains(body, []byte(siemSentinel)) {
		t.Fatal("SIEM file frame leaked the raw payload secret (AC-15 violated)")
	}
	// Sanity: the frame DID carry the hashes + descriptors.
	if !bytes.Contains(body, []byte("content_hash")) || !bytes.Contains(body, []byte("event_type")) {
		t.Fatal("SIEM frame missing the expected descriptor/hash fields")
	}
}

// TestSIEMExporter_HTTPSink_PostsNDJSON_NoPayload (AC-13/14/15): the HTTP sink
// POSTs an NDJSON batch to the configured URL with the optional bearer, the body
// is payload-free, and it uses a plain net/http client (the test server is a
// stdlib httptest server, reachable without any egress broker).
func TestSIEMExporter_HTTPSink_PostsNDJSON_NoPayload(t *testing.T) {
	var gotBody []byte
	var gotAuth string
	var gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = b
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp, err := NewSIEMExporter(SIEMConfig{HTTPURL: srv.URL, HTTPBearer: "tok-xyz"})
	if err != nil {
		t.Fatalf("NewSIEMExporter: %v", err)
	}

	rows := []EventRow{
		siemRow(100, 1, "ten", "sess", string(domain.EventTurnStarted)),
		siemRow(101, 2, "ten", "sess", string(domain.EventTurnFinished)),
	}
	if err := exp.Project(context.Background(), rows); err != nil {
		t.Fatalf("Project: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Fatalf("HTTP sink method = %q, want POST", gotMethod)
	}
	if gotAuth != "Bearer tok-xyz" {
		t.Fatalf("Authorization = %q, want Bearer tok-xyz", gotAuth)
	}
	if bytes.Contains(gotBody, []byte(siemSentinel)) {
		t.Fatal("SIEM HTTP body leaked the raw payload secret (AC-15 violated)")
	}
	// NDJSON: 2 lines, each a frame carrying global_id.
	lines := strings.Split(strings.TrimSpace(string(gotBody)), "\n")
	if len(lines) != 2 {
		t.Fatalf("HTTP body has %d NDJSON lines, want 2", len(lines))
	}
	for _, ln := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			t.Fatalf("HTTP frame not JSON (%q): %v", ln, err)
		}
		if _, ok := m["global_id"]; !ok {
			t.Fatalf("HTTP frame missing global_id: %v", m)
		}
	}
}

// TestSIEMExporter_NoSinkConfigured_IsNoop (AC-13): with neither a file nor an
// HTTP url, the exporter is inert (Project is a no-op, no error) so projectord
// can construct it unconditionally and only attach when configured.
func TestSIEMExporter_NoSinkConfigured_IsNoop(t *testing.T) {
	exp, err := NewSIEMExporter(SIEMConfig{})
	if err != nil {
		t.Fatalf("NewSIEMExporter empty: %v", err)
	}
	if err := exp.Project(context.Background(), []EventRow{siemRow(1, 1, "ten", "sess", "X")}); err != nil {
		t.Fatalf("no-sink Project should be a no-op, got %v", err)
	}
}
