// SPDX-License-Identifier: Apache-2.0

package projection

import (
	"context"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// TestFetchBatch_ContentAndChainHash_AndNullable (AC-2, task T1): the source
// SELECT/Scan was extended additively with content_hash + chain_hash. FetchBatch
// populates EventRow.ContentHash/ChainHash with the stored bytes for a chained
// (0009+) row, and leaves them nil for a pre-0009 NULL-hash row. The additive
// Actor/CreatedAt fields exist on EventRow (consumed by the SIEM exporter) and
// are zero-valued here (FetchBatch does not select them, by the merged
// audit-signer SELECT contract).
func TestFetchBatch_ContentAndChainHash_AndNullable(t *testing.T) {
	content := domain.ContentHash([]byte(`{"k":"v"}`))
	chain := domain.ChainHash(domain.GenesisChainHash("sess"), content)

	// Extended column order: transaction_id::text, global_id, seq, tenant_id,
	// session_id, event_type, payload, content_hash, chain_hash.
	cols := [][]any{
		// Chained 0009+ row carries the hash bytes.
		{uint64ToText(3), int64(1), int64(1), "ten", "sess", string(domain.EventTurnStarted), []byte(`{"k":"v"}`), content, chain},
		// Pre-0009 row: NULL hashes scan as nil []byte.
		{uint64ToText(3), int64(2), int64(2), "ten", "sess", string(domain.EventTurnFinished), []byte(`{}`), []byte(nil), []byte(nil)},
	}
	s := NewSource(&stubConn{rows: &fakeRows{cols: cols}})

	got, err := s.FetchBatch(context.Background(), Cursor{}, 10)
	if err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}

	// Chained row: hashes equal the stored bytes.
	if string(got[0].ContentHash) != string(content) {
		t.Fatalf("row0 ContentHash = %x, want %x", got[0].ContentHash, content)
	}
	if string(got[0].ChainHash) != string(chain) {
		t.Fatalf("row0 ChainHash = %x, want %x", got[0].ChainHash, chain)
	}

	// Pre-0009 row: hashes stay nil.
	if got[1].ContentHash != nil {
		t.Fatalf("row1 ContentHash = %x, want nil (pre-0009)", got[1].ContentHash)
	}
	if got[1].ChainHash != nil {
		t.Fatalf("row1 ChainHash = %x, want nil (pre-0009)", got[1].ChainHash)
	}

	// Additive descriptor fields exist on EventRow; FetchBatch leaves them zero.
	if got[0].Actor != "" {
		t.Fatalf("row0 Actor = %q, want empty (not selected by FetchBatch)", got[0].Actor)
	}
	if !got[0].CreatedAt.IsZero() {
		t.Fatalf("row0 CreatedAt = %v, want zero (not selected by FetchBatch)", got[0].CreatedAt)
	}

	// The other carried fields are intact (no positional drift from the new cols).
	if got[0].GlobalID != 1 || got[0].Seq != 1 || got[0].TenantID != "ten" || got[0].SessionID != "sess" {
		t.Fatalf("row0 descriptors drifted: %+v", got[0])
	}
	if got[0].Type != domain.EventTurnStarted {
		t.Fatalf("row0 Type = %q, want %q", got[0].Type, domain.EventTurnStarted)
	}
	if string(got[0].Payload) != `{"k":"v"}` {
		t.Fatalf("row0 Payload = %q, want original JSON", got[0].Payload)
	}
}
