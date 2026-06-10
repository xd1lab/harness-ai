package blob_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/boltrope/boltrope/internal/platform/blob"
)

// newTestFS creates a filesystem-backed store rooted in a temp dir.
func newTestFS(t *testing.T, maxBytes int64) *blob.FSStore {
	t.Helper()
	return blob.NewFSStore(t.TempDir(), maxBytes)
}

func ref(tenant, key string) blob.Ref { return blob.Ref{TenantID: tenant, Key: key} }

var ctx = context.Background()

// TestFSStore_RoundTrip_PutGetStat verifies that bytes written via Put are
// returned verbatim by Get, and that Stat reflects the stored size and
// media-type without re-reading the payload bytes.
func TestFSStore_RoundTrip_PutGetStat(t *testing.T) {
	fs := newTestFS(t, 0) // 0 = no size limit

	payload := []byte("hello filesystem blob")
	r := ref("tenant-alpha", "key-001")

	obj, err := fs.Put(ctx, r, "text/plain", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Put: unexpected error: %v", err)
	}
	if obj.SizeBytes != int64(len(payload)) {
		t.Errorf("Put returned SizeBytes=%d, want %d", obj.SizeBytes, len(payload))
	}
	if obj.MediaType != "text/plain" {
		t.Errorf("Put returned MediaType=%q, want %q", obj.MediaType, "text/plain")
	}
	if obj.Ref != r {
		t.Errorf("Put returned Ref=%+v, want %+v", obj.Ref, r)
	}

	// Get must return identical bytes and matching metadata.
	gotObj, rc, err := fs.Get(ctx, r)
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	defer func() {
		if cerr := rc.Close(); cerr != nil {
			t.Errorf("rc.Close: %v", cerr)
		}
	}()

	gotBytes, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll after Get: %v", err)
	}
	if !bytes.Equal(gotBytes, payload) {
		t.Errorf("Get bytes mismatch: got %q, want %q", gotBytes, payload)
	}
	if gotObj.SizeBytes != obj.SizeBytes {
		t.Errorf("Get SizeBytes=%d, want %d", gotObj.SizeBytes, obj.SizeBytes)
	}
	if gotObj.MediaType != obj.MediaType {
		t.Errorf("Get MediaType=%q, want %q", gotObj.MediaType, obj.MediaType)
	}

	// Stat must match without returning bytes.
	statObj, err := fs.Stat(ctx, r)
	if err != nil {
		t.Fatalf("Stat: unexpected error: %v", err)
	}
	if statObj.SizeBytes != obj.SizeBytes {
		t.Errorf("Stat SizeBytes=%d, want %d", statObj.SizeBytes, obj.SizeBytes)
	}
	if statObj.MediaType != obj.MediaType {
		t.Errorf("Stat MediaType=%q, want %q", statObj.MediaType, obj.MediaType)
	}
}

// TestFSStore_ErrTooLarge verifies that Put rejects payloads that exceed the
// configured maximum and returns blob.ErrTooLarge via errors.Is.
func TestFSStore_ErrTooLarge(t *testing.T) {
	const maxBytes = 8
	fs := newTestFS(t, maxBytes)

	payload := strings.Repeat("x", maxBytes+1)
	_, err := fs.Put(ctx, ref("t", "k"), "text/plain", strings.NewReader(payload))
	if err == nil {
		t.Fatal("Put: expected ErrTooLarge, got nil")
	}
	if !errors.Is(err, blob.ErrTooLarge) {
		t.Errorf("Put: got %v, want errors.Is(_, blob.ErrTooLarge)", err)
	}
}

// TestFSStore_TenantIsolation verifies that:
//  1. A Ref stored under tenant-A cannot be read by the same Key under tenant-B.
//  2. The error for a wrong-tenant lookup is ErrNotFound, indistinguishable from
//     an absent ref — no existence oracle (NFR-SEC-03).
func TestFSStore_TenantIsolation(t *testing.T) {
	fs := newTestFS(t, 0)

	const sharedKey = "same-key"
	payloadA := []byte("tenant A's private data")

	// Store under tenant-A.
	_, err := fs.Put(ctx, ref("tenant-A", sharedKey), "application/octet-stream", bytes.NewReader(payloadA))
	if err != nil {
		t.Fatalf("Put tenant-A: %v", err)
	}

	// Attempt Get with tenant-B using the same Key.
	_, _, err = fs.Get(ctx, ref("tenant-B", sharedKey))
	if !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Get cross-tenant: got %v, want blob.ErrNotFound", err)
	}

	// Attempt Stat with tenant-B using the same Key.
	_, err = fs.Stat(ctx, ref("tenant-B", sharedKey))
	if !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Stat cross-tenant: got %v, want blob.ErrNotFound", err)
	}

	// Absent ref for tenant-C must also return ErrNotFound — same sentinel,
	// same code path, no distinction (NFR-SEC-03).
	_, _, err = fs.Get(ctx, ref("tenant-C", "totally-absent"))
	if !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Get absent: got %v, want blob.ErrNotFound", err)
	}
	_, err = fs.Stat(ctx, ref("tenant-C", "totally-absent"))
	if !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Stat absent: got %v, want blob.ErrNotFound", err)
	}
}

// TestFSStore_IdempotentRePut verifies that calling Put twice with the same
// (tenant, ref) succeeds on both calls without error and leaves the stored
// bytes consistent (the second call is a no-op / overwrite returning the same
// metadata).
func TestFSStore_IdempotentRePut(t *testing.T) {
	fs := newTestFS(t, 0)

	r := ref("tenant-X", "idem-key")
	payload := []byte("idempotent content")
	mediaType := "text/plain"

	obj1, err := fs.Put(ctx, r, mediaType, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}

	obj2, err := fs.Put(ctx, r, mediaType, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("second Put (re-Put): %v", err)
	}

	if obj1.SizeBytes != obj2.SizeBytes {
		t.Errorf("re-Put SizeBytes mismatch: first=%d second=%d", obj1.SizeBytes, obj2.SizeBytes)
	}

	// Reading after re-Put must still return the correct content.
	_, rc, err := fs.Get(ctx, r)
	if err != nil {
		t.Fatalf("Get after re-Put: %v", err)
	}
	defer func() {
		if cerr := rc.Close(); cerr != nil {
			t.Errorf("rc.Close: %v", cerr)
		}
	}()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, payload) {
		t.Errorf("Get after re-Put: got %q, want %q", got, payload)
	}
}
