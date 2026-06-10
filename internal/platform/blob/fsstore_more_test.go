// This file extends fsstore_test.go with Delete semantics, Ref.IsZero, and the
// filesystem failure paths (corrupt/unreadable metadata, missing payload,
// failed reads, and the atomic-write rename failure with temp-file cleanup).
// Failure injection uses only portable filesystem shapes — a directory sitting
// where a file is expected fails identically on Windows and POSIX — so no
// chmod tricks or platform build tags are needed.
package blob_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/boltrope/boltrope/internal/platform/blob"
)

// TestRefIsZero pins the unset-ref predicate: only the fully empty composite
// key is zero; a half-set ref still names something.
func TestRefIsZero(t *testing.T) {
	cases := []struct {
		name string
		r    blob.Ref
		want bool
	}{
		{"both empty", blob.Ref{}, true},
		{"tenant only", blob.Ref{TenantID: "t"}, false},
		{"key only", blob.Ref{Key: "k"}, false},
		{"both set", blob.Ref{TenantID: "t", Key: "k"}, false},
	}
	for _, tc := range cases {
		if got := tc.r.IsZero(); got != tc.want {
			t.Errorf("%s: Ref%+v.IsZero() = %v, want %v", tc.name, tc.r, got, tc.want)
		}
	}
}

// TestFSStore_DeleteRemovesAndIsIdempotent verifies Delete removes both the
// payload and the metadata (subsequent Get/Stat see ErrNotFound) and that
// deleting an absent or already-deleted blob is a nil-error no-op.
func TestFSStore_DeleteRemovesAndIsIdempotent(t *testing.T) {
	fs := newTestFS(t, 0)
	r := ref("tenant-del", "key-del")

	if _, err := fs.Put(ctx, r, "text/plain", strings.NewReader("to be deleted")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := fs.Delete(ctx, r); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, _, err := fs.Get(ctx, r); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Get after Delete: got %v, want blob.ErrNotFound", err)
	}
	if _, err := fs.Stat(ctx, r); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Stat after Delete: got %v, want blob.ErrNotFound", err)
	}

	// Idempotent: a second Delete and a Delete of a never-stored ref are no-ops.
	if err := fs.Delete(ctx, r); err != nil {
		t.Errorf("second Delete: got %v, want nil (idempotent)", err)
	}
	if err := fs.Delete(ctx, ref("tenant-del", "never-existed")); err != nil {
		t.Errorf("Delete of absent ref: got %v, want nil (idempotent)", err)
	}
}

// TestFSStore_DeleteSurfacesRemoveFailure verifies that a remove failure other
// than not-exist is reported. A non-empty directory sitting at the payload
// path makes os.Remove fail on every platform.
func TestFSStore_DeleteSurfacesRemoveFailure(t *testing.T) {
	root := t.TempDir()
	fs := blob.NewFSStore(root, 0)

	payloadDir := filepath.Join(root, "ten", "k.blob")
	if err := os.MkdirAll(payloadDir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(payloadDir, "child"), []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := fs.Delete(ctx, ref("ten", "k")); err == nil {
		t.Fatal("Delete: expected error removing a non-empty directory at the payload path, got nil")
	}
}

// TestFSStore_GetCorruptMetadataErrors verifies that metadata which exists but
// does not parse is a real error (NOT ErrNotFound — the blob is present but
// damaged, and reporting absence would mislead the integrity check).
func TestFSStore_GetCorruptMetadataErrors(t *testing.T) {
	root := t.TempDir()
	fs := blob.NewFSStore(root, 0)

	dir := filepath.Join(root, "ten")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "k.meta.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, _, err := fs.Get(ctx, ref("ten", "k"))
	if err == nil {
		t.Fatal("Get: expected error for corrupt metadata, got nil")
	}
	if errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Get: corrupt metadata must not be reported as ErrNotFound, got %v", err)
	}
	if !strings.Contains(err.Error(), "parsing metadata") {
		t.Errorf("Get: expected a parsing-metadata error, got %v", err)
	}

	if _, err := fs.Stat(ctx, ref("ten", "k")); err == nil || errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Stat: corrupt metadata must be a non-NotFound error, got %v", err)
	}
}

// TestFSStore_GetMetadataWithoutPayloadIsNotFound verifies the crash window
// where metadata exists but the payload file is gone: Get reports the same
// ErrNotFound sentinel (no partial-blob oracle).
func TestFSStore_GetMetadataWithoutPayloadIsNotFound(t *testing.T) {
	root := t.TempDir()
	fs := blob.NewFSStore(root, 0)

	dir := filepath.Join(root, "ten")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	meta := []byte(`{"media_type":"text/plain","size_bytes":5}`)
	if err := os.WriteFile(filepath.Join(dir, "k.meta.json"), meta, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	// No k.blob payload alongside the metadata.

	_, _, err := fs.Get(ctx, ref("ten", "k"))
	if !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Get: got %v, want blob.ErrNotFound for metadata without payload", err)
	}
}

// TestFSStore_UnreadableMetadataErrors verifies a metadata read failure other
// than not-exist (a directory at the metadata path) is wrapped, not mapped to
// ErrNotFound.
func TestFSStore_UnreadableMetadataErrors(t *testing.T) {
	root := t.TempDir()
	fs := blob.NewFSStore(root, 0)

	if err := os.MkdirAll(filepath.Join(root, "ten", "k.meta.json"), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	_, err := fs.Stat(ctx, ref("ten", "k"))
	if err == nil {
		t.Fatal("Stat: expected error reading a directory as metadata, got nil")
	}
	if errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Stat: an IO failure must not masquerade as ErrNotFound, got %v", err)
	}
}

// TestFSStore_PutReaderFailurePropagates verifies a failing payload reader
// aborts the Put on both the limited (maxBytes set) and unlimited read paths,
// without writing anything.
func TestFSStore_PutReaderFailurePropagates(t *testing.T) {
	readErr := errors.New("upstream stream broke")
	for _, maxBytes := range []int64{0, 1024} {
		fs := newTestFS(t, maxBytes)
		_, err := fs.Put(ctx, ref("ten", "k"), "text/plain", iotest.ErrReader(readErr))
		if !errors.Is(err, readErr) {
			t.Errorf("maxBytes=%d: Put with failing reader: got %v, want wrapped %v", maxBytes, err, readErr)
		}
	}
}

// TestFSStore_PutTenantDirCreationFailure verifies that a file squatting on
// the tenant directory path fails the Put with a creating-tenant-dir error.
func TestFSStore_PutTenantDirCreationFailure(t *testing.T) {
	root := t.TempDir()
	fs := blob.NewFSStore(root, 0)

	// A regular FILE where the tenant directory must go.
	if err := os.WriteFile(filepath.Join(root, "squatter"), []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := fs.Put(ctx, ref("squatter", "k"), "text/plain", strings.NewReader("data"))
	if err == nil {
		t.Fatal("Put: expected error when the tenant dir path is a file, got nil")
	}
	if !strings.Contains(err.Error(), "creating tenant dir") {
		t.Errorf("Put: expected creating-tenant-dir error, got %v", err)
	}
}

// TestFSStore_PutPayloadRenameFailureCleansTemp verifies the atomic-write
// failure path: when the final rename cannot land (a directory occupies the
// payload path), Put errors AND the temp file is removed — no .blob-tmp-*
// litter that the sweeper would have to chase.
func TestFSStore_PutPayloadRenameFailureCleansTemp(t *testing.T) {
	root := t.TempDir()
	fs := blob.NewFSStore(root, 0)

	tenantDir := filepath.Join(root, "ten")
	if err := os.MkdirAll(filepath.Join(tenantDir, "k.blob"), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	_, err := fs.Put(ctx, ref("ten", "k"), "text/plain", strings.NewReader("payload"))
	if err == nil {
		t.Fatal("Put: expected error when the payload path is a directory, got nil")
	}
	if !strings.Contains(err.Error(), "writing payload") {
		t.Errorf("Put: expected writing-payload error, got %v", err)
	}

	leftovers, globErr := filepath.Glob(filepath.Join(tenantDir, ".blob-tmp-*"))
	if globErr != nil {
		t.Fatalf("Glob: %v", globErr)
	}
	if len(leftovers) != 0 {
		t.Errorf("temp files must be cleaned up after a failed rename; found %v", leftovers)
	}
}

// TestFSStore_PutMetadataRenameFailure verifies the second atomic write (the
// metadata file) failing is also surfaced, after the payload landed.
func TestFSStore_PutMetadataRenameFailure(t *testing.T) {
	root := t.TempDir()
	fs := blob.NewFSStore(root, 0)

	tenantDir := filepath.Join(root, "ten")
	if err := os.MkdirAll(filepath.Join(tenantDir, "k.meta.json"), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	_, err := fs.Put(ctx, ref("ten", "k"), "text/plain", strings.NewReader("payload"))
	if err == nil {
		t.Fatal("Put: expected error when the metadata path is a directory, got nil")
	}
	if !strings.Contains(err.Error(), "writing metadata") {
		t.Errorf("Put: expected writing-metadata error, got %v", err)
	}
}

// TestWriteFileAtomic_TempCreationFailure verifies the very first step of the
// atomic write failing (the temp file cannot be created because the target
// directory does not exist) is wrapped and surfaced. This branch is
// unreachable through Put, which always creates the directory first, so it is
// exercised via the export_test handle.
func TestWriteFileAtomic_TempCreationFailure(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-dir", "file.blob")

	err := blob.WriteFileAtomicForTest(missing, []byte("data"), 0o600)
	if err == nil {
		t.Fatal("writeFileAtomic: expected error for a non-existent directory, got nil")
	}
	if !strings.Contains(err.Error(), "creating temp file") {
		t.Errorf("expected creating-temp-file error, got %v", err)
	}
}

// TestFSStore_PathTraversalKeysAreConfined verifies that a hostile key/tenant
// containing separators or ".." is sanitized into the tenant directory rather
// than escaping the store root: the blob round-trips under its (sanitized)
// identity and nothing is written outside root.
func TestFSStore_PathTraversalKeysAreConfined(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "store-root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	fs := blob.NewFSStore(root, 0)

	hostile := ref("../../tenant", "../../../escape-key")
	payload := []byte("confined bytes")
	if _, err := fs.Put(ctx, hostile, "text/plain", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put with hostile ref: %v", err)
	}

	// Round trip under the same (hostile) ref still works.
	_, rc, err := fs.Get(ctx, hostile)
	if err != nil {
		t.Fatalf("Get with hostile ref: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("hostile-ref round trip: got %q, want %q", got, payload)
	}

	// Nothing escaped the store root: the parent dir contains ONLY the root.
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "store-root" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("hostile ref escaped the store root; parent now contains %v", names)
	}
}
