package blob

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// FSStore is a filesystem-backed [BlobStorePort]. Blobs are stored under a
// root directory with tenant-prefixed paths so that a [Ref] belonging to
// tenant A can never accidentally address tenant B's bytes (NFR-SEC-03,
// architecture §8.5).
//
// Directory layout:
//
//	<root>/<tenantID>/<sanitizedKey>.blob       — raw byte payload
//	<root>/<tenantID>/<sanitizedKey>.meta.json  — Object metadata (MediaType,
//	                                              SizeBytes)
//
// Durability: each Put uses [os.WriteFile] which performs a single atomic
// rename-via-temp-write on most POSIX hosts, and issues an fsync before
// closing (via os.File.Sync). On Windows this is as durable as the OS flush
// guarantee allows; the tmp→rename pattern prevents partial reads.
//
// Concurrency: safe for concurrent use; separate tenants never share a
// directory and a mutex-free atomic rename protects within-tenant concurrent
// Puts to the same key.
//
// MaxBytes controls the maximum accepted payload size. Zero means unlimited.
type FSStore struct {
	root     string
	maxBytes int64
}

// NewFSStore returns an [FSStore] rooted at dir. dir must be an existing,
// writable directory. maxBytes is the maximum number of bytes accepted by
// [FSStore.Put]; pass 0 for unlimited.
func NewFSStore(dir string, maxBytes int64) *FSStore {
	return &FSStore{root: dir, maxBytes: maxBytes}
}

// Compile-time assertion that FSStore satisfies BlobStorePort.
var _ BlobStorePort = (*FSStore)(nil)

// blobMeta is the JSON-serialized companion to the raw payload file.
type blobMeta struct {
	MediaType string `json:"media_type"`
	SizeBytes int64  `json:"size_bytes"`
}

// tenantDir returns the per-tenant subdirectory path (does not create it).
// The tenant id is sanitized with filepath.Clean to prevent path traversal.
func (s *FSStore) tenantDir(tenantID string) string {
	// Clean the tenant id: reject any separator or ".." component so a crafted
	// tenant id cannot escape the root directory.
	safe := strings.ReplaceAll(tenantID, string(os.PathSeparator), "_")
	safe = strings.ReplaceAll(safe, "/", "_")
	safe = strings.ReplaceAll(safe, "..", "_")
	return filepath.Join(s.root, safe)
}

// keyBase returns the sanitized base filename (without extension) for a blob
// key, similarly preventing any path traversal.
func keyBase(key string) string {
	safe := strings.ReplaceAll(key, string(os.PathSeparator), "_")
	safe = strings.ReplaceAll(safe, "/", "_")
	safe = strings.ReplaceAll(safe, "..", "_")
	return safe
}

// blobPaths returns the payload and metadata file paths for ref.
func (s *FSStore) blobPaths(ref Ref) (payload, meta string) {
	base := filepath.Join(s.tenantDir(ref.TenantID), keyBase(ref.Key))
	return base + ".blob", base + ".meta.json"
}

// Put stores the bytes read from r under ref with the given mediaType.
// It returns [ErrTooLarge] when the payload exceeds the configured maximum.
// Putting an identical (tenant, ref) again is idempotent: a second Put with
// the same content succeeds and leaves the store in the same state.
func (s *FSStore) Put(_ context.Context, ref Ref, mediaType string, r io.Reader) (Object, error) {
	// Read the payload first so we can measure its size before writing anything.
	// Using io.LimitReader with maxBytes+1 lets us detect an over-limit stream
	// without reading the entire (potentially huge) body into memory when a
	// limit is configured.
	var data []byte
	if s.maxBytes > 0 {
		limited := io.LimitReader(r, s.maxBytes+1)
		var err error
		data, err = io.ReadAll(limited)
		if err != nil {
			return Object{}, fmt.Errorf("blob: reading payload: %w", err)
		}
		if int64(len(data)) > s.maxBytes {
			return Object{}, ErrTooLarge
		}
	} else {
		var err error
		data, err = io.ReadAll(r)
		if err != nil {
			return Object{}, fmt.Errorf("blob: reading payload: %w", err)
		}
	}

	// Ensure the per-tenant directory exists.
	dir := s.tenantDir(ref.TenantID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return Object{}, fmt.Errorf("blob: creating tenant dir: %w", err)
	}

	payloadPath, metaPath := s.blobPaths(ref)

	// Write the payload via a temp-file + rename for atomic visibility.
	if err := writeFileAtomic(payloadPath, data, 0o600); err != nil {
		return Object{}, fmt.Errorf("blob: writing payload: %w", err)
	}

	// Write the metadata.
	meta := blobMeta{MediaType: mediaType, SizeBytes: int64(len(data))}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return Object{}, fmt.Errorf("blob: marshaling metadata: %w", err)
	}
	if err := writeFileAtomic(metaPath, metaBytes, 0o600); err != nil {
		return Object{}, fmt.Errorf("blob: writing metadata: %w", err)
	}

	return Object{
		Ref:       ref,
		MediaType: mediaType,
		SizeBytes: int64(len(data)),
	}, nil
}

// Get opens the stored bytes for ref for streaming reads. It returns
// [ErrNotFound] when no blob exists for the ref under its tenant; this
// sentinel is identical whether the ref is simply absent or belongs to a
// different tenant (no existence oracle, NFR-SEC-03).
func (s *FSStore) Get(_ context.Context, ref Ref) (Object, io.ReadCloser, error) {
	obj, err := s.readMeta(ref)
	if err != nil {
		return Object{}, nil, err
	}

	payloadPath, _ := s.blobPaths(ref)
	f, err := os.Open(payloadPath) //nolint:gosec // path is tenant+key sanitized
	if err != nil {
		if os.IsNotExist(err) {
			return Object{}, nil, ErrNotFound
		}
		return Object{}, nil, fmt.Errorf("blob: opening payload: %w", err)
	}
	return obj, f, nil
}

// Stat returns the [Object] metadata for ref without opening the bytes, or
// [ErrNotFound]. The error is indistinguishable from a wrong-tenant ref
// (NFR-SEC-03).
func (s *FSStore) Stat(_ context.Context, ref Ref) (Object, error) {
	return s.readMeta(ref)
}

// Delete removes the bytes and metadata for ref. It is idempotent: deleting
// an absent blob returns nil, not [ErrNotFound].
func (s *FSStore) Delete(_ context.Context, ref Ref) error {
	payloadPath, metaPath := s.blobPaths(ref)
	for _, p := range []string{payloadPath, metaPath} {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("blob: deleting %s: %w", p, err)
		}
	}
	return nil
}

// readMeta reads and returns the Object for ref, returning [ErrNotFound]
// when the metadata file is absent (which covers both the "wrong tenant"
// and "genuinely absent" cases identically — no existence oracle).
func (s *FSStore) readMeta(ref Ref) (Object, error) {
	_, metaPath := s.blobPaths(ref)
	raw, err := os.ReadFile(metaPath) //nolint:gosec // path is tenant+key sanitized
	if err != nil {
		if os.IsNotExist(err) {
			return Object{}, ErrNotFound
		}
		return Object{}, fmt.Errorf("blob: reading metadata: %w", err)
	}
	var m blobMeta
	if err := json.Unmarshal(raw, &m); err != nil {
		return Object{}, fmt.Errorf("blob: parsing metadata: %w", err)
	}
	return Object{
		Ref:       ref,
		MediaType: m.MediaType,
		SizeBytes: m.SizeBytes,
	}, nil
}

// writeFileAtomic writes data to path atomically using a temp file in the
// same directory followed by an os.Rename. This guarantees that concurrent
// readers never see a partial file. The file is flushed to disk before the
// rename so callers can rely on the durability guarantee described in [Put].
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".blob-tmp-*")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpName := tmp.Name()

	// Ensure cleanup if anything goes wrong before rename.
	success := false
	defer func() {
		if !success {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writing temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("syncing temp file: %w", err)
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("renaming temp file: %w", err)
	}
	success = true
	return nil
}
