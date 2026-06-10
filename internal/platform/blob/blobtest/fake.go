// Package blobtest provides a deterministic in-memory fake [blob.BlobStorePort]
// for tests. It stores blobs in memory, enforces the tenant-scoped identity
// invariant, and returns [blob.ErrNotFound] for absent or cross-tenant keys.
package blobtest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/xd1lab/harness-ai/internal/platform/blob"
)

// Compile-time assertion that FakeBlobStore satisfies blob.BlobStorePort.
var _ blob.BlobStorePort = (*FakeBlobStore)(nil)

// entry is one in-memory blob.
type entry struct {
	obj  blob.Object
	data []byte
}

// FakeBlobStore is an in-memory [blob.BlobStorePort]. It respects the tenant
// scoping contract: a Ref from tenant A can never address tenant B's bytes.
// MaxSize, when non-zero, causes Put to return [blob.ErrTooLarge] for payloads
// that exceed it.
type FakeBlobStore struct {
	mu      sync.Mutex
	store   map[string]entry // key: tenantID + "\x00" + ref.Key
	MaxSize int64
}

// NewFakeBlobStore returns an empty FakeBlobStore ready for use.
func NewFakeBlobStore() *FakeBlobStore {
	return &FakeBlobStore{store: make(map[string]entry)}
}

// storeKey returns the internal map key for a Ref.
func storeKey(ref blob.Ref) string {
	return ref.TenantID + "\x00" + ref.Key
}

// Put stores the bytes from r under ref with the given mediaType. It returns
// [blob.ErrTooLarge] when the payload exceeds MaxSize (if set). Putting the
// same (tenant, ref) again is idempotent.
func (f *FakeBlobStore) Put(_ context.Context, ref blob.Ref, mediaType string, r io.Reader) (blob.Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return blob.Object{}, fmt.Errorf("blobtest: reading data: %w", err)
	}
	if f.MaxSize > 0 && int64(len(data)) > f.MaxSize {
		return blob.Object{}, blob.ErrTooLarge
	}
	obj := blob.Object{
		Ref:       ref,
		MediaType: mediaType,
		SizeBytes: int64(len(data)),
	}
	f.mu.Lock()
	f.store[storeKey(ref)] = entry{obj: obj, data: data}
	f.mu.Unlock()
	return obj, nil
}

// Get opens the stored bytes for ref. It returns [blob.ErrNotFound] when no
// blob exists for the ref's (tenant, key) pair.
func (f *FakeBlobStore) Get(_ context.Context, ref blob.Ref) (blob.Object, io.ReadCloser, error) {
	f.mu.Lock()
	e, ok := f.store[storeKey(ref)]
	f.mu.Unlock()
	if !ok {
		return blob.Object{}, nil, blob.ErrNotFound
	}
	return e.obj, io.NopCloser(bytes.NewReader(e.data)), nil
}

// Stat returns the Object metadata for ref without opening the bytes, or
// [blob.ErrNotFound].
func (f *FakeBlobStore) Stat(_ context.Context, ref blob.Ref) (blob.Object, error) {
	f.mu.Lock()
	e, ok := f.store[storeKey(ref)]
	f.mu.Unlock()
	if !ok {
		return blob.Object{}, blob.ErrNotFound
	}
	return e.obj, nil
}

// Delete removes the bytes for ref. It is idempotent: deleting an absent blob
// returns nil, not [blob.ErrNotFound].
func (f *FakeBlobStore) Delete(_ context.Context, ref blob.Ref) error {
	f.mu.Lock()
	delete(f.store, storeKey(ref))
	f.mu.Unlock()
	return nil
}
