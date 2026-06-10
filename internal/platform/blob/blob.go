// Package blob defines the [BlobStorePort] for off-database storage of large tool
// outputs, addressed by a TENANT-SCOPED reference.
//
// # Why blobs
//
// Large tool outputs (above a 32 KiB threshold) are stored outside the events table
// to keep the WAL compact; the event row keeps only a lightweight descriptor plus a
// blob_ref (ADR-0011 §"Tenant-scoped blobs"; architecture §6.4). Bytes are written
// to this store BEFORE the append transaction commits the referencing event, and
// the blobs metadata row is inserted in the SAME transaction as the event, so the
// FK on events.blob_ref makes a dangling reference impossible (architecture §6.4,
// §7.4).
//
// # Tenant-scoped identity (security-critical)
//
// Blob identity is the pair (tenant_id, ref), never a bare global ref. Cross-tenant
// content-addressed dedup is FORBIDDEN because a global content key is a
// cross-tenant existence oracle (ADR-0011; ADR-0013 §"Tenant-scoped blobs";
// architecture §8.5). Therefore every method here takes an explicit [Ref] carrying
// the tenant id, and implementations MUST authorize against the requesting
// session's verified tenant_id and ownership — never the content key alone. The
// backing storage_uri is tenant-prefixed and the port enforces that prefix.
//
// # Backend is deferred
//
// The concrete backend (single-node filesystem vs. S3-compatible) is an open
// question deferred past this gate (architecture §14); both sit behind this port.
//
// # Purity
//
// Contract-only: the port interface, the [Ref] and [Object] types, and sentinel
// errors. It imports only the standard library (context, errors, io) and the
// platform tenant/blob value types.
package blob

import (
	"context"
	"errors"
	"io"
)

// ErrNotFound is returned when no blob exists for the given [Ref] under its tenant.
// It is a sentinel for [errors.Is]. It deliberately does not distinguish "wrong
// tenant" from "absent" to avoid leaking cross-tenant existence (architecture §8.5).
var ErrNotFound = errors.New("blob: not found")

// ErrTooLarge is returned by [BlobStorePort.Put] when the object exceeds the
// backend's configured maximum object size.
var ErrTooLarge = errors.New("blob: object exceeds maximum size")

// Ref is the tenant-scoped identity of a blob: the composite key (TenantID, Key)
// that mirrors the blobs table primary key (tenant_id, ref) (ADR-0011 §6.2). The
// Key is a per-tenant content key (for example the sha256 of the bytes); it is NOT
// global, so identical bytes in two tenants are two distinct blobs. All port
// methods take a Ref so authorization is always against (tenant, key) plus session
// ownership, never the key alone.
type Ref struct {
	// TenantID is the owning tenant's id (the sessions/events tenant_id). It scopes
	// the blob's identity and is part of its primary key.
	TenantID string
	// Key is the per-tenant content reference (e.g. a content hash). It is unique
	// only within a tenant; cross-tenant reuse of an identical Key denotes two
	// independent blobs and is never deduplicated.
	Key string
}

// IsZero reports whether the ref is unset (both fields empty).
func (r Ref) IsZero() bool { return r.TenantID == "" && r.Key == "" }

// Object is the metadata describing a stored blob, mirroring the non-byte columns
// of the blobs table (media_type, size_bytes; architecture §6.2). The bytes
// themselves are streamed separately via [io.Reader]/[io.ReadCloser] so large
// objects need not be held in memory.
type Object struct {
	// Ref is the tenant-scoped identity of this object.
	Ref Ref
	// MediaType is the IANA media type of the stored bytes, e.g. "text/plain" or
	// "application/json". It is required.
	MediaType string
	// SizeBytes is the exact length of the stored bytes. It is set by the store on
	// Put and returned on metadata reads.
	SizeBytes int64
}

// BlobStorePort stores and retrieves large opaque byte payloads keyed by a
// tenant-scoped [Ref]. Implementations MUST:
//
//   - prefix the backing storage location with the tenant id (tenant-prefixed
//     storage_uri) and never permit a Ref of one tenant to address another
//     tenant's bytes;
//   - require the caller to have already authorized the request against the
//     session's verified tenant and ownership (the port is the last line, the
//     handler/RLS is the first);
//   - be safe for concurrent use.
//
// The bytes are written here BEFORE the event that references them is committed
// (write-before-reference, architecture §6.4); orphaned bytes whose (tenant, ref)
// has no referencing event are reclaimed by a background sweeper in projectord.
type BlobStorePort interface {
	// Put stores the bytes read from r under ref with the given mediaType, and
	// returns the resulting [Object] (including the authoritative SizeBytes). It
	// returns [ErrTooLarge] when the payload exceeds the backend maximum. Put must
	// durably persist (fsync / 200) before returning success, because the caller
	// commits the referencing event only after Put returns (architecture §6.4).
	// Putting an identical (tenant, ref) again is idempotent.
	Put(ctx context.Context, ref Ref, mediaType string, r io.Reader) (Object, error)

	// Get opens the stored bytes for ref for streaming reads, returning the
	// [Object] metadata and a [io.ReadCloser] the caller must Close. It returns
	// [ErrNotFound] when no blob exists for ref under its tenant. Authorization is
	// by (tenant, ref) plus session ownership upstream, never the key alone
	// (architecture §8.5).
	Get(ctx context.Context, ref Ref) (Object, io.ReadCloser, error)

	// Stat returns the [Object] metadata for ref without opening the bytes, or
	// [ErrNotFound]. It is used by the startup integrity check that flags any event
	// whose blob_ref is absent from the store (architecture §6.4).
	Stat(ctx context.Context, ref Ref) (Object, error)

	// Delete removes the bytes for ref. It is idempotent: deleting an absent blob
	// returns nil, not [ErrNotFound]. Delete is used only by the orphan-reclaiming
	// sweeper after the grace period; referenced blobs are never deleted while an
	// event references them (architecture §6.4).
	Delete(ctx context.Context, ref Ref) error
}
