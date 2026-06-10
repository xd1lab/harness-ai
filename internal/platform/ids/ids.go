// Package ids defines the [IDGenerator] port for minting fresh identifiers and a
// trivial system implementation.
//
// # Why a port
//
// The architecture's cross-cutting determinism rule (architecture §5) forbids
// domain and app code from calling uuid.New (or rand.*) directly: every component
// that generates ids takes an injected IDGenerator through its ports.go, enforced
// by depguard/forbidigo. Injecting the generator makes id-bearing behavior
// reproducible under test — a fake generator yields a scripted sequence so a test
// can assert the exact turn_id, request_id, or session_id a flow produces.
//
// # Scope: non-derived ids only
//
// This port mints genuinely fresh ids: session ids, turn ids, and per-append
// request_ids (the idempotency token on every event append, ADR-0011 §"Per-append
// request_id idempotency"). It is deliberately NOT used for log-derived keys. The
// tool-execution idempotency key is hash(session_id, seq_of_ToolCall) and is
// reconstructed deterministically from the log on replay (ADR-0012; architecture
// §7.2) — that derivation is a pure function elsewhere, never a fresh id from this
// generator, precisely so any orchestrator replaying the log computes the same key.
//
// # Purity
//
// This package is contract-only apart from the [System] implementation, which is
// the one permitted platform system impl (NFR-TEST-01). It uses
// [github.com/google/uuid] for UUIDv7 generation; domain/app/policy packages must
// never import this package directly — they receive an injected [IDGenerator].
package ids

import "github.com/google/uuid"

// ID is an opaque, globally-unique identifier rendered as its canonical string
// form. It is the type minted by [IDGenerator] for sessions, turns, and append
// request ids. Treating it as a distinct string type (rather than bare string)
// documents intent at call sites and lets the wire/db layers convert explicitly
// (e.g. to a UUID column) at their edges.
type ID string

// String returns the canonical string form of the id.
func (i ID) String() string { return string(i) }

// IsZero reports whether the id is the empty/unset value.
func (i ID) IsZero() bool { return i == "" }

// IDGenerator mints fresh, unique identifiers. Implementations must be safe for
// concurrent use and must return collision-free values across the process (and, for
// the UUID-backed system implementation, across processes with negligible collision
// probability).
type IDGenerator interface {
	// NewID returns a freshly generated, unique [ID]. It is the general-purpose
	// minting method used for turn ids and any other opaque id; the typed helpers
	// below document intent for the two id classes the schema names explicitly.
	NewID() ID

	// NewSessionID returns a fresh [ID] for a new session aggregate (the
	// event-sourcing stream root; ADR-0011 §6.2 sessions.id). Semantically
	// equivalent to [IDGenerator.NewID]; named for clarity at the call site.
	NewSessionID() ID

	// NewRequestID returns a fresh [ID] to use as the per-append request_id — the
	// idempotency token carried on every [github.com/xd1lab/harness-ai/internal/orchestrator/app.EventLogPort]
	// Append so a retried append whose ack was lost is a no-op rather than a
	// spurious conflict (ADR-0011; architecture §6.3, §7.3). A new request_id is
	// minted per append attempt's logical operation, then reused across retries of
	// that same attempt.
	NewRequestID() ID
}

// System is the production [IDGenerator] backed by UUIDv7
// ([github.com/google/uuid.NewV7]). UUIDv7 encodes a millisecond-precision
// Unix timestamp in the high bits, making IDs both globally unique and
// roughly monotonically sortable — useful for the event-store's session and
// request identifiers. The zero value is ready to use. All methods are safe
// for concurrent use.
//
// This is the one permitted real-clock platform system impl (NFR-TEST-01).
// Domain and app code must inject an [IDGenerator] and never import or
// instantiate this type directly.
type System struct{}

// Compile-time assertion that System satisfies IDGenerator.
var _ IDGenerator = System{}

// newUUIDv7 mints a fresh UUIDv7 and returns its canonical string form.
// It panics only if the system's random source is broken (crypto/rand
// failure), which is an unrecoverable platform error.
func newUUIDv7() ID {
	u, err := uuid.NewV7()
	if err != nil {
		panic("ids.System: failed to generate UUIDv7: " + err.Error())
	}
	return ID(u.String())
}

// NewID returns a freshly generated UUIDv7 as a general-purpose turn id or
// opaque identifier.
func (System) NewID() ID { return newUUIDv7() }

// NewSessionID returns a freshly generated UUIDv7 for a new session aggregate
// root (semantically equivalent to [System.NewID]; named for call-site clarity).
func (System) NewSessionID() ID { return newUUIDv7() }

// NewRequestID returns a freshly generated UUIDv7 to use as the per-append
// idempotency token on every [EventLogPort] Append call.
func (System) NewRequestID() ID { return newUUIDv7() }
