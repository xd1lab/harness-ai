package domain

import "time"

// SessionStatus is the lifecycle state of a [Session], matching the sessions.status
// column (ADR-0011 §6.2). Appends are rejected to any session not [StatusActive]
// (the append transaction's status guard; ADR-0011 §6.3).
type SessionStatus string

const (
	// StatusActive is an open session that may be appended to. It is the default
	// on creation (sessions.status DEFAULT 'active').
	StatusActive SessionStatus = "active"
	// StatusFinished is a session whose run completed normally; no further appends
	// are accepted.
	StatusFinished SessionStatus = "finished"
	// StatusFailed is a session whose run terminated with an error; no further
	// appends are accepted.
	StatusFailed SessionStatus = "failed"
)

// OrDefault returns the [PermissionMode] (defined with the other persisted mode
// vocabulary in event.go), mapping the empty zero value to [ModeDefault] so
// callers never have to special-case an unset mode — an existing pre-migration
// session row, or an in-memory fake that does not populate it, reads as the
// secure default (ADR-0019). NOTE: the domain mode strings are NOT identical to
// policy.Mode's (domain uses "acceptEdits", policy uses "accept_edits"), so the
// orchestrator edge converts between the two by explicit mapping, never a cast.
func (m PermissionMode) OrDefault() PermissionMode {
	if m == "" {
		return ModeDefault
	}
	return m
}

// Session is the event-sourcing aggregate root: the stream identity and the small
// amount of mutable control state kept alongside the append-only log. It mirrors
// the sessions table (ADR-0011 §6.2). The conversation itself is NOT stored here —
// it is derived by folding the session's events; this struct holds only the
// optimistic version, the fencing lease, the fork lineage, and the status.
//
// HeadSeq is the optimistic concurrency version: an append supplies the expected
// head and the transaction bumps it atomically, tying the new event's seq to the
// transition so contiguity holds by construction (ADR-0011 §6.3). LeaseEpoch is the
// monotonic fencing token checked on every append so a stuck or stolen writer is
// rejected even when its expected seq is current (ADR-0014 §"Fenced lease";
// architecture §9.6).
type Session struct {
	// ID is the session/stream identifier (sessions.id).
	ID string
	// TenantID is the owning tenant; every derived row is RLS-scoped to it
	// (sessions.tenant_id; ADR-0011 §6.7).
	TenantID string

	// ParentID is the parent session id when this session is a fork, empty
	// otherwise (sessions.parent_id; architecture §6.6).
	ParentID string
	// ForkedFromSeq is the frozen, immutable parent seq this session branched at;
	// the child's own seq continues from ForkedFromSeq+1 so the composed timeline
	// has a single monotonic seq namespace (sessions.forked_from_seq; architecture
	// §6.6). Zero/unset for a non-fork.
	ForkedFromSeq int64

	// Status is the lifecycle state; appends are accepted only when [StatusActive]
	// (ADR-0011 §6.3).
	Status SessionStatus
	// Mode is the session's standing permission operating mode (sessions.mode;
	// ADR-0019), set at creation from the verified request and inherited by forks.
	// The zero value means [ModeDefault]. The orchestrator's Run path reads it into
	// the policy pipeline for the run.
	Mode PermissionMode
	// HeadSeq is the optimistic version: the seq of the last appended event (0 for
	// a fresh session before its first event) (sessions.head_seq; ADR-0011 §6.3).
	HeadSeq int64

	// LeaseOwner identifies the current single writer (SPIFFE id + instance) that
	// holds the session lease (sessions.lease_owner; architecture §9.6). Empty when
	// unleased.
	LeaseOwner string
	// LeaseEpoch is the monotonic fencing token; it is bumped on takeover and
	// checked on every append so a fenced-out writer cannot append (sessions
	// .lease_epoch; ADR-0011 §6.3, ADR-0014; architecture §9.6).
	LeaseEpoch int64
	// LeaseExpiry is the lease TTL deadline, renewed by heartbeat; after it elapses
	// a new owner may take over and bump LeaseEpoch (sessions.lease_expiry;
	// architecture §9.6). Zero when unleased.
	LeaseExpiry time.Time

	// LastEventAt is the time of the most recent append, the input to the
	// stuck-session detector (sessions.last_event_at; architecture §9.6, §10.5).
	LastEventAt time.Time
	// CreatedAt is when the session was created (sessions.created_at).
	CreatedAt time.Time
	// UpdatedAt is when the session control state last changed (sessions.updated_at).
	UpdatedAt time.Time
}

// IsActive reports whether the session may currently be appended to.
func (s Session) IsActive() bool { return s.Status == StatusActive }

// IsFork reports whether the session was created by forking a parent session.
func (s Session) IsFork() bool { return s.ParentID != "" }
