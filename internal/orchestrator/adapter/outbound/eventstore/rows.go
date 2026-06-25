package eventstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// defaultSchemaVersion is the per-event-type payload version stamped when an
// [app.AppendInput] leaves SchemaVersion unset (the current default; ADR-0011
// §"Migration policy").
const defaultSchemaVersion = 1

// eventColumns is the column list (in order) that [scanEnvelopes] reads. It is
// shared by every SELECT that returns whole events so the scan order cannot
// drift from the query.
const eventColumns = "session_id, seq, request_id, event_type, schema_version, payload, blob_ref, actor, created_at, content_hash, chain_hash"

// selectEventsByRequestSQL loads the events previously committed under a
// (session_id, request_id) pair, oldest-seq first — the idempotency
// short-circuit read (ADR-0011 §6.3).
const selectEventsByRequestSQL = "SELECT " + eventColumns +
	" FROM events WHERE session_id = $1 AND request_id = $2 ORDER BY seq"

// selectEventsFromSeqSQL loads a session's events from a seq (inclusive) onward,
// oldest first — the Load/fold read (architecture §6.6).
const selectEventsFromSeqSQL = "SELECT " + eventColumns +
	" FROM events WHERE session_id = $1 AND seq >= $2 ORDER BY seq"

// selectEventsAfterSeqSQL loads a session's events strictly after a seq, oldest
// first — the Subscribe catch-up read (only seq > fromSeq; architecture §7.1).
const selectEventsAfterSeqSQL = "SELECT " + eventColumns +
	" FROM events WHERE session_id = $1 AND seq > $2 ORDER BY seq"

// BlobUpload carries the metadata for the blobs row inserted in the same
// transaction as a blob-referencing event ([Store.AppendWithBlob]). The bytes
// themselves are written to the [github.com/xd1lab/harness-ai/internal/platform/blob.BlobStorePort]
// by the caller BEFORE the append (write-before-reference; architecture §6.4,
// §7.4); this struct is only the metadata row. The composite FK
// events(tenant_id, blob_ref) -> blobs(tenant_id, ref) makes a dangling
// reference impossible, and the matching tenant column closes a cross-tenant ref
// (architecture §8.5).
type BlobUpload struct {
	// Ref is the per-tenant blob key (the events.blob_ref value), e.g. a content
	// hash. It is scoped to the append's tenant.
	Ref string
	// MediaType is the IANA media type of the stored bytes.
	MediaType string
	// SizeBytes is the exact length of the stored bytes (authoritative, from the
	// blob store Put).
	SizeBytes int64
	// StorageURI is the tenant-prefixed location of the bytes in the blob store
	// (bytes live OUTSIDE Postgres; architecture §6.4).
	StorageURI string
}

// insertBlobRow inserts the blobs metadata row for a [BlobUpload] within the
// append transaction. The tenant is the append's tenant (the same value the
// SET LOCAL GUC scopes the connection to), so RLS admits the row.
func (s *Store) insertBlobRow(ctx context.Context, tx pgx.Tx, tenantID string, b BlobUpload) error {
	if b.Ref == "" {
		return fmt.Errorf("eventstore: blob upload requires a non-empty Ref")
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO blobs (tenant_id, ref, media_type, size_bytes, storage_uri)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (tenant_id, ref) DO NOTHING`,
		tenantID, b.Ref, b.MediaType, b.SizeBytes, b.StorageURI)
	if err != nil {
		return fmt.Errorf("eventstore: inserting blob row: %w", err)
	}
	return nil
}

// insertEventArgs bundles the inputs to [Store.insertEvent].
type insertEventArgs struct {
	tenantID  string
	sessionID string
	seq       int64
	requestID string
	in        app.AppendInput
	blob      *BlobUpload
	// prevChain is the running per-session chain head entering this event: the
	// prior event's chain_hash, or [domain.GenesisChainHash] for the first chained
	// event of the session. insertEvent folds it into this event's chain_hash and
	// returns the new head on the envelope (ADR-0033).
	prevChain []byte
}

// insertEvent inserts one event row at args.seq and returns its
// [domain.EventEnvelope] (with the DB-assigned created_at). It rejects an event
// whose payload declares a non-empty BlobRef unless a matching [BlobUpload] is
// being inserted in the same transaction, so a plain [Store.Append] can never
// create a dangling blob reference.
func (s *Store) insertEvent(ctx context.Context, tx pgx.Tx, args insertEventArgs) (domain.EventEnvelope, error) {
	payload, err := marshalPayload(args.in.Event)
	if err != nil {
		return domain.EventEnvelope{}, err
	}

	schemaVersion := args.in.SchemaVersion
	if schemaVersion == 0 {
		schemaVersion = defaultSchemaVersion
	}
	actor := args.in.Actor
	if actor == "" {
		actor = domain.ActorSystem
	}

	blobRef := eventBlobRef(args.in.Event)
	if blobRef != "" {
		// Guard: the referenced blob must be inserted in this same tx.
		if args.blob == nil || args.blob.Ref != blobRef {
			return domain.EventEnvelope{}, fmt.Errorf(
				"eventstore: event %s references blob_ref %q with no matching BlobUpload in the same tx (use AppendWithBlob)",
				args.in.Event.EventType(), blobRef)
		}
	}

	// Tamper-evident hashes (ADR-0033): content_hash over the EXACT payload bytes
	// just marshaled (so verify-on-read recomputes the identical digest), then
	// chain_hash = ChainHash(prevChain, content_hash). prevChain is the prior
	// event's chain_hash (or the session genesis for the first chained event).
	contentHash := domain.ContentHash(payload)
	chainHash := domain.ChainHash(args.prevChain, contentHash)

	var createdAt time.Time
	// blob_ref is written as NULL when empty so the composite FK is not exercised
	// for non-blob events.
	var blobArg any
	if blobRef != "" {
		blobArg = blobRef
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO events (tenant_id, session_id, seq, request_id, event_type, schema_version, payload, blob_ref, actor, content_hash, chain_hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING created_at`,
		args.tenantID, args.sessionID, args.seq, args.requestID,
		string(args.in.Event.EventType()), schemaVersion, payload, blobArg, string(actor),
		contentHash, chainHash,
	).Scan(&createdAt)
	if err != nil {
		return domain.EventEnvelope{}, fmt.Errorf("eventstore: inserting event seq=%d: %w", args.seq, err)
	}

	return domain.EventEnvelope{
		Type:          args.in.Event.EventType(),
		Seq:           args.seq,
		SessionID:     args.sessionID,
		TenantID:      args.tenantID,
		RequestID:     args.requestID,
		SchemaVersion: schemaVersion,
		Actor:         actor,
		CreatedAt:     createdAt,
		Event:         args.in.Event,
		ContentHash:   contentHash,
		ChainHash:     chainHash,
	}, nil
}

// eventBlobRef returns the blob_ref a payload carries, if any. Only
// [domain.ToolResult] offloads to a blob in v1 (architecture §6.4); other event
// types never reference a blob.
func eventBlobRef(e domain.Event) string {
	if tr, ok := e.(domain.ToolResult); ok {
		return tr.BlobRef
	}
	return ""
}

// scanEnvelopes reads whole-event rows (in [eventColumns] order) into
// [domain.EventEnvelope]s, decoding each payload back into its typed
// [domain.Event] via [decodePayload]. tenantID is stamped on each envelope (the
// rows are already tenant-scoped by RLS, so the value is the caller's tenant).
func scanEnvelopes(rows pgx.Rows, tenantID string) ([]domain.EventEnvelope, error) {
	var out []domain.EventEnvelope
	for rows.Next() {
		var (
			sessionID     string
			seq           int64
			requestID     string
			eventType     string
			schemaVersion int
			payload       []byte
			blobRef       *string
			actor         string
			createdAt     time.Time
			contentHash   []byte // nullable: nil for unchained pre-0009 rows.
			chainHash     []byte // nullable: nil for unchained pre-0009 rows.
		)
		if err := rows.Scan(&sessionID, &seq, &requestID, &eventType, &schemaVersion, &payload, &blobRef, &actor, &createdAt, &contentHash, &chainHash); err != nil {
			return nil, fmt.Errorf("eventstore: scanning event row: %w", err)
		}
		evt, err := decodePayload(domain.EventType(eventType), payload)
		if err != nil {
			return nil, err
		}
		out = append(out, domain.EventEnvelope{
			Type:          domain.EventType(eventType),
			Seq:           seq,
			SessionID:     sessionID,
			TenantID:      tenantID,
			RequestID:     requestID,
			SchemaVersion: schemaVersion,
			Actor:         domain.Actor(actor),
			CreatedAt:     createdAt,
			Event:         evt,
			ContentHash:   contentHash,
			ChainHash:     chainHash,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("eventstore: iterating event rows: %w", err)
	}
	return out, nil
}

// decodePayload reconstructs the typed [domain.Event] for an event_type from its
// JSON payload. The switch is exhaustive over the closed [domain.EventType] set
// (event.go); an unknown type is an error rather than a silent drop, so a
// payload written by a newer schema fails loudly on an older reader.
func decodePayload(t domain.EventType, payload []byte) (domain.Event, error) {
	var (
		evt domain.Event
		err error
	)
	switch t {
	case domain.EventSessionStarted:
		evt, err = unmarshalInto[domain.SessionStarted](payload)
	case domain.EventMessageAppended:
		evt, err = unmarshalInto[domain.MessageAppended](payload)
	case domain.EventTurnStarted:
		evt, err = unmarshalInto[domain.TurnStarted](payload)
	case domain.EventAssistantMessageDelta:
		evt, err = unmarshalInto[domain.AssistantMessageDelta](payload)
	case domain.EventAssistantMessage:
		evt, err = unmarshalInto[domain.AssistantMessage](payload)
	case domain.EventToolExecutionStarted:
		evt, err = unmarshalInto[domain.ToolExecutionStarted](payload)
	case domain.EventToolResult:
		evt, err = unmarshalInto[domain.ToolResult](payload)
	case domain.EventToolResultCleared:
		evt, err = unmarshalInto[domain.ToolResultCleared](payload)
	case domain.EventTurnAborted:
		evt, err = unmarshalInto[domain.TurnAborted](payload)
	case domain.EventTurnFinished:
		evt, err = unmarshalInto[domain.TurnFinished](payload)
	case domain.EventCompactionPerformed:
		evt, err = unmarshalInto[domain.CompactionPerformed](payload)
	case domain.EventPermissionDecided:
		evt, err = unmarshalInto[domain.PermissionDecided](payload)
	case domain.EventWorkspaceReset:
		evt, err = unmarshalInto[domain.WorkspaceReset](payload)
	case domain.EventBypassModeActivated:
		evt, err = unmarshalInto[domain.BypassModeActivated](payload)
	case domain.EventMCPToolApprovalRequested:
		evt, err = unmarshalInto[domain.MCPToolApprovalRequested](payload)
	case domain.EventMCPToolApprovalResolved:
		evt, err = unmarshalInto[domain.MCPToolApprovalResolved](payload)
	case domain.EventPlanUpdated:
		evt, err = unmarshalInto[domain.PlanUpdated](payload)
	case domain.EventApprovalRequested:
		evt, err = unmarshalInto[domain.ApprovalRequested](payload)
	default:
		return nil, fmt.Errorf("eventstore: unknown event_type %q (newer schema?)", t)
	}
	if err != nil {
		return nil, fmt.Errorf("eventstore: decoding %s payload: %w", t, err)
	}
	return evt, nil
}

// unmarshalInto decodes payload into a value of type E (a concrete domain event
// struct) and returns it as a [domain.Event]. The type parameter keeps the
// per-type cases in [decodePayload] to one line each while preserving the
// concrete dynamic type on the returned interface.
func unmarshalInto[E domain.Event](payload []byte) (domain.Event, error) {
	var v E
	if err := json.Unmarshal(payload, &v); err != nil {
		return nil, err
	}
	return v, nil
}
