package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// defaultMemoryNamespace is the namespace a memory_* call falls back to when the
// model does not supply one. It matches the agent_memory.namespace column
// DEFAULT 'default' (ADR-0030).
const defaultMemoryNamespace = "default"

// ---------------------------------------------------------------------------
// memory_write
// ---------------------------------------------------------------------------

// MemoryWriteTool stores a durable key/value memory for the calling tenant,
// persisting it across sessions (ADR-0030). It is classified
// [domain.SideEffectMutating] (it UPSERTs a row) and [domain.EgressClassNone]
// (no network egress — it talks only to the tenant-scoped [app.MemoryStore]),
// so the execute service's egress gate is never invoked for it. The owning
// tenant is read from the request context by the store, NEVER from an argument,
// so a write can never name or cross to another tenant.
type MemoryWriteTool struct {
	store app.MemoryStore
}

// NewMemoryWriteTool returns a [MemoryWriteTool] backed by the given store.
func NewMemoryWriteTool(store app.MemoryStore) *MemoryWriteTool {
	return &MemoryWriteTool{store: store}
}

// Spec returns the memory_write tool's declaration.
func (t *MemoryWriteTool) Spec() domain.ToolSpec {
	return domain.ToolSpec{
		Name:        "memory_write",
		Description: "Store a durable key/value memory that persists across sessions for this tenant. Use to remember facts, preferences, or decisions for later recall. Overwrites any existing memory with the same key in the same namespace.",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"required": ["key", "value"],
			"properties": {
				"namespace": {"type": "string", "description": "Optional namespace; defaults to 'default'."},
				"key": {"type": "string", "description": "The memory key to write (unique within tenant+namespace)."},
				"value": {"type": "string", "description": "The text value to store."},
				"tags": {"type": "array", "items": {"type": "string"}, "description": "Optional tags for retrieval."}
			},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
	}
}

// Execute UPSERTs the (namespace, key) memory for the context's tenant. A
// missing required field or a store error (including the fail-closed no-tenant
// error) is surfaced as an error observation with a nil Go error, matching the
// webfetch/fs pattern (FR-TOOL-01).
func (t *MemoryWriteTool) Execute(ctx context.Context, _ string, args map[string]any) (domain.Observation, error) {
	key, ok := stringArg(args, "key")
	if !ok || key == "" {
		return errObs("memory_write: required string field %q is missing", "key"), nil
	}
	value, ok := stringArg(args, "value")
	if !ok {
		return errObs("memory_write: required string field %q is missing", "value"), nil
	}
	ns, ok := optStringArg(args, "namespace", defaultMemoryNamespace)
	if !ok {
		return errObs("memory_write: %q must be a string", "namespace"), nil
	}
	if ns == "" {
		ns = defaultMemoryNamespace
	}
	tags, ok := stringSliceArg(args, "tags")
	if !ok {
		return errObs("memory_write: %q must be an array of strings", "tags"), nil
	}
	if err := t.store.Put(ctx, ns, key, value, tags); err != nil {
		return errObs("memory_write: %v", err), nil
	}
	return okObs(fmt.Sprintf("stored memory %q in namespace %q", key, ns)), nil
}

// ---------------------------------------------------------------------------
// memory_read
// ---------------------------------------------------------------------------

// MemoryReadTool retrieves a single durable memory by key for the calling
// tenant (ADR-0030). It is [domain.SideEffectReadOnly] and
// [domain.EgressClassNone]. A miss (no memory under the key) is a NON-error
// observation — a normal model-visible outcome, like websearch "No results." —
// not an IsError.
type MemoryReadTool struct {
	store app.MemoryStore
}

// NewMemoryReadTool returns a [MemoryReadTool] backed by the given store.
func NewMemoryReadTool(store app.MemoryStore) *MemoryReadTool {
	return &MemoryReadTool{store: store}
}

// Spec returns the memory_read tool's declaration.
func (t *MemoryReadTool) Spec() domain.ToolSpec {
	return domain.ToolSpec{
		Name:        "memory_read",
		Description: "Read a durable memory by key for this tenant, persisted across sessions. Returns the stored value, or a clear 'no memory found' message when the key is absent.",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"required": ["key"],
			"properties": {
				"namespace": {"type": "string"},
				"key": {"type": "string"}
			},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
	}
}

// Execute reads the (namespace, key) memory for the context's tenant. A missing
// required field or a store error is an error observation (nil Go error); a miss
// is a non-error observation.
func (t *MemoryReadTool) Execute(ctx context.Context, _ string, args map[string]any) (domain.Observation, error) {
	key, ok := stringArg(args, "key")
	if !ok || key == "" {
		return errObs("memory_read: required string field %q is missing", "key"), nil
	}
	ns, ok := optStringArg(args, "namespace", defaultMemoryNamespace)
	if !ok {
		return errObs("memory_read: %q must be a string", "namespace"), nil
	}
	if ns == "" {
		ns = defaultMemoryNamespace
	}
	entry, found, err := t.store.Get(ctx, ns, key)
	if err != nil {
		return errObs("memory_read: %v", err), nil
	}
	if !found {
		return okObs(fmt.Sprintf("no memory found for key %q in namespace %q", key, ns)), nil
	}
	return okObs(renderEntry(entry)), nil
}

// ---------------------------------------------------------------------------
// memory_search
// ---------------------------------------------------------------------------

// MemorySearchTool retrieves the calling tenant's memories by case-insensitive
// value substring and/or required tags (ADR-0030). It is
// [domain.SideEffectReadOnly] and [domain.EgressClassNone]. With no filters it
// lists the tenant's most recent memories up to the store's cap. It does NOT do
// vector/embedding search — that is deliberately out of scope (ADR-0030).
type MemorySearchTool struct {
	store app.MemoryStore
}

// NewMemorySearchTool returns a [MemorySearchTool] backed by the given store.
func NewMemorySearchTool(store app.MemoryStore) *MemorySearchTool {
	return &MemorySearchTool{store: store}
}

// Spec returns the memory_search tool's declaration.
func (t *MemorySearchTool) Spec() domain.ToolSpec {
	return domain.ToolSpec{
		Name:        "memory_search",
		Description: "Search this tenant's durable memories by a case-insensitive substring of the value and/or by required tags (ALL supplied tags must match). With no filters, lists the most recent memories. Use to recall facts, preferences, or decisions stored in earlier sessions.",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"query": {"type": "string", "description": "Case-insensitive substring to match."},
				"tags": {"type": "array", "items": {"type": "string"}, "description": "Require ALL these tags."},
				"limit": {"type": "integer", "description": "Max results (capped)."}
			},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
	}
}

// Execute searches the context tenant's memories. A malformed argument or a
// store error is an error observation (nil Go error); an empty result set is a
// non-error "No memories found." message.
func (t *MemorySearchTool) Execute(ctx context.Context, _ string, args map[string]any) (domain.Observation, error) {
	query, ok := optStringArg(args, "query", "")
	if !ok {
		return errObs("memory_search: %q must be a string", "query"), nil
	}
	tags, ok := stringSliceArg(args, "tags")
	if !ok {
		return errObs("memory_search: %q must be an array of strings", "tags"), nil
	}
	limit, ok := optIntArg(args, "limit", 0)
	if !ok {
		return errObs("memory_search: %q must be an integer", "limit"), nil
	}
	entries, err := t.store.Search(ctx, query, tags, limit)
	if err != nil {
		return errObs("memory_search: %v", err), nil
	}
	if len(entries) == 0 {
		return okObs("No memories found."), nil
	}
	var b strings.Builder
	for i, e := range entries {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(renderEntry(e))
	}
	return okObs(b.String()), nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// renderEntry formats a single memory for the model: namespace/key header, the
// tags (when present), then the value.
func renderEntry(e app.MemoryEntry) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[%s/%s]", e.Namespace, e.Key)
	if len(e.Tags) > 0 {
		fmt.Fprintf(&b, " tags=%s", strings.Join(e.Tags, ","))
	}
	b.WriteString("\n")
	b.WriteString(e.Value)
	return b.String()
}

// stringSliceArg extracts an optional array-of-strings argument. An absent key
// yields a nil slice; a present value must be a JSON array whose elements are
// all strings (the decoded shape is []any of string), otherwise ok=false so the
// caller emits an error observation. This is defensive depth behind the schema
// validation the registry already performed (FR-TOOL-01).
func stringSliceArg(args map[string]any, key string) ([]string, bool) {
	v, present := args[key]
	if !present || v == nil {
		return nil, true
	}
	raw, isSlice := v.([]any)
	if !isSlice {
		// Tolerate an already-typed []string too.
		if s, isStrs := v.([]string); isStrs {
			return s, true
		}
		return nil, false
	}
	out := make([]string, 0, len(raw))
	for _, el := range raw {
		s, isStr := el.(string)
		if !isStr {
			return nil, false
		}
		out = append(out, s)
	}
	return out, true
}

// optIntArg extracts an optional integer argument, returning def when absent. A
// JSON number decodes to float64 (the common case from the gRPC/JSON edge); an
// already-typed int is also accepted. A present value of any other type, or a
// non-integral float, yields ok=false.
func optIntArg(args map[string]any, key string, def int) (int, bool) {
	v, present := args[key]
	if !present || v == nil {
		return def, true
	}
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		if n != float64(int(n)) {
			return 0, false
		}
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, false
		}
		return int(i), true
	default:
		return 0, false
	}
}
