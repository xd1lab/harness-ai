// RED (TDD) — ADR-0030 AC-9 / AC-10 / AC-14. Tests for the three model-facing
// memory tools (memory_write, memory_read, memory_search) built from an
// app.MemoryStore. They assert:
//   - declared classifications (memory_write Mutating/None; memory_read &
//     memory_search ReadOnly/None) and non-empty model-facing descriptions;
//   - the EXACT JSON schemas (required fields, properties, additionalProperties);
//   - happy paths: write-then-read round-trips; search by substring and by tag;
//   - error-as-observation paths: a missing required field is an IsError
//     observation with a NIL Go error (never a panic, never a Go error);
//   - a read miss is a NON-error observation (a miss is a normal model outcome).
//
// The tools and the in-memory store they are driven against do not exist yet, so
// this file is expected to FAIL to compile until the feature lands (feature absent).
package tools_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/memory/inmem"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
	tenantctx "github.com/xd1lab/harness-ai/internal/toolruntime/infra/tenant"
	"github.com/xd1lab/harness-ai/internal/toolruntime/tools"
)

// memCtx returns a context scoped to a fixed tenant so the store the tools call is
// RLS/tenant-scoped at execution time (mirrors execute.Service propagation, AC-5).
func memCtx() context.Context {
	return tenantctx.WithTenant(context.Background(), "tenant-mem-test")
}

// newMemStore builds a fresh in-memory store as the app.MemoryStore the tools wrap.
func newMemStore() app.MemoryStore { return inmem.New() }

// TestMemoryToolClassifications pins the AC-9 SideEffect/EgressClass table and that
// each tool carries a non-empty model-facing description and JSON schema.
func TestMemoryToolClassifications(t *testing.T) {
	t.Parallel()
	st := newMemStore()

	type want struct {
		name   string
		side   domain.SideEffect
		egress domain.EgressClass
	}
	cases := []struct {
		tool domain.Tool
		want want
	}{
		{tools.NewMemoryWriteTool(st), want{"memory_write", domain.SideEffectMutating, domain.EgressClassNone}},
		{tools.NewMemoryReadTool(st), want{"memory_read", domain.SideEffectReadOnly, domain.EgressClassNone}},
		{tools.NewMemorySearchTool(st), want{"memory_search", domain.SideEffectReadOnly, domain.EgressClassNone}},
	}
	for _, c := range cases {
		spec := c.tool.Spec()
		if spec.Name != c.want.name {
			t.Errorf("name = %q, want %q", spec.Name, c.want.name)
		}
		if spec.SideEffect != c.want.side {
			t.Errorf("%s: SideEffect = %q, want %q", spec.Name, spec.SideEffect, c.want.side)
		}
		if spec.EgressClass != c.want.egress {
			t.Errorf("%s: EgressClass = %q, want %q", spec.Name, spec.EgressClass, c.want.egress)
		}
		if strings.TrimSpace(spec.Description) == "" {
			t.Errorf("%s: Description is empty", spec.Name)
		}
		if !json.Valid(spec.JSONSchema) {
			t.Errorf("%s: JSONSchema is not valid JSON: %s", spec.Name, spec.JSONSchema)
		}
	}
}

// schemaShape is the subset of a JSON Schema the tests assert on.
type schemaShape struct {
	Type                 string                     `json:"type"`
	Required             []string                   `json:"required"`
	AdditionalProperties *bool                      `json:"additionalProperties"`
	Properties           map[string]json.RawMessage `json:"properties"`
}

func parseSchema(t *testing.T, raw json.RawMessage) schemaShape {
	t.Helper()
	var s schemaShape
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("unmarshal schema: %v\n%s", err, raw)
	}
	return s
}

// TestMemoryWriteSchema pins memory_write's exact schema shape (AC-10).
func TestMemoryWriteSchema(t *testing.T) {
	t.Parallel()
	s := parseSchema(t, tools.NewMemoryWriteTool(newMemStore()).Spec().JSONSchema)

	if s.Type != "object" {
		t.Errorf("type = %q, want object", s.Type)
	}
	if s.AdditionalProperties == nil || *s.AdditionalProperties {
		t.Errorf("additionalProperties = %v, want false", s.AdditionalProperties)
	}
	wantRequired := map[string]bool{"key": true, "value": true}
	if len(s.Required) != 2 {
		t.Errorf("required = %v, want exactly [key value]", s.Required)
	}
	for _, r := range s.Required {
		if !wantRequired[r] {
			t.Errorf("unexpected required field %q", r)
		}
	}
	for _, p := range []string{"namespace", "key", "value", "tags"} {
		if _, ok := s.Properties[p]; !ok {
			t.Errorf("memory_write schema missing property %q", p)
		}
	}
}

// TestMemoryReadSchema pins memory_read's exact schema shape (AC-10).
func TestMemoryReadSchema(t *testing.T) {
	t.Parallel()
	s := parseSchema(t, tools.NewMemoryReadTool(newMemStore()).Spec().JSONSchema)

	if s.Type != "object" {
		t.Errorf("type = %q, want object", s.Type)
	}
	if s.AdditionalProperties == nil || *s.AdditionalProperties {
		t.Errorf("additionalProperties = %v, want false", s.AdditionalProperties)
	}
	if len(s.Required) != 1 || s.Required[0] != "key" {
		t.Errorf("required = %v, want [key]", s.Required)
	}
	for _, p := range []string{"namespace", "key"} {
		if _, ok := s.Properties[p]; !ok {
			t.Errorf("memory_read schema missing property %q", p)
		}
	}
}

// TestMemorySearchSchema pins memory_search's exact schema shape (AC-10): no
// required fields, three optional properties.
func TestMemorySearchSchema(t *testing.T) {
	t.Parallel()
	s := parseSchema(t, tools.NewMemorySearchTool(newMemStore()).Spec().JSONSchema)

	if s.Type != "object" {
		t.Errorf("type = %q, want object", s.Type)
	}
	if s.AdditionalProperties == nil || *s.AdditionalProperties {
		t.Errorf("additionalProperties = %v, want false", s.AdditionalProperties)
	}
	if len(s.Required) != 0 {
		t.Errorf("required = %v, want none", s.Required)
	}
	for _, p := range []string{"query", "tags", "limit"} {
		if _, ok := s.Properties[p]; !ok {
			t.Errorf("memory_search schema missing property %q", p)
		}
	}
}

// TestMemoryWriteThenRead exercises the round-trip through the tools against the
// in-mem store, carrying the tenant in the context (as execute.Service does).
func TestMemoryWriteThenRead(t *testing.T) {
	t.Parallel()
	st := newMemStore()
	write := tools.NewMemoryWriteTool(st)
	read := tools.NewMemoryReadTool(st)
	ctx := memCtx()

	obs, err := write.Execute(ctx, "sess-1", map[string]any{
		"key":   "deploy-target",
		"value": "us-east-1",
		"tags":  []any{"infra", "decision"},
	})
	if err != nil {
		t.Fatalf("memory_write Execute: %v", err)
	}
	if obs.IsError {
		t.Fatalf("memory_write returned error obs: %q", obs.Content)
	}

	obs, err = read.Execute(ctx, "sess-1", map[string]any{"key": "deploy-target"})
	if err != nil {
		t.Fatalf("memory_read Execute: %v", err)
	}
	if obs.IsError {
		t.Fatalf("memory_read returned error obs: %q", obs.Content)
	}
	if !strings.Contains(obs.Content, "us-east-1") {
		t.Errorf("memory_read content = %q, want it to contain the stored value", obs.Content)
	}
}

// TestMemoryReadMissIsNonError verifies a read of an absent key is a non-error
// observation (a miss is a normal model-visible outcome, like websearch "No results.").
func TestMemoryReadMissIsNonError(t *testing.T) {
	t.Parallel()
	read := tools.NewMemoryReadTool(newMemStore())

	obs, err := read.Execute(memCtx(), "sess-1", map[string]any{"key": "absent"})
	if err != nil {
		t.Fatalf("memory_read miss returned a Go error: %v", err)
	}
	if obs.IsError {
		t.Errorf("memory_read miss: IsError = true, want false (a miss is a normal outcome)")
	}
}

// TestMemorySearchBySubstring exercises substring recall through the tool.
func TestMemorySearchBySubstring(t *testing.T) {
	t.Parallel()
	st := newMemStore()
	write := tools.NewMemoryWriteTool(st)
	search := tools.NewMemorySearchTool(st)
	ctx := memCtx()

	if obs, err := write.Execute(ctx, "s", map[string]any{"key": "k1", "value": "the user prefers dark mode"}); err != nil || obs.IsError {
		t.Fatalf("seed write: err=%v obs=%q", err, obs.Content)
	}
	if obs, err := write.Execute(ctx, "s", map[string]any{"key": "k2", "value": "unrelated note"}); err != nil || obs.IsError {
		t.Fatalf("seed write 2: err=%v obs=%q", err, obs.Content)
	}

	obs, err := search.Execute(ctx, "s", map[string]any{"query": "dark mode"})
	if err != nil {
		t.Fatalf("memory_search Execute: %v", err)
	}
	if obs.IsError {
		t.Fatalf("memory_search returned error obs: %q", obs.Content)
	}
	if !strings.Contains(obs.Content, "k1") {
		t.Errorf("memory_search content = %q, want it to surface key k1", obs.Content)
	}
	if strings.Contains(obs.Content, "k2") {
		t.Errorf("memory_search content = %q, must NOT surface non-matching key k2", obs.Content)
	}
}

// TestMemorySearchByTag exercises tag-AND recall through the tool.
func TestMemorySearchByTag(t *testing.T) {
	t.Parallel()
	st := newMemStore()
	write := tools.NewMemoryWriteTool(st)
	search := tools.NewMemorySearchTool(st)
	ctx := memCtx()

	if obs, err := write.Execute(ctx, "s", map[string]any{"key": "k1", "value": "v1", "tags": []any{"alpha", "beta"}}); err != nil || obs.IsError {
		t.Fatalf("seed write: err=%v obs=%q", err, obs.Content)
	}
	if obs, err := write.Execute(ctx, "s", map[string]any{"key": "k2", "value": "v2", "tags": []any{"alpha"}}); err != nil || obs.IsError {
		t.Fatalf("seed write 2: err=%v obs=%q", err, obs.Content)
	}

	obs, err := search.Execute(ctx, "s", map[string]any{"tags": []any{"alpha", "beta"}})
	if err != nil {
		t.Fatalf("memory_search Execute: %v", err)
	}
	if obs.IsError {
		t.Fatalf("memory_search returned error obs: %q", obs.Content)
	}
	if !strings.Contains(obs.Content, "k1") {
		t.Errorf("memory_search tags [alpha,beta] content = %q, want k1", obs.Content)
	}
	if strings.Contains(obs.Content, "k2") {
		t.Errorf("memory_search tags [alpha,beta] content = %q, must NOT include k2 (only one tag)", obs.Content)
	}
}

// TestMemoryWriteMissingKeyIsErrorObs verifies a missing required "key" is an
// IsError observation with a NIL Go error (never a panic / Go error).
func TestMemoryWriteMissingKeyIsErrorObs(t *testing.T) {
	t.Parallel()
	write := tools.NewMemoryWriteTool(newMemStore())

	obs, err := write.Execute(memCtx(), "s", map[string]any{"value": "v"})
	if err != nil {
		t.Fatalf("memory_write missing key returned a Go error; want error-as-observation: %v", err)
	}
	if !obs.IsError {
		t.Errorf("memory_write missing key: IsError = false, want true")
	}
}

// TestMemoryWriteMissingValueIsErrorObs verifies a missing required "value" is an
// IsError observation with a nil Go error.
func TestMemoryWriteMissingValueIsErrorObs(t *testing.T) {
	t.Parallel()
	write := tools.NewMemoryWriteTool(newMemStore())

	obs, err := write.Execute(memCtx(), "s", map[string]any{"key": "k"})
	if err != nil {
		t.Fatalf("memory_write missing value returned a Go error: %v", err)
	}
	if !obs.IsError {
		t.Errorf("memory_write missing value: IsError = false, want true")
	}
}

// TestMemoryReadMissingKeyIsErrorObs verifies a missing required "key" on
// memory_read is an IsError observation with a nil Go error.
func TestMemoryReadMissingKeyIsErrorObs(t *testing.T) {
	t.Parallel()
	read := tools.NewMemoryReadTool(newMemStore())

	obs, err := read.Execute(memCtx(), "s", map[string]any{})
	if err != nil {
		t.Fatalf("memory_read missing key returned a Go error: %v", err)
	}
	if !obs.IsError {
		t.Errorf("memory_read missing key: IsError = false, want true")
	}
}

// TestMemoryWriteFailClosedNoTenant verifies that when the context carries no
// tenant the store's fail-closed error is surfaced as an IsError observation (the
// tool surfaces a store error as an error observation, nil Go error).
func TestMemoryWriteFailClosedNoTenant(t *testing.T) {
	t.Parallel()
	write := tools.NewMemoryWriteTool(newMemStore())

	obs, err := write.Execute(context.Background(), "s", map[string]any{"key": "k", "value": "v"})
	if err != nil {
		t.Fatalf("memory_write no-tenant returned a Go error; want error-as-observation: %v", err)
	}
	if !obs.IsError {
		t.Errorf("memory_write no-tenant: IsError = false, want true (store fail-closed)")
	}
}
