// RED (TDD) — ADR-0030 AC-5. The execute.Service must propagate the request's
// TenantID into the context the tool sees, so a tenant-scoped store the tool calls
// downstream (the memory tools' app.MemoryStore) is RLS/tenant-scoped at execution
// time. This in-package test registers a tool that captures the ctx it is invoked
// with and asserts the tenant is recoverable via the toolruntime tenant-context
// helper.
//
// The propagation (execCtx = tenantctx.WithTenant(execCtx, req.TenantID) before
// tool.Execute) does not exist yet, so this test is expected to FAIL (the captured
// ctx carries no tenant) until execute.Service is wired (feature absent). The
// import of the tenant helper additionally fails to compile until that package
// exists.
package execute

import (
	"context"
	"testing"

	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
	tenantctx "github.com/xd1lab/harness-ai/internal/toolruntime/infra/tenant"
)

// ctxCapturingTool records the context passed to its Execute so the test can assert
// the service-propagated tenant is present.
type ctxCapturingTool struct {
	spec        domain.ToolSpec
	capturedCtx context.Context
}

func (c *ctxCapturingTool) Spec() domain.ToolSpec { return c.spec }

func (c *ctxCapturingTool) Execute(ctx context.Context, _ string, _ map[string]any) (domain.Observation, error) {
	c.capturedCtx = ctx
	return domain.Observation{Content: "ok"}, nil
}

// TestExecutePropagatesTenantToToolContext asserts AC-5: the tool's Execute ctx
// carries the request tenant via the toolruntime tenant helper.
func TestExecutePropagatesTenantToToolContext(t *testing.T) {
	f := newFixture(t)
	tool := &ctxCapturingTool{spec: domain.ToolSpec{
		Name:        "capture",
		Description: "captures ctx",
		JSONSchema:  objectSchema,
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
	}}
	mustRegister(t, f.reg, tool)

	_, err := f.svc.Execute(context.Background(), Request{
		TenantID:       "tenant-XYZ",
		SessionID:      "sess1",
		CallID:         "call1",
		ToolName:       "capture",
		Args:           map[string]any{"x": "hi"},
		IdempotencyKey: "key-cap-1",
	}, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if tool.capturedCtx == nil {
		t.Fatal("tool was never executed (captured ctx is nil)")
	}
	got, terr := tenantctx.TenantFromContext(tool.capturedCtx)
	if terr != nil {
		t.Fatalf("tool ctx carried no tenant: %v — execute.Service did not propagate req.TenantID (AC-5)", terr)
	}
	if got != "tenant-XYZ" {
		t.Errorf("tool ctx tenant = %q, want %q", got, "tenant-XYZ")
	}
}
