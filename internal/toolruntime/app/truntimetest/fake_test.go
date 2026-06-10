package truntimetest_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/toolruntime/app"
	"github.com/boltrope/boltrope/internal/toolruntime/app/truntimetest"
	"github.com/boltrope/boltrope/internal/toolruntime/domain"
)

var ctx = context.Background()

// ---------------------------------------------------------------------------
// Interface satisfaction (compile-time assertions surfaced as test)
// ---------------------------------------------------------------------------

func TestCompileTimeAssertions(_ *testing.T) {
	var _ app.ToolRegistry = (*truntimetest.FakeToolRegistry)(nil)
	var _ app.RuntimePort = (*truntimetest.FakeRuntimePort)(nil)
	var _ app.Workspace = (*truntimetest.FakeWorkspace)(nil)
	var _ app.EgressBroker = (*truntimetest.FakeEgressBroker)(nil)
	var _ app.MCPClientPort = (*truntimetest.FakeMCPClient)(nil)
	var _ app.DedupStore = (*truntimetest.FakeDedupStore)(nil)
}

// ---------------------------------------------------------------------------
// FakeToolRegistry
// ---------------------------------------------------------------------------

func TestFakeToolRegistry_RegisterAndGet(t *testing.T) {
	reg := truntimetest.NewFakeToolRegistry()
	spec := domain.ToolSpec{
		Name:        "bash",
		Description: "run a bash command",
		JSONSchema:  json.RawMessage(`{"type":"object"}`),
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
	}
	tool := truntimetest.NewFakeTool(spec)
	err := reg.Register(ctx, tool)
	require.NoError(t, err)

	got, err := reg.Get(ctx, "bash")
	require.NoError(t, err)
	assert.Equal(t, "bash", got.Spec().Name)
}

func TestFakeToolRegistry_DuplicateReturnsError(t *testing.T) {
	reg := truntimetest.NewFakeToolRegistry()
	spec := domain.ToolSpec{Name: "bash", JSONSchema: json.RawMessage(`{}`)}
	reg.Register(ctx, truntimetest.NewFakeTool(spec)) //nolint:errcheck
	err := reg.Register(ctx, truntimetest.NewFakeTool(spec))
	require.Error(t, err)
}

func TestFakeToolRegistry_NotFound(t *testing.T) {
	reg := truntimetest.NewFakeToolRegistry()
	_, err := reg.Get(ctx, "missing")
	assert.ErrorIs(t, err, app.ErrToolNotFound)
}

func TestFakeToolRegistry_List(t *testing.T) {
	reg := truntimetest.NewFakeToolRegistry()
	reg.Register(ctx, truntimetest.NewFakeTool(domain.ToolSpec{Name: "t1", JSONSchema: json.RawMessage(`{}`)})) //nolint:errcheck
	reg.Register(ctx, truntimetest.NewFakeTool(domain.ToolSpec{Name: "t2", JSONSchema: json.RawMessage(`{}`)})) //nolint:errcheck
	specs, err := reg.List(ctx)
	require.NoError(t, err)
	assert.Len(t, specs, 2)
}

// ---------------------------------------------------------------------------
// FakeWorkspace
// ---------------------------------------------------------------------------

func TestFakeWorkspace_ReadWrite(t *testing.T) {
	ws := truntimetest.NewFakeWorkspace()
	err := ws.Write(ctx, "/file.txt", []byte("hello"))
	require.NoError(t, err)
	data, err := ws.Read(ctx, "/file.txt")
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), data)
}

func TestFakeWorkspace_ReadNotFound(t *testing.T) {
	ws := truntimetest.NewFakeWorkspace()
	_, err := ws.Read(ctx, "/missing.txt")
	require.Error(t, err)
}

func TestFakeWorkspace_Exec(t *testing.T) {
	ws := truntimetest.NewFakeWorkspace()
	ws.AddExecResult(app.ExecResult{ExitCode: 0, Stdout: []byte("done")}, nil)
	res, err := ws.Exec(ctx, app.ExecRequest{Cmd: []string{"echo", "hello"}})
	require.NoError(t, err)
	assert.Equal(t, 0, res.ExitCode)
	assert.Equal(t, []byte("done"), res.Stdout)
}

// ---------------------------------------------------------------------------
// FakeRuntimePort
// ---------------------------------------------------------------------------

func TestFakeRuntimePort_CreateGetDestroy(t *testing.T) {
	rt := truntimetest.NewFakeRuntimePort()
	ws, err := rt.Create(ctx, "sess-1", app.EgressPolicy{SessionID: "sess-1"})
	require.NoError(t, err)
	require.NotNil(t, ws)

	ws2, err := rt.Get(ctx, "sess-1")
	require.NoError(t, err)
	assert.Equal(t, ws, ws2)

	err = rt.Destroy(ctx, "sess-1")
	require.NoError(t, err)

	_, err = rt.Get(ctx, "sess-1")
	require.Error(t, err) // destroyed
}

// ---------------------------------------------------------------------------
// FakeEgressBroker
// ---------------------------------------------------------------------------

func TestFakeEgressBroker_DenyByDefault(t *testing.T) {
	b := truntimetest.NewFakeEgressBroker()
	allowed, err := b.Allow(ctx, "sess", "example.com")
	require.NoError(t, err)
	assert.False(t, allowed, "no policy configured: must deny by default")
}

func TestFakeEgressBroker_Allowlisted(t *testing.T) {
	b := truntimetest.NewFakeEgressBroker()
	err := b.SetPolicy(ctx, app.EgressPolicy{
		SessionID:    "sess",
		AllowedHosts: []string{"example.com"},
	})
	require.NoError(t, err)
	allowed, err := b.Allow(ctx, "sess", "example.com")
	require.NoError(t, err)
	assert.True(t, allowed)

	notAllowed, err := b.Allow(ctx, "sess", "evil.com")
	require.NoError(t, err)
	assert.False(t, notAllowed)
}

// ---------------------------------------------------------------------------
// FakeDedupStore
// ---------------------------------------------------------------------------

func TestFakeDedupStore_BeginComplete(t *testing.T) {
	ds := truntimetest.NewFakeDedupStore()
	rec := app.ExecutionRecord{
		TenantID:       "t1",
		SessionID:      "s1",
		IdempotencyKey: "key1",
	}
	started, err := ds.Begin(ctx, rec)
	require.NoError(t, err)
	assert.Equal(t, app.ExecStarted, started.Status)

	// Second Begin with same key returns existing record.
	same, err := ds.Begin(ctx, rec)
	require.NoError(t, err)
	assert.Equal(t, app.ExecStarted, same.Status)

	// Complete it.
	rec.Status = app.ExecCompleted
	rec.Result = domain.Observation{Content: "result"}
	err = ds.Complete(ctx, rec)
	require.NoError(t, err)

	got, err := ds.Lookup(ctx, "t1", "s1", "key1")
	require.NoError(t, err)
	assert.Equal(t, app.ExecCompleted, got.Status)
	assert.Equal(t, "result", got.Result.Content)
}

func TestFakeDedupStore_LookupMissing(t *testing.T) {
	ds := truntimetest.NewFakeDedupStore()
	_, err := ds.Lookup(ctx, "t", "s", "no-such-key")
	require.Error(t, err)
}
