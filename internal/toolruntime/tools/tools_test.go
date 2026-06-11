// Tests for the native tools (FR-TOOL-02). They assert each tool's declared
// SideEffect/EgressClass (AC-1), that each tool issues the right Workspace op,
// and that the external-comms tools are gated by the EgressBroker (FR-TOOL-06).
// Schema validation is exercised separately in the registry tests (FR-TOOL-01);
// here the tools are invoked directly with already-decoded args, and the
// defensive missing-required-field paths are checked too.
package tools_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app/truntimetest"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
	"github.com/xd1lab/harness-ai/internal/toolruntime/tools"
)

const testSession = "sess-1"

// wsFor adapts a single fake workspace into the per-session resolver the tools
// bind to; most unit tests exercise a single session against one fake.
// Per-session ROUTING is asserted separately in
// TestToolsRouteToCallingSessionsWorkspace.
func wsFor(ws app.Workspace) app.SessionWorkspaces {
	return truntimetest.StaticWorkspaces{WS: ws}
}

// TestToolsRouteToCallingSessionsWorkspace is the X-03 regression test at the
// tool layer: a tool executed for session S must operate on S's OWN workspace —
// the sessionID the [domain.Tool] contract carries drives the routing, never a
// fixed workspace binding shared by all sessions.
func TestToolsRouteToCallingSessionsWorkspace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	router := truntimetest.NewFakeSessionWorkspaces()
	write := tools.NewWriteTool(router)
	read := tools.NewReadTool(router)
	bash := tools.NewBashTool(router)

	// Session A writes a file.
	obs, err := write.Execute(ctx, "sess-a", map[string]any{"path": "/workspace/hello.txt", "content": "A's data"})
	if err != nil || obs.IsError {
		t.Fatalf("write in sess-a failed: err=%v obs=%q", err, obs.Content)
	}

	// Session A reads it back from its own workspace.
	obs, err = read.Execute(ctx, "sess-a", map[string]any{"path": "/workspace/hello.txt"})
	if err != nil || obs.IsError || obs.Content != "A's data" {
		t.Fatalf("read in sess-a = (err=%v, isErr=%v, %q); want A's data", err, obs.IsError, obs.Content)
	}

	// Session B must NOT see session A's file (cross-session isolation).
	obs, err = read.Execute(ctx, "sess-b", map[string]any{"path": "/workspace/hello.txt"})
	if err != nil {
		t.Fatalf("read in sess-b returned a Go error; want error-as-observation: %v", err)
	}
	if !obs.IsError {
		t.Fatalf("session B read session A's file: %q — cross-session workspace leak", obs.Content)
	}

	// bash executes inside the CALLING session's workspace only.
	if obs, err = bash.Execute(ctx, "sess-a", map[string]any{"command": "hostname"}); err != nil || obs.IsError {
		t.Fatalf("bash in sess-a failed: err=%v obs=%q", err, obs.Content)
	}
	if got := len(router.Get("sess-a").ExecLog); got != 1 {
		t.Errorf("sess-a workspace ExecLog = %d commands; want 1", got)
	}
	if got := len(router.Get("sess-b").ExecLog); got != 0 {
		t.Errorf("sess-b workspace ExecLog = %d commands; want 0 (bash ran in the wrong session's sandbox)", got)
	}
}

// TestClassifications is the FR-TOOL-02 AC-1 table: each core tool's declared
// SideEffect and EgressClass must match the spec.
func TestClassifications(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()

	type want struct {
		side   domain.SideEffect
		egress domain.EgressClass
	}
	cases := map[string]want{
		"read":      {domain.SideEffectReadOnly, domain.EgressClassNone},
		"glob":      {domain.SideEffectReadOnly, domain.EgressClassNone},
		"grep":      {domain.SideEffectReadOnly, domain.EgressClassNone},
		"write":     {domain.SideEffectMutating, domain.EgressClassNone},
		"edit":      {domain.SideEffectMutating, domain.EgressClassNone},
		"bash":      {domain.SideEffectMutating, domain.EgressClassNone},
		"webfetch":  {domain.SideEffectMutating, domain.EgressClassExternal},
		"websearch": {domain.SideEffectMutating, domain.EgressClassExternal},
	}

	got := tools.Native(wsFor(ws), newFakeFetcher(), "")
	if len(got) != len(cases) {
		t.Fatalf("Native returned %d tools; want %d", len(got), len(cases))
	}
	for _, tool := range got {
		spec := tool.Spec()
		w, ok := cases[spec.Name]
		if !ok {
			t.Errorf("unexpected tool %q", spec.Name)
			continue
		}
		if spec.SideEffect != w.side {
			t.Errorf("%s: SideEffect = %q; want %q", spec.Name, spec.SideEffect, w.side)
		}
		if spec.EgressClass != w.egress {
			t.Errorf("%s: EgressClass = %q; want %q", spec.Name, spec.EgressClass, w.egress)
		}
		if spec.Description == "" {
			t.Errorf("%s: Description is empty", spec.Name)
		}
		if len(spec.JSONSchema) == 0 {
			t.Errorf("%s: JSONSchema is empty", spec.Name)
		}
	}
}

func TestReadTool(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	if err := ws.Write(context.Background(), "/a.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	tool := tools.NewReadTool(wsFor(ws))

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"path": "/a.txt"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if obs.IsError {
		t.Fatalf("read returned error obs: %q", obs.Content)
	}
	if obs.Content != "hello" {
		t.Errorf("read content = %q; want %q", obs.Content, "hello")
	}
}

func TestReadToolMissingFieldIsErrorObs(t *testing.T) {
	t.Parallel()

	tool := tools.NewReadTool(wsFor(truntimetest.NewFakeWorkspace()))
	obs, err := tool.Execute(context.Background(), testSession, map[string]any{})
	if err != nil {
		t.Fatalf("Execute returned a Go error; want error-as-observation: %v", err)
	}
	if !obs.IsError {
		t.Errorf("missing path: IsError = false; want true")
	}
}

func TestReadToolMissingFileIsErrorObs(t *testing.T) {
	t.Parallel()

	tool := tools.NewReadTool(wsFor(truntimetest.NewFakeWorkspace()))
	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"path": "/nope"})
	if err != nil {
		t.Fatalf("Execute returned a Go error: %v", err)
	}
	if !obs.IsError {
		t.Errorf("missing file: IsError = false; want true")
	}
}

func TestWriteTool(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	tool := tools.NewWriteTool(wsFor(ws))

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"path": "/out.txt", "content": "data"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if obs.IsError {
		t.Fatalf("write returned error obs: %q", obs.Content)
	}
	got, err := ws.Read(context.Background(), "/out.txt")
	if err != nil {
		t.Fatalf("workspace read-back: %v", err)
	}
	if string(got) != "data" {
		t.Errorf("written content = %q; want %q", string(got), "data")
	}
}

func TestWriteToolMissingContentIsErrorObs(t *testing.T) {
	t.Parallel()

	tool := tools.NewWriteTool(wsFor(truntimetest.NewFakeWorkspace()))
	obs, _ := tool.Execute(context.Background(), testSession, map[string]any{"path": "/x"})
	if !obs.IsError {
		t.Errorf("missing content: IsError = false; want true")
	}
}

func TestEditToolReplacesUnique(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	if err := ws.Write(context.Background(), "/f.txt", []byte("foo bar baz")); err != nil {
		t.Fatal(err)
	}
	tool := tools.NewEditTool(wsFor(ws))

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{
		"path": "/f.txt", "old_string": "bar", "new_string": "QUX",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if obs.IsError {
		t.Fatalf("edit returned error obs: %q", obs.Content)
	}
	got, _ := ws.Read(context.Background(), "/f.txt")
	if string(got) != "foo QUX baz" {
		t.Errorf("edited content = %q; want %q", string(got), "foo QUX baz")
	}
}

func TestEditToolNonUniqueWithoutReplaceAllIsError(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	if err := ws.Write(context.Background(), "/f.txt", []byte("a a a")); err != nil {
		t.Fatal(err)
	}
	tool := tools.NewEditTool(wsFor(ws))

	obs, _ := tool.Execute(context.Background(), testSession, map[string]any{
		"path": "/f.txt", "old_string": "a", "new_string": "b",
	})
	if !obs.IsError {
		t.Errorf("non-unique edit without replace_all: IsError = false; want true")
	}
	// File must be unchanged.
	got, _ := ws.Read(context.Background(), "/f.txt")
	if string(got) != "a a a" {
		t.Errorf("file mutated on failed edit: %q", string(got))
	}
}

func TestEditToolReplaceAll(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	if err := ws.Write(context.Background(), "/f.txt", []byte("a a a")); err != nil {
		t.Fatal(err)
	}
	tool := tools.NewEditTool(wsFor(ws))

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{
		"path": "/f.txt", "old_string": "a", "new_string": "b", "replace_all": true,
	})
	if err != nil || obs.IsError {
		t.Fatalf("replace_all edit failed: err=%v obs=%q", err, obs.Content)
	}
	got, _ := ws.Read(context.Background(), "/f.txt")
	if string(got) != "b b b" {
		t.Errorf("replace_all content = %q; want %q", string(got), "b b b")
	}
}

func TestEditToolOldStringNotFoundIsError(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	if err := ws.Write(context.Background(), "/f.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	tool := tools.NewEditTool(wsFor(ws))
	obs, _ := tool.Execute(context.Background(), testSession, map[string]any{
		"path": "/f.txt", "old_string": "zzz", "new_string": "b",
	})
	if !obs.IsError {
		t.Errorf("old_string-not-found: IsError = false; want true")
	}
}

func TestBashToolRunsExec(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	ws.AddExecResult(app.ExecResult{ExitCode: 0, Stdout: []byte("output")}, nil)
	tool := tools.NewBashTool(wsFor(ws))

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"command": "echo output"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if obs.IsError {
		t.Fatalf("bash returned error obs: %q", obs.Content)
	}
	if obs.Content != "output" {
		t.Errorf("bash content = %q; want %q", obs.Content, "output")
	}
	if len(ws.ExecLog) != 1 {
		t.Fatalf("expected exactly one Exec; got %d", len(ws.ExecLog))
	}
	cmd := ws.ExecLog[0].Cmd
	if len(cmd) != 3 || cmd[0] != "sh" || cmd[1] != "-c" || cmd[2] != "echo output" {
		t.Errorf("bash Exec cmd = %v; want [sh -c \"echo output\"]", cmd)
	}
}

func TestBashToolNonZeroExitIsErrorObs(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	ws.AddExecResult(app.ExecResult{ExitCode: 2, Stderr: []byte("boom")}, nil)
	tool := tools.NewBashTool(wsFor(ws))

	obs, _ := tool.Execute(context.Background(), testSession, map[string]any{"command": "false"})
	if !obs.IsError {
		t.Errorf("non-zero exit: IsError = false; want true")
	}
	if !strings.Contains(obs.Content, "boom") {
		t.Errorf("expected stderr in content; got %q", obs.Content)
	}
}

func TestBashToolKilledIsErrorObs(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	ws.AddExecResult(app.ExecResult{Killed: true}, nil)
	tool := tools.NewBashTool(wsFor(ws))

	obs, _ := tool.Execute(context.Background(), testSession, map[string]any{"command": "sleep 999"})
	if !obs.IsError {
		t.Errorf("killed process: IsError = false; want true")
	}
}

func TestGlobToolRunsExec(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	ws.AddExecResult(app.ExecResult{Stdout: []byte("./a.go\n./b.go")}, nil)
	tool := tools.NewGlobTool(wsFor(ws))

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"pattern": "**/*.go"})
	if err != nil || obs.IsError {
		t.Fatalf("glob failed: err=%v obs=%q", err, obs.Content)
	}
	if len(ws.ExecLog) != 1 {
		t.Fatalf("expected one Exec; got %d", len(ws.ExecLog))
	}
	if ws.ExecLog[0].Cmd[0] != "find" {
		t.Errorf("glob should run find; got %v", ws.ExecLog[0].Cmd)
	}
}

func TestGrepToolRunsExec(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	ws.AddExecResult(app.ExecResult{Stdout: []byte("a.go:1:match")}, nil)
	tool := tools.NewGrepTool(wsFor(ws))

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"pattern": "match"})
	if err != nil || obs.IsError {
		t.Fatalf("grep failed: err=%v obs=%q", err, obs.Content)
	}
	if ws.ExecLog[0].Cmd[0] != "grep" {
		t.Errorf("grep should run grep; got %v", ws.ExecLog[0].Cmd)
	}
}

func TestGrepToolNoMatchIsNotError(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	// grep exits 1 with no output when nothing matches.
	ws.AddExecResult(app.ExecResult{ExitCode: 1}, nil)
	tool := tools.NewGrepTool(wsFor(ws))

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"pattern": "nope"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if obs.IsError {
		t.Errorf("grep no-match should not be an error obs; got %q", obs.Content)
	}
}

// --- web tools: egress data path (FR-TOOL-06; ADR-0021) ---
//
// The broker mediation now lives INSIDE the WebFetcher (exercised exhaustively
// in the egressclient package tests). At the tool layer the fetcher is faked:
// these tests assert the tool's behavior — denial surfaced as an error
// observation with the canonical wording, success rendered for the model, the
// egress TARGET each tool declares to the service gate, and that a denied fetch
// produced no output for the model.

// fakeFetcher is a scripted app.WebFetcher: it returns a preset result/error
// and records the URLs it was asked to fetch.
type fakeFetcher struct {
	result  app.FetchResult
	err     error
	fetched []string
}

func newFakeFetcher() *fakeFetcher { return &fakeFetcher{result: app.FetchResult{Status: 200}} }

func (f *fakeFetcher) Fetch(_ context.Context, _, rawURL string) (app.FetchResult, error) {
	f.fetched = append(f.fetched, rawURL)
	if f.err != nil {
		return app.FetchResult{}, f.err
	}
	return f.result, nil
}

func TestWebFetchDeniedSurfacesEgressError(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{err: fmt.Errorf(`egressclient: egress denied: host %q is not on the session allowlist`, "evil.example")}
	tool := tools.NewWebFetchTool(f)

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"url": "https://evil.example/x"})
	if err != nil {
		t.Fatalf("Execute returned a Go error; want error-as-observation: %v", err)
	}
	if !obs.IsError {
		t.Fatalf("denied fetch: IsError = false; want true")
	}
	if !strings.Contains(strings.ToLower(obs.Content), "egress denied") {
		t.Errorf("expected egress-denied message; got %q", obs.Content)
	}
	// The canonical wording must be preserved verbatim (no double prefix).
	if !strings.Contains(obs.Content, `egress denied: host "evil.example" is not on the session allowlist`) {
		t.Errorf("canonical denial wording lost: %q", obs.Content)
	}
}

func TestWebFetchSuccessRendersBodyAndStatus(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{result: app.FetchResult{
		Status:      200,
		FinalURL:    "https://ok.example/page",
		ContentType: "text/html",
		Body:        []byte("<html/>"),
	}}
	tool := tools.NewWebFetchTool(f)

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"url": "https://ok.example/page"})
	if err != nil || obs.IsError {
		t.Fatalf("allowed webfetch failed: err=%v obs=%q", err, obs.Content)
	}
	if !strings.Contains(obs.Content, "<html/>") {
		t.Errorf("webfetch content missing body: %q", obs.Content)
	}
	if !strings.Contains(obs.Content, "HTTP 200") {
		t.Errorf("webfetch content missing status line: %q", obs.Content)
	}
	if len(f.fetched) != 1 || f.fetched[0] != "https://ok.example/page" {
		t.Errorf("fetcher asked for %v; want one fetch of the URL", f.fetched)
	}
}

func TestWebFetchMissingURLNeverFetches(t *testing.T) {
	t.Parallel()

	f := newFakeFetcher()
	tool := tools.NewWebFetchTool(f)

	obs, _ := tool.Execute(context.Background(), testSession, map[string]any{})
	if !obs.IsError {
		t.Errorf("missing url: IsError = false; want true")
	}
	if len(f.fetched) != 0 {
		t.Errorf("missing-url webfetch still called the fetcher: %v", f.fetched)
	}
}

func TestWebFetchEgressTargetIsURLHost(t *testing.T) {
	t.Parallel()

	tool := tools.NewWebFetchTool(newFakeFetcher())
	host, ok := tool.EgressTarget(map[string]any{"url": "https://api.example.com/v1"})
	if !ok || host != "api.example.com" {
		t.Errorf("EgressTarget = (%q, %v); want (api.example.com, true)", host, ok)
	}
	if _, ok := tool.EgressTarget(map[string]any{"url": "not a url"}); ok {
		t.Error("EgressTarget(unparseable) ok = true; want false (fail closed)")
	}
}

func TestWebSearchDeniedSurfacesEgressError(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{err: fmt.Errorf(`egressclient: egress denied: host %q is not on the session allowlist`, tools.DefaultSearchHost)}
	tool := tools.NewWebSearchTool(f, "")

	obs, _ := tool.Execute(context.Background(), testSession, map[string]any{"query": "secrets"})
	if !obs.IsError {
		t.Fatalf("denied websearch: IsError = false; want true")
	}
	if !strings.Contains(strings.ToLower(obs.Content), "egress denied") {
		t.Errorf("expected egress-denied message; got %q", obs.Content)
	}
}

func TestWebSearchRendersResults(t *testing.T) {
	t.Parallel()

	body := `{"results":[
		{"title":"First","url":"https://a.example/1","content":"snippet one"},
		{"title":"Second","url":"https://b.example/2","content":"snippet two"}
	]}`
	f := &fakeFetcher{result: app.FetchResult{Status: 200, Body: []byte(body)}}
	tool := tools.NewWebSearchTool(f, "https://search.example/search")

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"query": "weather"})
	if err != nil || obs.IsError {
		t.Fatalf("allowed websearch failed: err=%v obs=%q", err, obs.Content)
	}
	for _, want := range []string{"First", "https://a.example/1", "snippet one", "Second"} {
		if !strings.Contains(obs.Content, want) {
			t.Errorf("rendered results missing %q: %q", want, obs.Content)
		}
	}
	// The query must be sent to the configured backend as a SearXNG JSON query.
	if len(f.fetched) != 1 || !strings.Contains(f.fetched[0], "format=json") || !strings.Contains(f.fetched[0], "q=weather") {
		t.Errorf("search request = %v; want the configured endpoint with q + format=json", f.fetched)
	}
}

func TestWebSearchEgressTargetIsBackendHost(t *testing.T) {
	t.Parallel()

	tool := tools.NewWebSearchTool(newFakeFetcher(), "https://search.example/search")
	host, ok := tool.EgressTarget(map[string]any{"query": "anything"})
	if !ok || host != "search.example" {
		t.Errorf("EgressTarget = (%q, %v); want (search.example, true)", host, ok)
	}

	// The default backend's host is the documented DefaultSearchHost.
	def := tools.NewWebSearchTool(newFakeFetcher(), "")
	if host, _ := def.EgressTarget(nil); host != tools.DefaultSearchHost {
		t.Errorf("default EgressTarget host = %q; want %q", host, tools.DefaultSearchHost)
	}
}

func TestWebSearchNon2xxIsError(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{result: app.FetchResult{Status: 502, Body: []byte("bad gateway")}}
	tool := tools.NewWebSearchTool(f, "https://search.example/search")

	obs, _ := tool.Execute(context.Background(), testSession, map[string]any{"query": "x"})
	if !obs.IsError {
		t.Errorf("non-2xx search backend: IsError = false; want true")
	}
}
