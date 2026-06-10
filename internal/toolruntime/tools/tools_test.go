// Tests for the native tools (FR-TOOL-02). They assert each tool's declared
// SideEffect/EgressClass (AC-1), that each tool issues the right Workspace op,
// and that the external-comms tools are gated by the EgressBroker (FR-TOOL-06).
// Schema validation is exercised separately in the registry tests (FR-TOOL-01);
// here the tools are invoked directly with already-decoded args, and the
// defensive missing-required-field paths are checked too.
package tools_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app/truntimetest"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
	"github.com/xd1lab/harness-ai/internal/toolruntime/tools"
)

const testSession = "sess-1"

// TestClassifications is the FR-TOOL-02 AC-1 table: each core tool's declared
// SideEffect and EgressClass must match the spec.
func TestClassifications(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	broker := truntimetest.NewFakeEgressBroker()

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

	got := tools.Native(ws, broker)
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
	tool := tools.NewReadTool(ws)

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

	tool := tools.NewReadTool(truntimetest.NewFakeWorkspace())
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

	tool := tools.NewReadTool(truntimetest.NewFakeWorkspace())
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
	tool := tools.NewWriteTool(ws)

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

	tool := tools.NewWriteTool(truntimetest.NewFakeWorkspace())
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
	tool := tools.NewEditTool(ws)

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
	tool := tools.NewEditTool(ws)

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
	tool := tools.NewEditTool(ws)

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
	tool := tools.NewEditTool(ws)
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
	tool := tools.NewBashTool(ws)

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
	tool := tools.NewBashTool(ws)

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
	tool := tools.NewBashTool(ws)

	obs, _ := tool.Execute(context.Background(), testSession, map[string]any{"command": "sleep 999"})
	if !obs.IsError {
		t.Errorf("killed process: IsError = false; want true")
	}
}

func TestGlobToolRunsExec(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	ws.AddExecResult(app.ExecResult{Stdout: []byte("./a.go\n./b.go")}, nil)
	tool := tools.NewGlobTool(ws)

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
	tool := tools.NewGrepTool(ws)

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
	tool := tools.NewGrepTool(ws)

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"pattern": "nope"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if obs.IsError {
		t.Errorf("grep no-match should not be an error obs; got %q", obs.Content)
	}
}

// --- web tools: egress gating (FR-TOOL-06) ---

func TestWebFetchDeniedByDefault(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	broker := truntimetest.NewFakeEgressBroker() // no policy → deny all
	tool := tools.NewWebFetchTool(ws, broker)

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"url": "https://evil.example/x"})
	if err != nil {
		t.Fatalf("Execute returned a Go error; want error-as-observation: %v", err)
	}
	if !obs.IsError {
		t.Fatalf("deny-by-default: IsError = false; want true")
	}
	if !strings.Contains(strings.ToLower(obs.Content), "egress denied") {
		t.Errorf("expected egress-denied message; got %q", obs.Content)
	}
	// The fetch must NOT have run in the workspace.
	if len(ws.ExecLog) != 0 {
		t.Errorf("denied webfetch must not Exec; ran %d commands", len(ws.ExecLog))
	}
}

func TestWebFetchAllowedHostExecutes(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	ws.AddExecResult(app.ExecResult{Stdout: []byte("<html/>")}, nil)
	broker := truntimetest.NewFakeEgressBroker()
	if err := broker.SetPolicy(context.Background(), app.EgressPolicy{
		SessionID:    testSession,
		AllowedHosts: []string{"ok.example"},
	}); err != nil {
		t.Fatal(err)
	}
	tool := tools.NewWebFetchTool(ws, broker)

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"url": "https://ok.example/page"})
	if err != nil || obs.IsError {
		t.Fatalf("allowed webfetch failed: err=%v obs=%q", err, obs.Content)
	}
	if obs.Content != "<html/>" {
		t.Errorf("webfetch content = %q; want %q", obs.Content, "<html/>")
	}
	if len(ws.ExecLog) != 1 {
		t.Errorf("allowed webfetch should run one Exec; ran %d", len(ws.ExecLog))
	}
}

func TestWebFetchUnparseableHostFailsClosed(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	broker := truntimetest.NewFakeEgressBroker()
	if err := broker.SetPolicy(context.Background(), app.EgressPolicy{
		SessionID:    testSession,
		AllowedHosts: []string{"ok.example"},
	}); err != nil {
		t.Fatal(err)
	}
	tool := tools.NewWebFetchTool(ws, broker)

	obs, _ := tool.Execute(context.Background(), testSession, map[string]any{"url": "not a url"})
	if !obs.IsError {
		t.Errorf("unparseable URL: IsError = false; want true (fail closed)")
	}
	if len(ws.ExecLog) != 0 {
		t.Errorf("fail-closed webfetch must not Exec; ran %d", len(ws.ExecLog))
	}
}

func TestWebSearchDeniedByDefault(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	broker := truntimetest.NewFakeEgressBroker()
	tool := tools.NewWebSearchTool(ws, broker, "")

	obs, _ := tool.Execute(context.Background(), testSession, map[string]any{"query": "secrets"})
	if !obs.IsError {
		t.Fatalf("websearch deny-by-default: IsError = false; want true")
	}
	if len(ws.ExecLog) != 0 {
		t.Errorf("denied websearch must not Exec; ran %d", len(ws.ExecLog))
	}
}

func TestWebSearchAllowedExecutes(t *testing.T) {
	t.Parallel()

	ws := truntimetest.NewFakeWorkspace()
	ws.AddExecResult(app.ExecResult{Stdout: []byte("results")}, nil)
	broker := truntimetest.NewFakeEgressBroker()
	if err := broker.SetPolicy(context.Background(), app.EgressPolicy{
		SessionID:    testSession,
		AllowedHosts: []string{tools.DefaultSearchHost},
	}); err != nil {
		t.Fatal(err)
	}
	tool := tools.NewWebSearchTool(ws, broker, "")

	obs, err := tool.Execute(context.Background(), testSession, map[string]any{"query": "weather"})
	if err != nil || obs.IsError {
		t.Fatalf("allowed websearch failed: err=%v obs=%q", err, obs.Content)
	}
	if obs.Content != "results" {
		t.Errorf("websearch content = %q; want %q", obs.Content, "results")
	}
}
