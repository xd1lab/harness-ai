package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// execObservation renders an [app.ExecResult] into a [domain.Observation]. A
// killed process (cancellation or a hard resource limit; architecture §9.3) or a
// non-zero exit code is reported as an error observation with the captured
// stderr/stdout so the model can adapt.
func execObservation(res app.ExecResult) domain.Observation {
	out := string(res.Stdout)
	errOut := strings.TrimRight(string(res.Stderr), "\n")
	switch {
	case res.Killed:
		return domain.Observation{
			Content: "process terminated by the runtime (cancellation or resource limit): " + errOut,
			IsError: true,
		}
	case res.ExitCode != 0:
		content := fmt.Sprintf("exited with code %d", res.ExitCode)
		if errOut != "" {
			content += ": " + errOut
		}
		if out != "" {
			content += "\n" + out
		}
		return domain.Observation{Content: content, IsError: true}
	default:
		return domain.Observation{Content: out}
	}
}

// ---------------------------------------------------------------------------
// bash
// ---------------------------------------------------------------------------

// BashTool runs a shell command inside the calling session's workspace via
// [app.Workspace.Exec]. It is mutating (serialized per session) and declares
// [domain.EgressClassNone] because all in-sandbox network is itself confined by
// the per-session egress broker at the network layer (architecture §8.4), not by
// this tool's classification.
type BashTool struct {
	ws app.SessionWorkspaces
}

// NewBashTool returns a [BashTool] that resolves the calling session's
// workspace through ws.
func NewBashTool(ws app.SessionWorkspaces) *BashTool { return &BashTool{ws: ws} }

// Spec returns the bash tool's declaration.
func (t *BashTool) Spec() domain.ToolSpec {
	return domain.ToolSpec{
		Name:        "bash",
		Description: "Run a shell command inside the workspace sandbox and return its combined output. The command runs through 'sh -c'.",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"required": ["command"],
			"properties": {
				"command": {"type": "string", "description": "The shell command to execute."},
				"workdir": {"type": "string", "description": "Optional working directory inside the sandbox."}
			},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
	}
}

// Execute runs args["command"] via 'sh -c' inside the calling session's
// workspace. The supplied ctx carries cancellation that the workspace maps to a
// process-group/cgroup kill of the in-sandbox tree (architecture §9.3); a
// killed or non-zero exit is returned as an error observation.
func (t *BashTool) Execute(ctx context.Context, sessionID string, args map[string]any) (domain.Observation, error) {
	command, ok := stringArg(args, "command")
	if !ok || command == "" {
		return errObs("bash: required string field %q is missing", "command"), nil
	}
	workdir, ok := optStringArg(args, "workdir", "")
	if !ok {
		return errObs("bash: field %q must be a string", "workdir"), nil
	}
	ws, err := t.ws.Workspace(ctx, sessionID)
	if err != nil {
		return errObs("bash: %v", err), nil
	}
	res, err := ws.Exec(ctx, app.ExecRequest{
		Cmd:     []string{"sh", "-c", command},
		WorkDir: workdir,
	})
	if err != nil {
		return errObs("bash: %v", err), nil
	}
	return execObservation(res), nil
}

// ---------------------------------------------------------------------------
// glob
// ---------------------------------------------------------------------------

// GlobTool finds files matching a glob pattern inside the calling session's
// workspace. It is read-only with no network egress and runs via
// [app.Workspace.Exec].
type GlobTool struct {
	ws app.SessionWorkspaces
}

// NewGlobTool returns a [GlobTool] that resolves the calling session's
// workspace through ws.
func NewGlobTool(ws app.SessionWorkspaces) *GlobTool { return &GlobTool{ws: ws} }

// Spec returns the glob tool's declaration.
func (t *GlobTool) Spec() domain.ToolSpec {
	return domain.ToolSpec{
		Name:        "glob",
		Description: "Find files in the workspace whose path matches a glob pattern (e.g. \"**/*.go\").",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"required": ["pattern"],
			"properties": {
				"pattern": {"type": "string", "description": "The glob pattern to match file paths against."},
				"path": {"type": "string", "description": "Optional directory to search in; defaults to the workspace root."}
			},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
	}
}

// Execute runs a find-based glob inside the workspace and returns the matching
// paths. It is read-only: it uses [app.Workspace.Exec] only to enumerate files.
func (t *GlobTool) Execute(ctx context.Context, sessionID string, args map[string]any) (domain.Observation, error) {
	pattern, ok := stringArg(args, "pattern")
	if !ok || pattern == "" {
		return errObs("glob: required string field %q is missing", "pattern"), nil
	}
	dir, ok := optStringArg(args, "path", ".")
	if !ok {
		return errObs("glob: field %q must be a string", "path"), nil
	}
	if dir == "" {
		dir = "."
	}
	ws, err := t.ws.Workspace(ctx, sessionID)
	if err != nil {
		return errObs("glob: %v", err), nil
	}
	// -path matches against the whole path so "**/*.go"-style patterns work; we
	// translate the conventional "**" to find's "*" semantics for the path test.
	findPattern := strings.ReplaceAll(pattern, "**/", "*/")
	res, err := ws.Exec(ctx, app.ExecRequest{
		Cmd: []string{"find", dir, "-type", "f", "-path", findPattern},
	})
	if err != nil {
		return errObs("glob: %v", err), nil
	}
	return execObservation(res), nil
}

// ---------------------------------------------------------------------------
// grep
// ---------------------------------------------------------------------------

// GrepTool searches file contents in the calling session's workspace for a
// regular expression. It is read-only with no network egress and runs via
// [app.Workspace.Exec].
type GrepTool struct {
	ws app.SessionWorkspaces
}

// NewGrepTool returns a [GrepTool] that resolves the calling session's
// workspace through ws.
func NewGrepTool(ws app.SessionWorkspaces) *GrepTool { return &GrepTool{ws: ws} }

// Spec returns the grep tool's declaration.
func (t *GrepTool) Spec() domain.ToolSpec {
	return domain.ToolSpec{
		Name:        "grep",
		Description: "Search file contents in the workspace for a regular expression and return matching lines with their file and line number.",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"required": ["pattern"],
			"properties": {
				"pattern": {"type": "string", "description": "The regular expression to search for."},
				"path": {"type": "string", "description": "Optional file or directory to search in; defaults to the workspace root."}
			},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
	}
}

// Execute runs a recursive grep inside the workspace and returns matching lines.
// grep exits non-zero when there are no matches; that is reported as a normal
// (non-error) empty observation rather than a tool failure.
func (t *GrepTool) Execute(ctx context.Context, sessionID string, args map[string]any) (domain.Observation, error) {
	pattern, ok := stringArg(args, "pattern")
	if !ok || pattern == "" {
		return errObs("grep: required string field %q is missing", "pattern"), nil
	}
	path, ok := optStringArg(args, "path", ".")
	if !ok {
		return errObs("grep: field %q must be a string", "path"), nil
	}
	if path == "" {
		path = "."
	}
	ws, err := t.ws.Workspace(ctx, sessionID)
	if err != nil {
		return errObs("grep: %v", err), nil
	}
	res, err := ws.Exec(ctx, app.ExecRequest{
		Cmd: []string{"grep", "-rnE", "--", pattern, path},
	})
	if err != nil {
		return errObs("grep: %v", err), nil
	}
	// grep exit code 1 means "no matches" — a successful search with no hits, not
	// an execution error (architecture §3: only genuine failures are errors).
	if !res.Killed && res.ExitCode == 1 && len(res.Stderr) == 0 {
		return domain.Observation{Content: "no matches"}, nil
	}
	return execObservation(res), nil
}
