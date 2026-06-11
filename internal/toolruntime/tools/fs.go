package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// errObs builds an error [domain.Observation] from a format string. It is the
// error-as-observation helper every native tool uses so a recoverable failure is
// reported to the model rather than surfaced as a runtime error (FR-TOOL-01;
// architecture §3).
func errObs(format string, args ...any) domain.Observation {
	return domain.Observation{Content: fmt.Sprintf(format, args...), IsError: true}
}

// okObs is the success-observation helper for tools that build their own
// content string (rather than wrapping an [app.ExecResult] via execObservation).
func okObs(content string) domain.Observation {
	return domain.Observation{Content: content}
}

// stringArg extracts a required string argument by key from the validated args
// map. It reports ok=false when the key is absent or not a string so callers can
// emit an error observation; this is defensive depth behind the schema validation
// the registry already performed (FR-TOOL-01).
func stringArg(args map[string]any, key string) (string, bool) {
	v, ok := args[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// optStringArg extracts an optional string argument, returning def when absent.
// A present value of the wrong type yields ok=false.
func optStringArg(args map[string]any, key, def string) (val string, ok bool) {
	v, present := args[key]
	if !present {
		return def, true
	}
	s, isStr := v.(string)
	if !isStr {
		return "", false
	}
	return s, true
}

// ---------------------------------------------------------------------------
// read
// ---------------------------------------------------------------------------

// ReadTool reads a file from the calling session's workspace. It is read-only
// with no network egress and is eligible for the orchestrator's bounded
// read-only pool (architecture §9.2).
type ReadTool struct {
	ws app.SessionWorkspaces
}

// NewReadTool returns a [ReadTool] that resolves the calling session's
// workspace through ws.
func NewReadTool(ws app.SessionWorkspaces) *ReadTool { return &ReadTool{ws: ws} }

// Spec returns the read tool's declaration.
func (t *ReadTool) Spec() domain.ToolSpec {
	return domain.ToolSpec{
		Name:        "read",
		Description: "Read the contents of a file in the workspace at the given absolute or workspace-relative path.",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"required": ["path"],
			"properties": {
				"path": {"type": "string", "description": "Path of the file to read."}
			},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
	}
}

// Execute reads the file at args["path"] via the calling session's workspace
// and returns its contents. A read failure (e.g. missing file) is reported as
// an error observation, not a Go error.
func (t *ReadTool) Execute(ctx context.Context, sessionID string, args map[string]any) (domain.Observation, error) {
	path, ok := stringArg(args, "path")
	if !ok || path == "" {
		return errObs("read: required string field %q is missing", "path"), nil
	}
	ws, err := t.ws.Workspace(ctx, sessionID)
	if err != nil {
		return errObs("read: %v", err), nil
	}
	data, err := ws.Read(ctx, path)
	if err != nil {
		return errObs("read: %v", err), nil
	}
	return domain.Observation{Content: string(data)}, nil
}

// ---------------------------------------------------------------------------
// write
// ---------------------------------------------------------------------------

// WriteTool creates or overwrites a file in the calling session's workspace.
// It is a mutating tool (serialized per session) with no network egress.
type WriteTool struct {
	ws app.SessionWorkspaces
}

// NewWriteTool returns a [WriteTool] that resolves the calling session's
// workspace through ws.
func NewWriteTool(ws app.SessionWorkspaces) *WriteTool { return &WriteTool{ws: ws} }

// Spec returns the write tool's declaration.
func (t *WriteTool) Spec() domain.ToolSpec {
	return domain.ToolSpec{
		Name:        "write",
		Description: "Write content to a file in the workspace, creating it or overwriting it entirely.",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"required": ["path", "content"],
			"properties": {
				"path": {"type": "string", "description": "Path of the file to write."},
				"content": {"type": "string", "description": "Full file content to write."}
			},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
	}
}

// Execute writes args["content"] to args["path"] via the calling session's
// workspace.
func (t *WriteTool) Execute(ctx context.Context, sessionID string, args map[string]any) (domain.Observation, error) {
	path, ok := stringArg(args, "path")
	if !ok || path == "" {
		return errObs("write: required string field %q is missing", "path"), nil
	}
	content, ok := stringArg(args, "content")
	if !ok {
		return errObs("write: required string field %q is missing", "content"), nil
	}
	ws, err := t.ws.Workspace(ctx, sessionID)
	if err != nil {
		return errObs("write: %v", err), nil
	}
	if err := ws.Write(ctx, path, []byte(content)); err != nil {
		return errObs("write: %v", err), nil
	}
	return domain.Observation{Content: fmt.Sprintf("wrote %d bytes to %s", len(content), path)}, nil
}

// ---------------------------------------------------------------------------
// edit
// ---------------------------------------------------------------------------

// EditTool performs an exact string replacement in a file of the calling
// session's workspace: it reads the file, replaces an occurrence of old_string
// with new_string, and writes it back. It is a mutating tool (serialized per
// session) with no network egress.
type EditTool struct {
	ws app.SessionWorkspaces
}

// NewEditTool returns an [EditTool] that resolves the calling session's
// workspace through ws.
func NewEditTool(ws app.SessionWorkspaces) *EditTool { return &EditTool{ws: ws} }

// Spec returns the edit tool's declaration.
func (t *EditTool) Spec() domain.ToolSpec {
	return domain.ToolSpec{
		Name:        "edit",
		Description: "Replace an exact occurrence of old_string with new_string in a workspace file. By default the old_string must occur exactly once.",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"required": ["path", "old_string", "new_string"],
			"properties": {
				"path": {"type": "string", "description": "Path of the file to edit."},
				"old_string": {"type": "string", "description": "The exact text to replace."},
				"new_string": {"type": "string", "description": "The text to replace it with."},
				"replace_all": {"type": "boolean", "description": "Replace all occurrences instead of requiring a unique match.", "default": false}
			},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
	}
}

// Execute reads args["path"], replaces args["old_string"] with
// args["new_string"] (all occurrences when replace_all is true, otherwise
// requiring a unique match), and writes the result back. Mismatches (file
// missing, old_string absent, or non-unique without replace_all) are reported as
// error observations.
func (t *EditTool) Execute(ctx context.Context, sessionID string, args map[string]any) (domain.Observation, error) {
	path, ok := stringArg(args, "path")
	if !ok || path == "" {
		return errObs("edit: required string field %q is missing", "path"), nil
	}
	oldStr, ok := stringArg(args, "old_string")
	if !ok {
		return errObs("edit: required string field %q is missing", "old_string"), nil
	}
	newStr, ok := stringArg(args, "new_string")
	if !ok {
		return errObs("edit: required string field %q is missing", "new_string"), nil
	}
	replaceAll, _ := args["replace_all"].(bool)

	ws, err := t.ws.Workspace(ctx, sessionID)
	if err != nil {
		return errObs("edit: %v", err), nil
	}
	data, err := ws.Read(ctx, path)
	if err != nil {
		return errObs("edit: %v", err), nil
	}
	content := string(data)
	count := strings.Count(content, oldStr)
	if count == 0 {
		return errObs("edit: old_string not found in %s", path), nil
	}
	if !replaceAll && count > 1 {
		return errObs("edit: old_string is not unique in %s (%d occurrences); set replace_all or provide more context", path, count), nil
	}

	var updated string
	if replaceAll {
		updated = strings.ReplaceAll(content, oldStr, newStr)
	} else {
		updated = strings.Replace(content, oldStr, newStr, 1)
	}
	if err := ws.Write(ctx, path, []byte(updated)); err != nil {
		return errObs("edit: %v", err), nil
	}
	return domain.Observation{Content: fmt.Sprintf("edited %s (%d replacement(s))", path, count)}, nil
}
