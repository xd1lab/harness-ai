package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xd1lab/harness-ai/internal/platform/jsonschema"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// Compile-time assertion that *Registry satisfies the frozen port.
var _ app.ToolRegistry = (*Registry)(nil)

// RegistrationError is returned by [Registry.Register] when a tool's
// [domain.ToolSpec] is invalid — an empty name, an empty description, or a
// missing/malformed JSON Schema (FR-TOOL-02 registration validation). Recover it
// with [errors.As]; the underlying cause (a schema-compile error) is wrapped.
type RegistrationError struct {
	// ToolName is the offending tool's name (may be empty when that is the fault).
	ToolName string
	// Reason is a human-readable description of why registration was rejected.
	Reason string
	// Err is the wrapped underlying cause, when any (e.g. a schema-compile error).
	Err error
}

// Error implements error.
func (e *RegistrationError) Error() string {
	name := e.ToolName
	if name == "" {
		name = "<unnamed>"
	}
	if e.Err != nil {
		return fmt.Sprintf("toolruntime/registry: register tool %q: %s: %v", name, e.Reason, e.Err)
	}
	return fmt.Sprintf("toolruntime/registry: register tool %q: %s", name, e.Reason)
}

// Unwrap returns the wrapped cause for [errors.Is]/[errors.As].
func (e *RegistrationError) Unwrap() error { return e.Err }

// ErrDuplicateTool is returned (wrapped in a [RegistrationError]) when a tool is
// registered under a name that is already taken.
var ErrDuplicateTool = errors.New("toolruntime/registry: duplicate tool name")

// MCPSource lazily supplies the tools discovered from configured MCP servers. The
// registry calls [MCPSource.Tools] at most once (on first access after native
// registration) to merge MCP tools into the catalog. Implementations return tools
// that are already approval-gated and proxied to their MCP server (architecture
// §8.11); the registry treats them like any other [domain.Tool] and wraps them
// with schema validation. A nil MCPSource means "native tools only".
//
// This is a consumer-defined port declared where it is used (the registry), per
// the clean-architecture rule; the MCP client adapter implements it.
type MCPSource interface {
	// Tools returns the currently available MCP-sourced tools, loaded lazily. It
	// is invoked on demand, never at registry construction, so MCP schemas are not
	// fetched eagerly (FR-EXT-01: lazy schema loading). An error surfaces a load
	// failure (e.g. a version-pin mismatch gating the server).
	Tools(ctx context.Context) ([]domain.Tool, error)
}

// compiledTool is a registered tool paired with its compiled input schema.
type compiledTool struct {
	tool   domain.Tool
	schema jsonschema.Compiled
}

// Registry is the merged native+MCP tool catalog implementing [app.ToolRegistry].
// Construct it with [New]. It is safe for concurrent use.
type Registry struct {
	mcp MCPSource

	mu        sync.Mutex
	native    map[string]compiledTool // eagerly registered tools
	mcpTools  map[string]compiledTool // lazily loaded MCP tools
	mcpLoaded bool                    // whether the MCP source has been consulted
}

// New returns an empty [Registry] with the given lazy MCP source. Pass nil for
// mcp to build a native-only registry. Register native tools with
// [Registry.Register]; MCP tools are merged in lazily on first [Registry.Get] or
// [Registry.List].
func New(mcp MCPSource) *Registry {
	return &Registry{
		mcp:      mcp,
		native:   make(map[string]compiledTool),
		mcpTools: make(map[string]compiledTool),
	}
}

// Register validates the tool's spec and adds it to the registry. It rejects an
// empty name, an empty description, or a missing/malformed JSON Schema with a
// [*RegistrationError], and a duplicate name with a [*RegistrationError] wrapping
// [ErrDuplicateTool]. The compiled schema is retained so [Registry.Get] can
// validate inputs before execution.
func (r *Registry) Register(_ context.Context, tool domain.Tool) error {
	spec := tool.Spec()
	ct, err := compile(spec, tool)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.native[spec.Name]; ok {
		return &RegistrationError{ToolName: spec.Name, Reason: "name already registered", Err: ErrDuplicateTool}
	}
	r.native[spec.Name] = ct
	return nil
}

// compile validates a spec and compiles its schema into a compiledTool.
func compile(spec domain.ToolSpec, tool domain.Tool) (compiledTool, error) {
	if spec.Name == "" {
		return compiledTool{}, &RegistrationError{Reason: "name must not be empty"}
	}
	if spec.Description == "" {
		return compiledTool{}, &RegistrationError{ToolName: spec.Name, Reason: "description must not be empty"}
	}
	if len(spec.JSONSchema) == 0 {
		return compiledTool{}, &RegistrationError{ToolName: spec.Name, Reason: "JSON Schema must not be empty"}
	}
	compiled, err := jsonschema.Compile(spec.JSONSchema)
	if err != nil {
		return compiledTool{}, &RegistrationError{ToolName: spec.Name, Reason: "invalid JSON Schema", Err: err}
	}
	return compiledTool{tool: tool, schema: compiled}, nil
}

// Get returns the registered tool by name wrapped in a validating decorator that
// checks inputs against the tool's JSON Schema before execution (FR-TOOL-01). For
// an MCP-sourced tool, the first Get triggers lazy loading of the MCP catalog. It
// returns [app.ErrToolNotFound] when no tool matches.
func (r *Registry) Get(ctx context.Context, name string) (domain.Tool, error) {
	if err := r.ensureMCPLoaded(ctx); err != nil {
		return nil, err
	}
	r.mu.Lock()
	ct, ok := r.native[name]
	if !ok {
		ct, ok = r.mcpTools[name]
	}
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", app.ErrToolNotFound, name)
	}
	return &validatingTool{inner: ct.tool, schema: ct.schema}, nil
}

// List returns the specs of all registered tools — native plus lazily-loaded MCP
// tools — so the service can answer ListTools and build model tool definitions
// (FR-EXT-01 AC-3). The first List triggers lazy MCP loading.
func (r *Registry) List(ctx context.Context) ([]domain.ToolSpec, error) {
	if err := r.ensureMCPLoaded(ctx); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.ToolSpec, 0, len(r.native)+len(r.mcpTools))
	for _, ct := range r.native {
		out = append(out, ct.tool.Spec())
	}
	for _, ct := range r.mcpTools {
		out = append(out, ct.tool.Spec())
	}
	return out, nil
}

// ensureMCPLoaded consults the injected MCP source at most once and merges its
// tools into the catalog. A native tool of the same name takes precedence: an MCP
// tool that collides with a native name is skipped (native tools are trusted; the
// shadowing MCP tool is dropped rather than overriding a first-party tool).
func (r *Registry) ensureMCPLoaded(ctx context.Context) error {
	r.mu.Lock()
	if r.mcpLoaded || r.mcp == nil {
		r.mcpLoaded = true
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	// Call the source outside the lock (it may do I/O).
	mcpTools, err := r.mcp.Tools(ctx)
	if err != nil {
		return fmt.Errorf("toolruntime/registry: load MCP tools: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mcpLoaded { // another goroutine won the race
		return nil
	}
	for _, tool := range mcpTools {
		spec := tool.Spec()
		ct, cerr := compile(spec, tool)
		if cerr != nil {
			// A malformed MCP tool is skipped, not fatal: it must not take down the
			// whole catalog. (The MCP layer is expected to have gated it already.)
			continue
		}
		if _, clash := r.native[spec.Name]; clash {
			continue // never let an MCP tool shadow a native tool
		}
		r.mcpTools[spec.Name] = ct
	}
	r.mcpLoaded = true
	return nil
}

// ---------------------------------------------------------------------------
// validatingTool — the validate-then-execute decorator
// ---------------------------------------------------------------------------

// validatingTool wraps a [domain.Tool] and validates Execute's args against the
// tool's compiled JSON Schema before delegating. A schema violation is returned
// as an error [domain.Observation] and the inner Execute is never called
// (FR-TOOL-01).
type validatingTool struct {
	inner  domain.Tool
	schema jsonschema.Compiled
}

// Spec returns the underlying tool's spec unchanged.
func (v *validatingTool) Spec() domain.ToolSpec { return v.inner.Spec() }

// Execute validates args against the schema and, on success, delegates to the
// wrapped tool. On a validation failure it returns an error Observation with the
// human-readable validation message and does not invoke the wrapped tool.
func (v *validatingTool) Execute(ctx context.Context, sessionID string, args map[string]any) (domain.Observation, error) {
	// jsonschema validates the decoded value; an object schema expects a non-nil
	// map, so normalize a nil args to an empty object.
	if args == nil {
		args = map[string]any{}
	}
	if err := v.schema.Validate(args); err != nil {
		return domain.Observation{
			Content: fmt.Sprintf("invalid arguments for tool %q: %v", v.inner.Spec().Name, err),
			IsError: true,
		}, nil
	}
	return v.inner.Execute(ctx, sessionID, args)
}
