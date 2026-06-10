// Package registry implements [github.com/xd1lab/harness-ai/internal/toolruntime/app.ToolRegistry]:
// the merged, validate-then-execute tool catalog the tool-runtime exposes to the
// orchestrator (architecture §2.3, §5.3, §8.13). It holds the eagerly-registered
// native tools and merges in tools discovered LAZILY from MCP servers, validates
// every registration's [github.com/xd1lab/harness-ai/internal/toolruntime/domain.ToolSpec],
// and — crucially — wraps each tool so its inputs are validated against its JSON
// Schema before [domain.Tool.Execute] is ever invoked (FR-TOOL-01).
//
// # Validate-then-execute
//
// The frozen [domain.Tool] contract says inputs are schema-validated by the
// registry before Execute. This package enforces that by returning, from
// [Registry.Get], a validating decorator around the underlying tool: a schema
// violation (missing required field, additionalProperties:false breach, wrong
// type) is turned into an error [domain.Observation] and the underlying Execute is
// NOT called (FR-TOOL-01 AC-1/AC-2). The decorator never panics on bad input.
//
// # Lazy MCP loading
//
// MCP tools are not loaded at construction. An injected [MCPSource] is consulted
// at most once, on the first [Registry.Get] or [Registry.List] after native
// registration, to discover the (already approval-gated, proxied) MCP tools; their
// untrusted descriptions/schemas and fail-safe Mutating/External defaults are the
// MCP layer's responsibility (architecture §8.11). The source is the lazy seam so
// the registry itself stays decoupled from the MCP client transport.
//
// Implementations of this type are safe for concurrent use.
package registry
