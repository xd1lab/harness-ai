package main

import (
	"context"

	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/mcp"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// mcpServerName is the local name under which the single configured stdio MCP
// server is registered (see [newRegistry]).
const mcpServerName = "default"

// mcpSource adapts the MCP client ([app.MCPClientPort]) to the registry's lazy
// [registry.MCPSource] port: it lists the configured server's tools on demand and
// wraps each advertised [domain.ToolSpec] in an [mcpProxyTool] whose Execute
// proxies back to the client's CallTool. Tool schemas and descriptions remain
// untrusted and fail-safe-classified (the client maps them so); the registry
// validates inputs against the (untrusted) schema before any call.
type mcpSource struct {
	client app.MCPClientPort
}

// ref is the configured server reference (stdio transport, no version pin in the
// v1 single-server wiring).
func (m mcpSource) ref() app.MCPServerRef {
	return app.MCPServerRef{Name: mcpServerName, Transport: "stdio"}
}

// Tools lazily lists the MCP server's tools and wraps each as a [domain.Tool].
// It is invoked by the registry on first access only (FR-EXT-01 lazy loading).
func (m mcpSource) Tools(ctx context.Context) ([]domain.Tool, error) {
	specs, err := m.client.ListTools(ctx, m.ref())
	if err != nil {
		return nil, err
	}
	out := make([]domain.Tool, 0, len(specs))
	for _, spec := range specs {
		out = append(out, &mcpProxyTool{client: m.client, server: m.ref(), spec: spec})
	}
	return out, nil
}

// mcpProxyTool is a [domain.Tool] that proxies execution to an MCP server via the
// [app.MCPClientPort]. Its Spec carries the server-advertised (untrusted)
// declaration; Execute forwards the already-validated args to CallTool, whose
// egress (for http servers) is constrained by the session's policy through the
// broker (architecture §8.11).
type mcpProxyTool struct {
	client app.MCPClientPort
	server app.MCPServerRef
	spec   domain.ToolSpec
}

// Spec returns the MCP tool's declaration.
func (t *mcpProxyTool) Spec() domain.ToolSpec { return t.spec }

// Execute proxies the call to the MCP server for sessionID.
func (t *mcpProxyTool) Execute(ctx context.Context, sessionID string, args map[string]any) (domain.Observation, error) {
	return t.client.CallTool(ctx, t.server, sessionID, t.spec.Name, args)
}

// Compile-time assertions.
var (
	_ domain.Tool       = (*mcpProxyTool)(nil)
	_ app.MCPClientPort = (*mcp.Client)(nil)
)
