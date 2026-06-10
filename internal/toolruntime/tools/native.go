package tools

import (
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// Native returns the full set of native tools wired to the given session ports:
// the workspace ws backs the filesystem and in-sandbox command tools, and the
// egress broker supplies the deny-by-default allowlist policy consulted by the
// EgressClass=External tools (webfetch, websearch); in v1 the sandbox network
// namespace (`--network none` by default) denies the network path regardless, so
// those tools are effectively disabled unless an egress path is configured
// (architecture §8.4). The returned slice is the Agent-Computer Interface
// presented to the model (FR-TOOL-02): read, write, edit, bash, glob, grep,
// webfetch, websearch.
//
// Callers (the tool-runtime wiring) register these into a
// [github.com/xd1lab/harness-ai/internal/toolruntime/app.ToolRegistry], which
// wraps each with schema validation before execution (FR-TOOL-01).
func Native(ws app.Workspace, broker app.EgressBroker) []domain.Tool {
	return []domain.Tool{
		NewReadTool(ws),
		NewWriteTool(ws),
		NewEditTool(ws),
		NewBashTool(ws),
		NewGlobTool(ws),
		NewGrepTool(ws),
		NewWebFetchTool(ws, broker),
		NewWebSearchTool(ws, broker, ""),
	}
}
