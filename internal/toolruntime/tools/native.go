package tools

import (
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// Native returns the full set of native tools wired to the given ports: the
// per-session workspace resolver ws backs the filesystem and in-sandbox command
// tools — every execution resolves the CALLING session's own sandbox from the
// session id the call carries (per-session isolation; architecture §2.2, §5.3)
// — and the [app.WebFetcher] egress data path backs the EgressClass=External
// tools (webfetch, websearch), performing their fetches at the trust boundary
// mediated per request (and per redirect) by the deny-by-default per-session
// allowlist (ADR-0021). The in-sandbox path stays `--network none`, so the
// sandbox's own `bash` still has no network; only these two tool clients reach
// allowlisted hosts. searchURL is the SearXNG-compatible JSON search endpoint
// websearch queries (empty uses the built-in default host). The returned slice
// is the Agent-Computer Interface presented to the model (FR-TOOL-02): read,
// write, edit, bash, glob, grep, webfetch, websearch.
//
// Callers (the tool-runtime wiring) register these into a
// [github.com/xd1lab/harness-ai/internal/toolruntime/app.ToolRegistry], which
// wraps each with schema validation before execution (FR-TOOL-01).
func Native(ws app.SessionWorkspaces, fetcher app.WebFetcher, searchURL string) []domain.Tool {
	return []domain.Tool{
		NewReadTool(ws),
		NewWriteTool(ws),
		NewEditTool(ws),
		NewBashTool(ws),
		NewGlobTool(ws),
		NewGrepTool(ws),
		NewWebFetchTool(fetcher),
		NewWebSearchTool(fetcher, searchURL),
	}
}
