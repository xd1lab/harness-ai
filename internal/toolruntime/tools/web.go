package tools

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// hostFromURL extracts the host (without port) from a raw URL for an egress
// allowlist check. It returns ok=false for a URL it cannot parse to a host, so
// the caller fails closed (denies) on ambiguity (architecture §8.4).
func hostFromURL(raw string) (host string, ok bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", false
	}
	h := u.Hostname()
	if h == "" {
		return "", false
	}
	return h, true
}

// ---------------------------------------------------------------------------
// webfetch
// ---------------------------------------------------------------------------

// WebFetchTool fetches the contents of a URL. It is external communication
// ([domain.EgressClassExternal]) — a read of an attacker-controlled URL is a
// write to the attacker (ADR-0013) — so it is gated by the per-session
// [app.EgressBroker] (deny-by-default) and the fetch itself runs inside the
// session workspace so it is confined to the sandbox network namespace
// (architecture §8.4). It is classified Mutating so the orchestrator never
// schedules it in the harmless read-only parallel pool (architecture §9.2).
type WebFetchTool struct {
	ws     app.SessionWorkspaces
	broker app.EgressBroker
}

// NewWebFetchTool returns a [WebFetchTool] backed by the per-session workspace
// resolver ws (for the confined fetch) and the egress broker (for the
// deny-by-default host check).
func NewWebFetchTool(ws app.SessionWorkspaces, broker app.EgressBroker) *WebFetchTool {
	return &WebFetchTool{ws: ws, broker: broker}
}

// Spec returns the webfetch tool's declaration.
func (t *WebFetchTool) Spec() domain.ToolSpec {
	return domain.ToolSpec{
		Name:        "webfetch",
		Description: "Fetch the contents of a URL over HTTP(S). Subject to the per-session network egress allowlist.",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"required": ["url"],
			"properties": {
				"url": {"type": "string", "description": "The absolute http(s) URL to fetch."}
			},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassExternal,
	}
}

// Execute resolves the URL's host, checks it against the session's egress
// allowlist via the broker (fail-closed on a denied or unparseable host), and on
// allow performs the fetch inside the workspace sandbox. A denied host returns an
// error observation with an egress-denied message (FR-TOOL-06 AC-1).
func (t *WebFetchTool) Execute(ctx context.Context, sessionID string, args map[string]any) (domain.Observation, error) {
	rawURL, ok := stringArg(args, "url")
	if !ok || rawURL == "" {
		return errObs("webfetch: required string field %q is missing", "url"), nil
	}
	host, ok := hostFromURL(rawURL)
	if !ok {
		// Fail closed: a URL we cannot resolve to a host is denied.
		return errObs("webfetch: egress denied: cannot determine host from URL %q", rawURL), nil
	}
	allowed, err := t.broker.Allow(ctx, sessionID, host)
	if err != nil {
		// Broker error → fail closed (deny) per the egress contract.
		return errObs("webfetch: egress denied for host %q: %v", host, err), nil
	}
	if !allowed {
		return errObs("webfetch: egress denied: host %q is not on the session allowlist", host), nil
	}
	ws, err := t.ws.Workspace(ctx, sessionID)
	if err != nil {
		return errObs("webfetch: %v", err), nil
	}
	res, err := ws.Exec(ctx, app.ExecRequest{
		Cmd: []string{"curl", "-sSL", "--", rawURL},
	})
	if err != nil {
		return errObs("webfetch: %v", err), nil
	}
	return execObservation(res), nil
}

// ---------------------------------------------------------------------------
// websearch
// ---------------------------------------------------------------------------

// WebSearchTool performs a web search for a query. Like webfetch it is external
// communication ([domain.EgressClassExternal]) routed through the per-session
// [app.EgressBroker] and confined to the workspace sandbox, and is classified
// Mutating so it is never parallelized as a read (architecture §8.4, §9.2).
type WebSearchTool struct {
	ws         app.SessionWorkspaces
	broker     app.EgressBroker
	searchHost string
}

// DefaultSearchHost is the host the websearch tool routes queries through; it
// must be present on a session's egress allowlist for websearch to be permitted.
const DefaultSearchHost = "search.boltrope.local"

// NewWebSearchTool returns a [WebSearchTool] backed by the per-session
// workspace resolver ws and the egress broker. searchHost is the host the
// search backend is reached at (used for the egress allowlist check); empty
// uses [DefaultSearchHost].
func NewWebSearchTool(ws app.SessionWorkspaces, broker app.EgressBroker, searchHost string) *WebSearchTool {
	if searchHost == "" {
		searchHost = DefaultSearchHost
	}
	return &WebSearchTool{ws: ws, broker: broker, searchHost: searchHost}
}

// Spec returns the websearch tool's declaration.
func (t *WebSearchTool) Spec() domain.ToolSpec {
	return domain.ToolSpec{
		Name:        "websearch",
		Description: "Search the web for a query and return result snippets. Subject to the per-session network egress allowlist.",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"required": ["query"],
			"properties": {
				"query": {"type": "string", "description": "The search query."}
			},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassExternal,
	}
}

// Execute checks the search backend host against the session's egress allowlist
// (fail-closed) and, on allow, runs the query inside the workspace sandbox. A
// denied host returns an egress-denied error observation.
func (t *WebSearchTool) Execute(ctx context.Context, sessionID string, args map[string]any) (domain.Observation, error) {
	query, ok := stringArg(args, "query")
	if !ok || query == "" {
		return errObs("websearch: required string field %q is missing", "query"), nil
	}
	allowed, err := t.broker.Allow(ctx, sessionID, t.searchHost)
	if err != nil {
		return errObs("websearch: egress denied for host %q: %v", t.searchHost, err), nil
	}
	if !allowed {
		return errObs("websearch: egress denied: search host %q is not on the session allowlist", t.searchHost), nil
	}
	ws, err := t.ws.Workspace(ctx, sessionID)
	if err != nil {
		return errObs("websearch: %v", err), nil
	}
	res, err := ws.Exec(ctx, app.ExecRequest{
		Cmd: []string{"boltrope-websearch", "--", query},
	})
	if err != nil {
		return errObs("websearch: %v", err), nil
	}
	return execObservation(res), nil
}
