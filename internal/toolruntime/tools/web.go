package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// maxSearchResults bounds how many search hits websearch renders to the model.
const maxSearchResults = 8

// ---------------------------------------------------------------------------
// webfetch
// ---------------------------------------------------------------------------

// WebFetchTool fetches the contents of a URL. It is external communication
// ([domain.EgressClassExternal]) — a read of an attacker-controlled URL is a
// write to the attacker (ADR-0013) — so the fetch runs through the
// [app.WebFetcher] egress data path, which mediates the host (and every
// redirect hop) against the per-session deny-by-default allowlist and refuses
// non-public destinations (ADR-0021). It is classified Mutating so the
// orchestrator never schedules it in the harmless read-only parallel pool
// (architecture §9.2).
type WebFetchTool struct {
	fetcher app.WebFetcher
}

// NewWebFetchTool returns a [WebFetchTool] backed by the egress data path.
func NewWebFetchTool(fetcher app.WebFetcher) *WebFetchTool {
	return &WebFetchTool{fetcher: fetcher}
}

// Spec returns the webfetch tool's declaration.
func (t *WebFetchTool) Spec() domain.ToolSpec {
	return domain.ToolSpec{
		Name:        "webfetch",
		Description: "Fetch the contents of an http(s) URL. Subject to the per-session network egress allowlist.",
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

// EgressTarget reports the URL's host for the execute service's egress gate.
// It mirrors the fetcher's own parse so the service-level gate adjudicates the
// real destination; the fetcher independently re-gates the actual fetch.
func (t *WebFetchTool) EgressTarget(args map[string]any) (string, bool) {
	raw, ok := stringArg(args, "url")
	if !ok {
		return "", false
	}
	return hostFromURL(raw)
}

// Execute fetches the URL through the egress data path. A denied host,
// unparseable URL, or non-public address comes back from the fetcher as an
// error carrying the canonical "egress denied" wording, which is surfaced to
// the model as an error observation (FR-TOOL-06 AC-1); a completed fetch (even
// non-2xx) is rendered for the model.
func (t *WebFetchTool) Execute(ctx context.Context, sessionID string, args map[string]any) (domain.Observation, error) {
	rawURL, ok := stringArg(args, "url")
	if !ok || rawURL == "" {
		return errObs("webfetch: required string field %q is missing", "url"), nil
	}
	res, err := t.fetcher.Fetch(ctx, sessionID, rawURL)
	if err != nil {
		return errObs("webfetch: %s", egressMessage(err)), nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "HTTP %d %s\n", res.Status, res.FinalURL)
	if res.ContentType != "" {
		fmt.Fprintf(&b, "Content-Type: %s\n", res.ContentType)
	}
	b.WriteString("\n")
	b.Write(res.Body)
	if res.Truncated {
		b.WriteString("\n…[truncated at fetch size cap]")
	}
	return okObs(b.String()), nil
}

// ---------------------------------------------------------------------------
// websearch
// ---------------------------------------------------------------------------

// WebSearchTool performs a web search by querying a configured
// SearXNG-compatible JSON endpoint through the [app.WebFetcher] egress data
// path. Like webfetch it is external communication ([domain.EgressClassExternal])
// — the search backend host must be on the session's egress allowlist — and
// Mutating so it is never parallelized as a read (architecture §8.4, §9.2).
type WebSearchTool struct {
	fetcher   app.WebFetcher
	searchURL string
}

// DefaultSearchHost is the host websearch's default backend URL resolves to; it
// must be present on a session's egress allowlist for websearch to be
// permitted. Production deployments point BOLTROPE_TOOLRT_SEARCH_URL at a real
// SearXNG-compatible JSON endpoint and allowlist its host.
const DefaultSearchHost = "search.boltrope.local"

// defaultSearchURL is the backend websearch queries when none is configured.
const defaultSearchURL = "https://search.boltrope.local/search"

// NewWebSearchTool returns a [WebSearchTool] backed by the egress data path.
// searchURL is the SearXNG-compatible JSON search endpoint (it receives
// ?q=<query>&format=json); empty uses [defaultSearchURL]. The endpoint's host
// must be on the session's egress allowlist.
func NewWebSearchTool(fetcher app.WebFetcher, searchURL string) *WebSearchTool {
	if searchURL == "" {
		searchURL = defaultSearchURL
	}
	return &WebSearchTool{fetcher: fetcher, searchURL: searchURL}
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

// EgressTarget reports the configured search backend's host for the execute
// service's egress gate — websearch's target is NOT in its arguments, so
// without this the gate would fail closed on "no host in args".
func (t *WebSearchTool) EgressTarget(map[string]any) (string, bool) {
	return hostFromURL(t.searchURL)
}

// Execute queries the configured search backend through the egress data path
// and renders the top results. A denied backend host comes back from the
// fetcher with the canonical egress-denied wording.
func (t *WebSearchTool) Execute(ctx context.Context, sessionID string, args map[string]any) (domain.Observation, error) {
	query, ok := stringArg(args, "query")
	if !ok || query == "" {
		return errObs("websearch: required string field %q is missing", "query"), nil
	}
	reqURL, err := buildSearchURL(t.searchURL, query)
	if err != nil {
		return errObs("websearch: %v", err), nil
	}
	res, err := t.fetcher.Fetch(ctx, sessionID, reqURL)
	if err != nil {
		return errObs("websearch: %s", egressMessage(err)), nil
	}
	if res.Status < 200 || res.Status >= 300 {
		return errObs("websearch: search backend returned HTTP %d", res.Status), nil
	}
	rendered, err := renderSearchResults(res.Body)
	if err != nil {
		return errObs("websearch: %v", err), nil
	}
	return okObs(rendered), nil
}

// buildSearchURL appends the SearXNG JSON query parameters to the configured
// endpoint, preserving any path/params it already carries.
func buildSearchURL(endpoint, query string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid search endpoint %q: %w", endpoint, err)
	}
	q := u.Query()
	q.Set("q", query)
	q.Set("format", "json")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// searchResponse is the subset of the SearXNG JSON response websearch renders.
type searchResponse struct {
	Results []struct {
		Title   string `json:"title"`
		URL     string `json:"url"`
		Content string `json:"content"`
	} `json:"results"`
}

// renderSearchResults formats the top results as a compact, model-readable
// list. An empty result set is a non-error "no results" message.
func renderSearchResults(body []byte) (string, error) {
	var parsed searchResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("search backend returned unparseable JSON: %w", err)
	}
	if len(parsed.Results) == 0 {
		return "No results.", nil
	}
	var b strings.Builder
	for i, r := range parsed.Results {
		if i >= maxSearchResults {
			break
		}
		fmt.Fprintf(&b, "%d. %s\n   %s\n", i+1, strings.TrimSpace(r.Title), strings.TrimSpace(r.URL))
		if c := strings.TrimSpace(r.Content); c != "" {
			fmt.Fprintf(&b, "   %s\n", c)
		}
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

// hostFromURL extracts the host (without port) from a raw URL for an egress
// allowlist check. It returns ok=false for a URL it cannot parse to a host so
// the caller fails closed (architecture §8.4).
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

// egressMessage returns the error text without the package-internal
// "egressclient:"/"webfetch:" prefixes doubling up — it keeps the canonical
// "egress denied…" phrase intact for the model-facing observation.
func egressMessage(err error) string {
	msg := err.Error()
	if i := strings.Index(msg, "egress denied"); i >= 0 {
		return msg[i:]
	}
	return msg
}
