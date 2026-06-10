package agentctx

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// CacheInput describes the content a [CachePrefix] is computed over. Only the
// tenant-agnostic STABLE region (the system prompt and the tool definitions) is
// eligible to be marked cacheable; History is accepted only so a caller may pass
// the full request shape, and is deliberately EXCLUDED from the cached prefix —
// session history is never placed in a shared cache prefix (architecture §8.10).
type CacheInput struct {
	// TenantID is the owning tenant. It is mixed into the cache key so two
	// tenants NEVER share a prefix, eliminating a cross-tenant cache hit or a
	// hit-latency timing oracle (architecture §8.10). It is required for a
	// cacheable prefix.
	TenantID string
	// System is the system prompt — tenant-agnostic stable content eligible for
	// caching.
	System string
	// Tools is the set of tool definitions — tenant-agnostic stable content
	// eligible for caching (the schemas/descriptions the model is shown).
	Tools []llm.ToolDef
	// History is the conversation history. It is NOT cached and is ignored when
	// computing the prefix and key; it is present only so callers can pass an
	// entire request without a separate carve-out. Including it here can never
	// cause private data to enter the cached prefix.
	History []llm.Message
}

// CachePrefix is the result of [BuildCachePrefix]: the stable, tenant-agnostic
// region of a request marked as cacheable, plus a tenant-scoped cache key that
// the model-gateway adapter turns into the provider's prompt-cache control
// (e.g. an Anthropic cache_control breakpoint after the tool defs). It marks
// ONLY the system prompt and tool definitions; HistoryMessagesCached is always
// zero, encoding the invariant that no session-history message is inside the
// cached prefix (architecture §8.10; FR-CTX-03).
type CachePrefix struct {
	// Cacheable reports whether there is any stable content to cache (a
	// non-empty system prompt and/or at least one tool definition) together with
	// a tenant id. When false, CacheKey is empty and the caller marks nothing
	// cacheable.
	Cacheable bool
	// CacheKey is the tenant-scoped, content-addressed key for the stable
	// prefix. It is stable across rebuilds for the same (tenant, system, tools)
	// and differs whenever the tenant OR the stable content differs, so a
	// provider/edge cache keyed on it can never serve one tenant's prefix to
	// another. Empty when Cacheable is false.
	CacheKey string
	// System is the system prompt covered by the cacheable prefix (echoed from
	// the input for the caller's convenience).
	System string
	// Tools are the tool definitions covered by the cacheable prefix (echoed
	// from the input).
	Tools []llm.ToolDef
	// HistoryMessagesCached is the number of leading conversation-history
	// messages included in the cached prefix. It is ALWAYS zero: history is never
	// cached (architecture §8.10). It is surfaced explicitly so the invariant is
	// directly assertable.
	HistoryMessagesCached int
}

// BuildCachePrefix computes the cacheable stable prefix and its tenant-scoped
// key for in (FR-CTX-03 AC-1/AC-2). It marks only the system prompt and tool
// definitions as cacheable and never any session history, and derives the key
// from the tenant id together with the stable content so that:
//
//   - two tenants with identical stable content get DISTINCT keys (no
//     cross-tenant cache hit / timing oracle; AC-2), and
//   - the same tenant with the same stable content gets the SAME key across
//     rebuilds (the provider prompt cache actually hits within a tenant), and
//   - changing the system prompt or any tool definition changes the key.
//
// When there is no stable content (empty system prompt and no tools) the prefix
// is not cacheable: Cacheable is false and CacheKey is empty. It is a pure
// function and performs no I/O.
func BuildCachePrefix(in CacheInput) CachePrefix {
	hasStable := in.System != "" || len(in.Tools) > 0
	if !hasStable || in.TenantID == "" {
		return CachePrefix{
			Cacheable:             false,
			System:                in.System,
			Tools:                 in.Tools,
			HistoryMessagesCached: 0,
		}
	}

	return CachePrefix{
		Cacheable:             true,
		CacheKey:              cacheKey(in.TenantID, in.System, in.Tools),
		System:                in.System,
		Tools:                 in.Tools,
		HistoryMessagesCached: 0, // invariant: history is never cached
	}
}

// cacheKey derives a deterministic, tenant-scoped, content-addressed key over
// the stable region. The tenant id is hashed FIRST (length-prefixed) so it
// cannot collide with stable content under concatenation, guaranteeing two
// tenants never produce the same key even for byte-identical system/tools
// (architecture §8.10). Only tenant-agnostic stable content (system + tool
// name/description/schema) feeds the hash — never history.
func cacheKey(tenantID, system string, tools []llm.ToolDef) string {
	h := sha256.New()
	writeField := func(b []byte) {
		// Length-prefix every field so distinct field boundaries cannot be
		// forged by concatenation (e.g. tenant "ab"+system "c" vs tenant
		// "a"+system "bc").
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(b)))
		_, _ = h.Write(n[:])
		_, _ = h.Write(b)
	}

	writeField([]byte("boltrope/agentctx/cache/v1")) // domain-separation tag
	writeField([]byte(tenantID))
	writeField([]byte(system))

	// Tool order is significant (it is the order presented to the model), so the
	// tools are hashed in slice order without sorting.
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(tools)))
	_, _ = h.Write(n[:])
	for _, t := range tools {
		writeField([]byte(t.Name))
		writeField([]byte(t.Description))
		writeField(t.JSONSchema)
	}

	return hex.EncodeToString(h.Sum(nil))
}
