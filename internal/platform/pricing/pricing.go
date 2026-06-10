// Package pricing provides a table of per-token costs for known LLM models,
// the [Cost] function that computes a turn's USD cost from a [llm.Usage]
// snapshot, and a config-driven overlay ([Overrides], [ParseOverrides]) for
// correcting or extending the built-in rates per deployment.
//
// # Rate maintenance
//
// Rates in this package are seeded from the providers' publicly-listed prices,
// verified against the cited source on the date noted per family (most
// recently 2026-06-11).  Provider pricing changes frequently and the built-in
// table cannot follow it in real time, so deployments that use cost for
// billing or budget enforcement SHOULD pin their own rates: point
// BOLTROPE_PRICING_FILE at a JSON document in the [ParseOverrides] format and
// the daemons layer it over [DefaultTable] at startup (override wins per
// model; unlisted models keep the defaults).  See the doc comment on
// [ModelRates] for the citation format each built-in entry carries.
//
// Known simplifications of the built-in table (override if they matter to your
// deployment): tiered long-context pricing is NOT modeled (entries use the
// base/≤200k-token tier); Gemini's per-hour context-cache STORAGE fee is not a
// per-token rate and is not modeled; providers without a published cache-write
// premium carry CacheWritePerToken = InputPerToken.
//
// # Design
//
// The package is intentionally pure and dependency-light: it imports only this
// module's [llm] package (for the [llm.Usage] type) and the standard library.
// There is no network I/O, no SDK, and no gen/ import.  File reading and env
// plumbing for the overlay live in the daemons' wiring, not here.
//
// # Error handling
//
// An unknown model id never silently returns a zero or best-guess cost: [Cost]
// returns a typed [*UnknownModelError] so callers can distinguish a missing
// table entry from a genuine zero-cost result.
package pricing

import (
	"fmt"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ModelRates holds the per-token USD prices for a single model.
//
// All prices are in USD per single token (i.e. divide the published "per 1M
// tokens" price by 1_000_000).  Each field's doc comment cites the source and
// effective date of the placeholder value so maintenance is straightforward.
//
// The json tags define the rate-object shape of the [ParseOverrides] document;
// parsing is strict (unknown fields rejected), so renaming a tag is a breaking
// config change.
type ModelRates struct {
	// InputPerToken is the price per standard input/prompt token.
	InputPerToken float64 `json:"input_per_token"`
	// OutputPerToken is the price per generated output token.
	OutputPerToken float64 `json:"output_per_token"`
	// CacheReadPerToken is the price per cache-read input token (served from
	// prompt cache; typically lower than InputPerToken).
	CacheReadPerToken float64 `json:"cache_read_per_token"`
	// CacheWritePerToken is the price per cache-write input token (written to
	// prompt cache; typically higher than InputPerToken).
	CacheWritePerToken float64 `json:"cache_write_per_token"`
}

// UnknownModelError is returned by [Cost] when the model id is not present in
// the pricing table.  It is a distinct, typed error so callers can branch on it
// without string matching.
type UnknownModelError struct {
	// Model is the id that was not found.
	Model string
}

// Error implements the error interface.
func (e *UnknownModelError) Error() string {
	return fmt.Sprintf("pricing: unknown model %q — add it to the pricing table or supply a config override", e.Model)
}

// DefaultTable is the package-level pricing table seeded with the CURRENT
// (actively served) models of the Anthropic, OpenAI, and Google Gemini
// families, at the providers' listed rates as of the citation date on each
// family header.  Retired models (e.g. the Claude 3 family, retired by the
// provider in early 2026) are deliberately absent: an entry for a model the
// provider no longer serves is dead weight that masks staleness.
//
// Deployments that bill or budget-enforce against these numbers SHOULD pin
// their own rates via BOLTROPE_PRICING_FILE (see the package doc): provider
// price changes between releases of this module will not be reflected here.
//
// To add a model: append an entry whose key is the canonical model id string
// used in [llm.Request.Model] and whose value is a [ModelRates] with rates
// expressed in USD per single token (published "per 1M" ÷ 1_000_000), and
// carry a citation comment (source + effective date).
var DefaultTable = map[string]ModelRates{
	// -------------------------------------------------------------------------
	// Anthropic — current Claude models
	// Source: https://www.anthropic.com/pricing (verified 2026-06; rates per the
	// vendor's published table). Cache-read = 0.1 × input; cache-write (5-minute
	// TTL) = 1.25 × input — Anthropic's standard prompt-caching multipliers.
	// The 1-hour-TTL cache-write premium (2 × input) is NOT modeled; override if
	// your deployment uses it.
	// -------------------------------------------------------------------------

	// claude-fable-5 — Anthropic's most capable model tier.
	// Listed rates (per 1M): input $10, output $50.
	"claude-fable-5": {
		InputPerToken:      10.00 / 1_000_000,
		OutputPerToken:     50.00 / 1_000_000,
		CacheReadPerToken:  1.00 / 1_000_000,
		CacheWritePerToken: 12.50 / 1_000_000,
	},

	// claude-opus-4-8 — current Opus. Listed rates (per 1M): input $5, output $25.
	"claude-opus-4-8": {
		InputPerToken:      5.00 / 1_000_000,
		OutputPerToken:     25.00 / 1_000_000,
		CacheReadPerToken:  0.50 / 1_000_000,
		CacheWritePerToken: 6.25 / 1_000_000,
	},

	// claude-opus-4-7 — previous-generation Opus, same listed rates as 4.8.
	"claude-opus-4-7": {
		InputPerToken:      5.00 / 1_000_000,
		OutputPerToken:     25.00 / 1_000_000,
		CacheReadPerToken:  0.50 / 1_000_000,
		CacheWritePerToken: 6.25 / 1_000_000,
	},

	// claude-opus-4-6 — older Opus, still served at the same listed rates.
	"claude-opus-4-6": {
		InputPerToken:      5.00 / 1_000_000,
		OutputPerToken:     25.00 / 1_000_000,
		CacheReadPerToken:  0.50 / 1_000_000,
		CacheWritePerToken: 6.25 / 1_000_000,
	},

	// claude-opus-4-5 / claude-opus-4-1 — legacy Opus aliases still served.
	// Opus 4.5 listed (per 1M): input $5, output $25. Opus 4.1: input $15, output $75.
	"claude-opus-4-5": {
		InputPerToken:      5.00 / 1_000_000,
		OutputPerToken:     25.00 / 1_000_000,
		CacheReadPerToken:  0.50 / 1_000_000,
		CacheWritePerToken: 6.25 / 1_000_000,
	},
	"claude-opus-4-1": {
		InputPerToken:      15.00 / 1_000_000,
		OutputPerToken:     75.00 / 1_000_000,
		CacheReadPerToken:  1.50 / 1_000_000,
		CacheWritePerToken: 18.75 / 1_000_000,
	},

	// claude-sonnet-4-6 — current Sonnet. Listed rates (per 1M): input $3, output $15.
	"claude-sonnet-4-6": {
		InputPerToken:      3.00 / 1_000_000,
		OutputPerToken:     15.00 / 1_000_000,
		CacheReadPerToken:  0.30 / 1_000_000,
		CacheWritePerToken: 3.75 / 1_000_000,
	},

	// claude-sonnet-4-5 — legacy Sonnet alias, same listed rates.
	"claude-sonnet-4-5": {
		InputPerToken:      3.00 / 1_000_000,
		OutputPerToken:     15.00 / 1_000_000,
		CacheReadPerToken:  0.30 / 1_000_000,
		CacheWritePerToken: 3.75 / 1_000_000,
	},

	// claude-haiku-4-5 — current Haiku (fast, low-cost); the dated id is the
	// full snapshot name of the same model. Listed (per 1M): input $1, output $5.
	"claude-haiku-4-5": {
		InputPerToken:      1.00 / 1_000_000,
		OutputPerToken:     5.00 / 1_000_000,
		CacheReadPerToken:  0.10 / 1_000_000,
		CacheWritePerToken: 1.25 / 1_000_000,
	},
	"claude-haiku-4-5-20251001": {
		InputPerToken:      1.00 / 1_000_000,
		OutputPerToken:     5.00 / 1_000_000,
		CacheReadPerToken:  0.10 / 1_000_000,
		CacheWritePerToken: 1.25 / 1_000_000,
	},

	// -------------------------------------------------------------------------
	// OpenAI — current GPT-5.x text models
	// Source: https://developers.openai.com/api/docs/pricing (verified
	// 2026-06-11). The page lists input / cached-input / output; OpenAI
	// publishes no cache-write premium, so CacheWritePerToken = InputPerToken.
	// -------------------------------------------------------------------------

	// gpt-5.5 — flagship. Listed (per 1M): input $5, cached $0.50, output $30.
	"gpt-5.5": {
		InputPerToken:      5.00 / 1_000_000,
		OutputPerToken:     30.00 / 1_000_000,
		CacheReadPerToken:  0.50 / 1_000_000,
		CacheWritePerToken: 5.00 / 1_000_000, // no write premium published
	},

	// gpt-5.4 — listed (per 1M): input $2.50, cached $0.25, output $15.
	"gpt-5.4": {
		InputPerToken:      2.50 / 1_000_000,
		OutputPerToken:     15.00 / 1_000_000,
		CacheReadPerToken:  0.25 / 1_000_000,
		CacheWritePerToken: 2.50 / 1_000_000, // no write premium published
	},

	// gpt-5.4-mini — listed (per 1M): input $0.75, cached $0.075, output $4.50.
	"gpt-5.4-mini": {
		InputPerToken:      0.75 / 1_000_000,
		OutputPerToken:     4.50 / 1_000_000,
		CacheReadPerToken:  0.075 / 1_000_000,
		CacheWritePerToken: 0.75 / 1_000_000, // no write premium published
	},

	// gpt-5.4-nano — listed (per 1M): input $0.20, cached $0.02, output $1.25.
	"gpt-5.4-nano": {
		InputPerToken:      0.20 / 1_000_000,
		OutputPerToken:     1.25 / 1_000_000,
		CacheReadPerToken:  0.02 / 1_000_000,
		CacheWritePerToken: 0.20 / 1_000_000, // no write premium published
	},

	// -------------------------------------------------------------------------
	// Google Gemini — current models
	// Source: https://ai.google.dev/gemini-api/docs/pricing (verified
	// 2026-06-11). Where the vendor tiers by prompt length, the ≤200k-token
	// tier is used (the >200k tier is NOT modeled — override if you run long
	// contexts). Cache-read is the listed per-token context-cache price; the
	// separate per-hour cache STORAGE fee is not a per-token rate and is not
	// modeled. Gemini publishes no cache-write premium, so
	// CacheWritePerToken = InputPerToken. Audio-input rates are not modeled
	// (text/image/video rate used).
	// -------------------------------------------------------------------------

	// gemini-3.5-flash — listed (per 1M): input $1.50, output $9, cache-read $0.15.
	"gemini-3.5-flash": {
		InputPerToken:      1.50 / 1_000_000,
		OutputPerToken:     9.00 / 1_000_000,
		CacheReadPerToken:  0.15 / 1_000_000,
		CacheWritePerToken: 1.50 / 1_000_000, // no write premium published
	},

	// gemini-3.1-pro-preview — ≤200k tier (per 1M): input $2, output $12, cache-read $0.20.
	"gemini-3.1-pro-preview": {
		InputPerToken:      2.00 / 1_000_000,
		OutputPerToken:     12.00 / 1_000_000,
		CacheReadPerToken:  0.20 / 1_000_000,
		CacheWritePerToken: 2.00 / 1_000_000, // no write premium published
	},

	// gemini-3.1-flash-lite — listed (per 1M): input $0.25, output $1.50, cache-read $0.025.
	"gemini-3.1-flash-lite": {
		InputPerToken:      0.25 / 1_000_000,
		OutputPerToken:     1.50 / 1_000_000,
		CacheReadPerToken:  0.025 / 1_000_000,
		CacheWritePerToken: 0.25 / 1_000_000, // no write premium published
	},

	// gemini-2.5-pro — ≤200k tier (per 1M): input $1.25, output $10, cache-read $0.125.
	"gemini-2.5-pro": {
		InputPerToken:      1.25 / 1_000_000,
		OutputPerToken:     10.00 / 1_000_000,
		CacheReadPerToken:  0.125 / 1_000_000,
		CacheWritePerToken: 1.25 / 1_000_000, // no write premium published
	},

	// gemini-2.5-flash — listed (per 1M): input $0.30, output $2.50, cache-read $0.03.
	"gemini-2.5-flash": {
		InputPerToken:      0.30 / 1_000_000,
		OutputPerToken:     2.50 / 1_000_000,
		CacheReadPerToken:  0.03 / 1_000_000,
		CacheWritePerToken: 0.30 / 1_000_000, // no write premium published
	},

	// gemini-2.5-flash-lite — listed (per 1M): input $0.10, output $0.40, cache-read $0.01.
	"gemini-2.5-flash-lite": {
		InputPerToken:      0.10 / 1_000_000,
		OutputPerToken:     0.40 / 1_000_000,
		CacheReadPerToken:  0.01 / 1_000_000,
		CacheWritePerToken: 0.10 / 1_000_000, // no write premium published
	},
}

// Cost returns the estimated USD cost of a single turn described by u for the
// given model, using rates from [DefaultTable].
//
// The formula is:
//
//	cost = InputTokens      × rates.InputPerToken
//	     + OutputTokens     × rates.OutputPerToken
//	     + CacheReadTokens  × rates.CacheReadPerToken
//	     + CacheWriteTokens × rates.CacheWritePerToken
//
// ReasoningTokens are NOT billed separately here because by convention they are
// already included in OutputTokens (a subset, not an addend).  If a future
// provider bills reasoning tokens at a distinct rate that behavior should be
// reflected in a new [ModelRates] field and a corresponding update to this
// function.
//
// On success the returned float64 is ≥ 0.  If the model id is not in
// [DefaultTable] the function returns 0 and a [*UnknownModelError] — it never
// silently guesses or returns a zero cost for an unknown model.
func Cost(model string, u llm.Usage) (float64, error) {
	rates, ok := DefaultTable[model]
	if !ok {
		return 0, &UnknownModelError{Model: model}
	}
	return usageCost(rates, u), nil
}

// usageCost applies the [Cost] formula for one resolved set of rates.  It is
// shared by [Cost] and [Overrides.Cost] so the two paths can never drift.
func usageCost(rates ModelRates, u llm.Usage) float64 {
	return float64(u.InputTokens)*rates.InputPerToken +
		float64(u.OutputTokens)*rates.OutputPerToken +
		float64(u.CacheReadTokens)*rates.CacheReadPerToken +
		float64(u.CacheWriteTokens)*rates.CacheWritePerToken
}
