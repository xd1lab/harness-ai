// Package pricing provides a table of per-token costs for known LLM models,
// the [Cost] function that computes a turn's USD cost from a [llm.Usage]
// snapshot, and a config-driven overlay ([Overrides], [ParseOverrides]) for
// correcting or extending the built-in rates per deployment.
//
// # Rate maintenance
//
// Rates in this package are placeholder DEFAULTS based on publicly-listed
// prices at the time of writing (June 2026).  Provider pricing changes
// frequently, so deployments that use cost for billing or budget enforcement
// SHOULD override them: point BOLTROPE_PRICING_FILE at a JSON document in the
// [ParseOverrides] format and the daemons layer it over [DefaultTable] at
// startup (override wins per model; unlisted models keep the defaults).  See
// the doc comment on [ModelRates] for the citation format each built-in entry
// should carry.
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

	"github.com/boltrope/boltrope/internal/platform/llm"
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

// DefaultTable is the package-level pricing table seeded with representative
// models from the Anthropic, OpenAI, and Google Gemini families.
//
// IMPORTANT — PLACEHOLDER RATES: every entry below uses placeholder rates
// derived from publicly-available pricing pages as of approximately June 2026.
// These MUST be reviewed — or overridden per deployment via BOLTROPE_PRICING_FILE
// and [Overrides] — before being used for billing or budget enforcement.  A
// commented citation (source + effective date) accompanies each entry.
//
// To add a model: append an entry whose key is the canonical model id string
// used in [llm.Request.Model] and whose value is a [ModelRates] with rates
// expressed in USD per single token (published "per 1M" ÷ 1_000_000).
var DefaultTable = map[string]ModelRates{
	// -------------------------------------------------------------------------
	// Anthropic — Claude 3 family
	// Source: https://www.anthropic.com/pricing (placeholder, ~June 2026)
	// -------------------------------------------------------------------------

	// claude-3-opus-20240229 — most capable Claude 3 model.
	// Listed rates (per 1M): input $15, output $75, cache-read $1.50, cache-write $18.75.
	"claude-3-opus-20240229": {
		InputPerToken:      15.00 / 1_000_000,
		OutputPerToken:     75.00 / 1_000_000,
		CacheReadPerToken:  1.50 / 1_000_000,
		CacheWritePerToken: 18.75 / 1_000_000,
	},

	// claude-3-5-sonnet-20241022 — Claude 3.5 Sonnet.
	// Listed rates (per 1M): input $3, output $15, cache-read $0.30, cache-write $3.75.
	"claude-3-5-sonnet-20241022": {
		InputPerToken:      3.00 / 1_000_000,
		OutputPerToken:     15.00 / 1_000_000,
		CacheReadPerToken:  0.30 / 1_000_000,
		CacheWritePerToken: 3.75 / 1_000_000,
	},

	// claude-3-haiku-20240307 — Claude 3 Haiku (fast, low-cost).
	// Listed rates (per 1M): input $0.25, output $1.25, cache-read $0.03, cache-write $0.30.
	"claude-3-haiku-20240307": {
		InputPerToken:      0.25 / 1_000_000,
		OutputPerToken:     1.25 / 1_000_000,
		CacheReadPerToken:  0.03 / 1_000_000,
		CacheWritePerToken: 0.30 / 1_000_000,
	},

	// -------------------------------------------------------------------------
	// Anthropic — Claude 3.7 / 4 family (placeholder rates, ~June 2026)
	// Source: https://www.anthropic.com/pricing (placeholder)
	// -------------------------------------------------------------------------

	// claude-3-7-sonnet-20250219 — Claude 3.7 Sonnet (extended thinking).
	// Placeholder rates (per 1M): input $3, output $15, cache-read $0.30, cache-write $3.75.
	"claude-3-7-sonnet-20250219": {
		InputPerToken:      3.00 / 1_000_000,
		OutputPerToken:     15.00 / 1_000_000,
		CacheReadPerToken:  0.30 / 1_000_000,
		CacheWritePerToken: 3.75 / 1_000_000,
	},

	// -------------------------------------------------------------------------
	// OpenAI family
	// Source: https://platform.openai.com/pricing (placeholder, ~June 2026)
	// Cache-read prices reflect OpenAI's automatic prompt caching discount.
	// OpenAI does not currently expose a separate cache-write price; the write
	// rate is set equal to the standard input rate (no premium).
	// -------------------------------------------------------------------------

	// gpt-4o — OpenAI GPT-4o.
	// Placeholder rates (per 1M): input $2.50, output $10, cache-read $1.25, cache-write = input.
	"gpt-4o": {
		InputPerToken:      2.50 / 1_000_000,
		OutputPerToken:     10.00 / 1_000_000,
		CacheReadPerToken:  1.25 / 1_000_000,
		CacheWritePerToken: 2.50 / 1_000_000, // no write premium for OpenAI
	},

	// gpt-4o-mini — OpenAI GPT-4o mini (low-cost).
	// Placeholder rates (per 1M): input $0.15, output $0.60, cache-read $0.075, cache-write = input.
	"gpt-4o-mini": {
		InputPerToken:      0.15 / 1_000_000,
		OutputPerToken:     0.60 / 1_000_000,
		CacheReadPerToken:  0.075 / 1_000_000,
		CacheWritePerToken: 0.15 / 1_000_000, // no write premium for OpenAI
	},

	// o1 — OpenAI o1 (reasoning model).
	// Placeholder rates (per 1M): input $15, output $60, cache-read $7.50, cache-write = input.
	"o1": {
		InputPerToken:      15.00 / 1_000_000,
		OutputPerToken:     60.00 / 1_000_000,
		CacheReadPerToken:  7.50 / 1_000_000,
		CacheWritePerToken: 15.00 / 1_000_000, // no write premium for OpenAI
	},

	// -------------------------------------------------------------------------
	// Google Gemini family
	// Source: https://ai.google.dev/pricing (placeholder, ~June 2026)
	// Gemini does not currently expose explicit cache-write prices; the write
	// rate is set equal to the standard input rate as a conservative placeholder.
	// -------------------------------------------------------------------------

	// gemini-2.0-flash — Gemini 2.0 Flash (fast, low-cost).
	// Placeholder rates (per 1M): input $0.075, output $0.30, cache-read $0.01875, cache-write = input.
	"gemini-2.0-flash": {
		InputPerToken:      0.075 / 1_000_000,
		OutputPerToken:     0.30 / 1_000_000,
		CacheReadPerToken:  0.01875 / 1_000_000,
		CacheWritePerToken: 0.075 / 1_000_000, // no write premium for Gemini
	},

	// gemini-1.5-pro — Gemini 1.5 Pro.
	// Placeholder rates (per 1M, for ≤128k context): input $1.25, output $5, cache-read $0.3125, cache-write = input.
	"gemini-1.5-pro": {
		InputPerToken:      1.25 / 1_000_000,
		OutputPerToken:     5.00 / 1_000_000,
		CacheReadPerToken:  0.3125 / 1_000_000,
		CacheWritePerToken: 1.25 / 1_000_000, // no write premium for Gemini
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
