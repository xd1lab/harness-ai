package pricing_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
	"github.com/xd1lab/harness-ai/internal/platform/pricing"
)

// epsilon is the floating-point tolerance shared by the cost assertions below.
const epsilon = 1e-9

// TestParseOverrides_ValidDocument verifies that a well-formed overrides
// document parses into the expected per-model rates, including a model id that
// is absent from the built-in defaults.
func TestParseOverrides_ValidDocument(t *testing.T) {
	t.Parallel()

	doc := []byte(`{
		"claude-3-5-sonnet-20241022": {
			"input_per_token": 0.000004,
			"output_per_token": 0.00002,
			"cache_read_per_token": 0.0000004,
			"cache_write_per_token": 0.000005
		},
		"local-llama-3.3-70b": {
			"input_per_token": 0,
			"output_per_token": 0,
			"cache_read_per_token": 0,
			"cache_write_per_token": 0
		}
	}`)

	ov, err := pricing.ParseOverrides(doc)
	if err != nil {
		t.Fatalf("ParseOverrides: unexpected error: %v", err)
	}
	if len(ov) != 2 {
		t.Fatalf("ParseOverrides: got %d entries, want 2", len(ov))
	}

	got, ok := ov["claude-3-5-sonnet-20241022"]
	if !ok {
		t.Fatal("ParseOverrides: missing entry for claude-3-5-sonnet-20241022")
	}
	want := pricing.ModelRates{
		InputPerToken:      0.000004,
		OutputPerToken:     0.00002,
		CacheReadPerToken:  0.0000004,
		CacheWritePerToken: 0.000005,
	}
	if got != want {
		t.Errorf("ParseOverrides rates = %+v, want %+v", got, want)
	}

	// Zero rates are valid: a self-hosted model genuinely costs $0 per token.
	if free, ok := ov["local-llama-3.3-70b"]; !ok || free != (pricing.ModelRates{}) {
		t.Errorf("ParseOverrides free-model rates = %+v (present=%v), want all-zero", free, ok)
	}
}

// TestParseOverrides_EmptyDocument verifies that an empty JSON object is a
// valid no-op overlay.
func TestParseOverrides_EmptyDocument(t *testing.T) {
	t.Parallel()

	ov, err := pricing.ParseOverrides([]byte(`{}`))
	if err != nil {
		t.Fatalf("ParseOverrides({}): unexpected error: %v", err)
	}
	if len(ov) != 0 {
		t.Errorf("ParseOverrides({}): got %d entries, want 0", len(ov))
	}
}

// TestParseOverrides_UnknownField_Rejected verifies strict parsing: a rate
// object carrying a field that is not part of ModelRates is rejected rather
// than silently dropped (a mistyped field name must not result in a zero
// rate).
func TestParseOverrides_UnknownField_Rejected(t *testing.T) {
	t.Parallel()

	doc := []byte(`{
		"gpt-4o": {
			"input_per_token": 0.0000025,
			"input_per_million": 2.50
		}
	}`)

	if _, err := pricing.ParseOverrides(doc); err == nil {
		t.Fatal("ParseOverrides with unknown field: expected error, got nil")
	}
}

// TestParseOverrides_NegativeRate_Rejected verifies that each rate field
// rejects negative values: a negative per-token price can only be a config
// mistake and would corrupt budget accounting.
func TestParseOverrides_NegativeRate_Rejected(t *testing.T) {
	t.Parallel()

	docs := map[string][]byte{
		"input":       []byte(`{"m": {"input_per_token": -0.000001}}`),
		"output":      []byte(`{"m": {"output_per_token": -0.000001}}`),
		"cache_read":  []byte(`{"m": {"cache_read_per_token": -0.000001}}`),
		"cache_write": []byte(`{"m": {"cache_write_per_token": -0.000001}}`),
	}
	for name, doc := range docs {
		name, doc := name, doc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := pricing.ParseOverrides(doc); err == nil {
				t.Fatalf("ParseOverrides with negative %s rate: expected error, got nil", name)
			}
		})
	}
}

// TestParseOverrides_MalformedJSON_Rejected verifies that syntactically invalid
// JSON is rejected with an error (never an empty/partial overlay).
func TestParseOverrides_MalformedJSON_Rejected(t *testing.T) {
	t.Parallel()

	for _, doc := range []string{``, `not json`, `[1,2,3]`, `{"m": {"input_per_token": "three"}}`} {
		doc := doc
		if _, err := pricing.ParseOverrides([]byte(doc)); err == nil {
			t.Errorf("ParseOverrides(%q): expected error, got nil", doc)
		}
	}
}

// TestParseOverrides_TrailingData_Rejected verifies that content after the
// closing brace is rejected: a concatenated or truncated-then-patched file
// must not be half-applied.
func TestParseOverrides_TrailingData_Rejected(t *testing.T) {
	t.Parallel()

	doc := []byte(`{"gpt-4o": {"input_per_token": 0.0000025}} {"o1": {}}`)
	if _, err := pricing.ParseOverrides(doc); err == nil {
		t.Fatal("ParseOverrides with trailing data: expected error, got nil")
	}
}

// TestParseOverrides_EmptyModelID_Rejected verifies that an empty model-id key
// is rejected: it can never match a request and indicates a malformed file.
func TestParseOverrides_EmptyModelID_Rejected(t *testing.T) {
	t.Parallel()

	doc := []byte(`{"": {"input_per_token": 0.000001}}`)
	if _, err := pricing.ParseOverrides(doc); err == nil {
		t.Fatal("ParseOverrides with empty model id: expected error, got nil")
	}
}

// TestOverridesCost_OverrideWins verifies that an overridden model is priced
// from the override rates, not the built-in defaults — the entry replaces the
// default wholesale.
func TestOverridesCost_OverrideWins(t *testing.T) {
	t.Parallel()

	// Double the default claude-sonnet-4-6 rates (per 1M: input $6, output $30,
	// cache-read $0.60, cache-write $7.50).
	ov, err := pricing.ParseOverrides([]byte(`{
		"claude-sonnet-4-6": {
			"input_per_token": 0.000006,
			"output_per_token": 0.00003,
			"cache_read_per_token": 0.0000006,
			"cache_write_per_token": 0.0000075
		}
	}`))
	if err != nil {
		t.Fatalf("ParseOverrides: unexpected error: %v", err)
	}

	u := llm.Usage{
		InputTokens:      1_000_000, // $6.00
		OutputTokens:     100_000,   // $3.00
		CacheReadTokens:  500_000,   // $0.30
		CacheWriteTokens: 200_000,   // $1.50
	}
	// expected = 6.00 + 3.00 + 0.30 + 1.50 = 10.80 (double the default-table 5.40)
	const wantUSD = 10.80

	got, err := ov.Cost("claude-sonnet-4-6", u)
	if err != nil {
		t.Fatalf("Overrides.Cost: unexpected error: %v", err)
	}
	if diff := got - wantUSD; diff > epsilon || diff < -epsilon {
		t.Errorf("Overrides.Cost = %.10f, want %.10f (diff %.2e)", got, wantUSD, diff)
	}
}

// TestOverridesCost_FallbackMatchesDefault verifies that a model NOT present in
// the overlay is priced exactly as pricing.Cost prices it today.
func TestOverridesCost_FallbackMatchesDefault(t *testing.T) {
	t.Parallel()

	ov, err := pricing.ParseOverrides([]byte(`{"gpt-4o": {"input_per_token": 0.000005}}`))
	if err != nil {
		t.Fatalf("ParseOverrides: unexpected error: %v", err)
	}

	u := llm.Usage{InputTokens: 12_345, OutputTokens: 6_789, CacheReadTokens: 1_000, CacheWriteTokens: 500}
	want, err := pricing.Cost("claude-haiku-4-5", u)
	if err != nil {
		t.Fatalf("pricing.Cost: unexpected error: %v", err)
	}
	got, err := ov.Cost("claude-haiku-4-5", u)
	if err != nil {
		t.Fatalf("Overrides.Cost: unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("Overrides.Cost fallback = %v, want %v (must match pricing.Cost exactly)", got, want)
	}
}

// TestOverridesCost_NewModel verifies that the overlay can introduce a model id
// the built-in table does not know about.
func TestOverridesCost_NewModel(t *testing.T) {
	t.Parallel()

	ov, err := pricing.ParseOverrides([]byte(`{
		"in-house-model-v1": {"input_per_token": 0.000001, "output_per_token": 0.000002}
	}`))
	if err != nil {
		t.Fatalf("ParseOverrides: unexpected error: %v", err)
	}

	got, err := ov.Cost("in-house-model-v1", llm.Usage{InputTokens: 1_000_000, OutputTokens: 1_000_000})
	if err != nil {
		t.Fatalf("Overrides.Cost: unexpected error: %v", err)
	}
	const wantUSD = 3.00 // $1 input + $2 output
	if diff := got - wantUSD; diff > epsilon || diff < -epsilon {
		t.Errorf("Overrides.Cost = %.10f, want %.10f", got, wantUSD)
	}
}

// TestOverridesCost_UnknownModel_TypedError verifies that a model in neither
// the overlay nor the defaults still yields the typed *UnknownModelError —
// the overlay must not weaken the no-silent-guess guarantee.
func TestOverridesCost_UnknownModel_TypedError(t *testing.T) {
	t.Parallel()

	ov, err := pricing.ParseOverrides([]byte(`{}`))
	if err != nil {
		t.Fatalf("ParseOverrides: unexpected error: %v", err)
	}

	_, err = ov.Cost("does-not-exist-v99", llm.Usage{InputTokens: 100})
	if err == nil {
		t.Fatal("Overrides.Cost with unknown model: expected error, got nil")
	}
	var ume *pricing.UnknownModelError
	if !errors.As(err, &ume) {
		t.Fatalf("Overrides.Cost with unknown model: expected *pricing.UnknownModelError, got %T: %v", err, err)
	}
	if ume.Model != "does-not-exist-v99" {
		t.Errorf("UnknownModelError.Model = %q, want %q", ume.Model, "does-not-exist-v99")
	}
}

// TestOverridesCost_NilOverlay verifies that a nil Overrides map behaves
// exactly like the built-in table (defensive: callers may pass the zero value).
func TestOverridesCost_NilOverlay(t *testing.T) {
	t.Parallel()

	var ov pricing.Overrides
	u := llm.Usage{InputTokens: 1_000}
	want, err := pricing.Cost("gpt-5.4-mini", u)
	if err != nil {
		t.Fatalf("pricing.Cost: unexpected error: %v", err)
	}
	got, err := ov.Cost("gpt-5.4-mini", u)
	if err != nil {
		t.Fatalf("Overrides.Cost on nil map: unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("Overrides.Cost on nil map = %v, want %v", got, want)
	}
}

// TestParseOverrides_ErrorMentionsModel verifies that a validation error names
// the offending model id so an operator can fix the file without bisecting it.
func TestParseOverrides_ErrorMentionsModel(t *testing.T) {
	t.Parallel()

	_, err := pricing.ParseOverrides([]byte(`{"bad-model": {"output_per_token": -1}}`))
	if err == nil {
		t.Fatal("ParseOverrides: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "bad-model") {
		t.Errorf("ParseOverrides error %q does not mention the offending model id", err)
	}
}
