package pricing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// Overrides is a config-driven pricing overlay: model id → [ModelRates] that
// replace the [DefaultTable] entry for that model wholesale.  Models absent
// from the overlay fall back to the built-in defaults, so a deployment only
// needs to list the models whose placeholder rates are wrong (or missing).
//
// A nil Overrides is valid and behaves exactly like the built-in table.
type Overrides map[string]ModelRates

// ParseOverrides parses a JSON overrides document of the form
//
//	{
//	  "<model id>": {
//	    "input_per_token":       0.000003,
//	    "output_per_token":      0.000015,
//	    "cache_read_per_token":  0.0000003,
//	    "cache_write_per_token": 0.00000375
//	  },
//	  ...
//	}
//
// All prices are USD per single token (published "per 1M" ÷ 1_000_000),
// mirroring [ModelRates].  Omitted rate fields default to zero, which is a
// legitimate price (e.g. a self-hosted model).
//
// Parsing is deliberately strict — this document feeds budget enforcement, and
// a silently misread file is worse than a startup failure:
//
//   - unknown fields inside a rate object are rejected (a typo must not
//     silently zero a rate),
//   - negative rates are rejected,
//   - empty model-id keys are rejected,
//   - trailing content after the document is rejected.
func ParseOverrides(data []byte) (Overrides, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	var ov Overrides
	if err := dec.Decode(&ov); err != nil {
		return nil, fmt.Errorf("pricing: parse overrides: %w", err)
	}
	// A second token means the document was followed by more content — likely a
	// concatenation or editing accident; refuse rather than apply half a file.
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("pricing: parse overrides: trailing data after JSON document")
	}

	for model, rates := range ov {
		if model == "" {
			return nil, fmt.Errorf("pricing: parse overrides: empty model id key")
		}
		for _, f := range []struct {
			name string
			rate float64
		}{
			{"input_per_token", rates.InputPerToken},
			{"output_per_token", rates.OutputPerToken},
			{"cache_read_per_token", rates.CacheReadPerToken},
			{"cache_write_per_token", rates.CacheWritePerToken},
		} {
			if f.rate < 0 {
				return nil, fmt.Errorf("pricing: parse overrides: model %q: negative %s %v", model, f.name, f.rate)
			}
		}
	}
	return ov, nil
}

// Cost returns the estimated USD cost of a single turn described by u for the
// given model, using the override rates when the model is in the overlay and
// falling back to [Cost] (the built-in [DefaultTable]) otherwise.
//
// The method value o.Cost satisfies the CostFunc signature used by the
// daemons' wiring, so an overlaid table is a drop-in replacement for [Cost].
// The no-silent-guess guarantee is preserved: a model in neither the overlay
// nor the defaults yields a [*UnknownModelError].
func (o Overrides) Cost(model string, u llm.Usage) (float64, error) {
	if rates, ok := o[model]; ok {
		return usageCost(rates, u), nil
	}
	return Cost(model, u)
}
