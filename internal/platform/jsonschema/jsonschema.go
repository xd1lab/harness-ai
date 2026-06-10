// Package jsonschema provides a thin wrapper over
// github.com/santhosh-tekuri/jsonschema/v6 that compiles a raw JSON Schema
// ([]byte or [encoding/json.RawMessage]) and validates a decoded Go value
// (map[string]any) or a [encoding/json.RawMessage], returning a clear,
// human-readable error on failure.
//
// # Purpose
//
// This package is the shared validator used for tool-input validation
// (FR-TOOL-01). The [Compiled] type produced by [Compile] is the single unit
// of currency: call [Compiled.Validate] with a pre-decoded value or
// [Compiled.ValidateRaw] with raw JSON bytes.
//
// # Concurrency
//
// A [Compiled] schema is safe for concurrent use by multiple goroutines.
//
// # Determinism
//
// This package is pure and stateless: it does not inject or rely on
// [github.com/xd1lab/harness-ai/internal/platform/clock.Clock] or
// [github.com/xd1lab/harness-ai/internal/platform/ids.IDGenerator] because it
// contains no time- or id-dependent logic. It is a platform helper, not a
// domain/app component, so it is outside the forbidigo scope for time.Now.
package jsonschema

import (
	"bytes"
	"encoding/json"
	"fmt"

	extschema "github.com/santhosh-tekuri/jsonschema/v6"
)

// Compiled holds a compiled JSON Schema that is ready for repeated validation.
// The zero value is not valid; use [Compile] to create one.
type Compiled struct {
	schema *extschema.Schema
}

// Compile parses and compiles the given raw JSON Schema bytes.
// It returns a [Compiled] ready for validation or an error if the bytes are
// not valid JSON or are not a valid JSON Schema.
func Compile(raw json.RawMessage) (Compiled, error) {
	// Parse the raw bytes into a generic Go value so we can hand it to the
	// compiler as an in-memory resource (no disk I/O required).
	doc, err := extschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return Compiled{}, fmt.Errorf("jsonschema: parse schema: %w", err)
	}

	const schemaURL = "schema://boltrope/inline"

	c := extschema.NewCompiler()
	if err := c.AddResource(schemaURL, doc); err != nil {
		return Compiled{}, fmt.Errorf("jsonschema: add schema resource: %w", err)
	}

	sch, err := c.Compile(schemaURL)
	if err != nil {
		return Compiled{}, fmt.Errorf("jsonschema: compile schema: %w", err)
	}

	return Compiled{schema: sch}, nil
}

// Validate validates v (typically a map[string]any decoded from JSON) against
// the compiled schema. It returns nil when v is valid, or a descriptive error
// listing every constraint violation.
//
// Validate does not accept a nil value; pass an empty map[string]any instead.
func (c Compiled) Validate(v any) error {
	if err := c.schema.Validate(v); err != nil {
		return fmt.Errorf("jsonschema: validation failed: %w", err)
	}
	return nil
}

// ValidateRaw unmarshals raw and then validates it in one step. It returns an
// error when raw is not valid JSON or when the decoded value fails validation.
//
// ValidateRaw is a convenience wrapper for callers that have not yet decoded
// their input.
func (c Compiled) ValidateRaw(raw json.RawMessage) error {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return fmt.Errorf("jsonschema: unmarshal input: %w", err)
	}
	return c.Validate(v)
}
