// Tests for the jsonschema wrapper.
//
// Test order follows the task spec (T-PLAT-07a):
//  1. Missing required field → error.
//  2. Extra field under additionalProperties:false → error.
//  3. Extra field under a permissive schema (no additionalProperties) → nil.
//  4. Fully valid input → nil.
package jsonschema_test

import (
	"encoding/json"
	"testing"

	"github.com/boltrope/boltrope/internal/platform/jsonschema"
)

// schemaWith returns the raw bytes of a minimal JSON Schema object that
// requires exactly one property "name" (string) and, when strict is true,
// also sets additionalProperties:false.
func schemaWith(strict bool) json.RawMessage {
	if strict {
		return json.RawMessage(`{
			"type": "object",
			"required": ["name"],
			"properties": {
				"name": {"type": "string"}
			},
			"additionalProperties": false
		}`)
	}
	return json.RawMessage(`{
		"type": "object",
		"required": ["name"],
		"properties": {
			"name": {"type": "string"}
		}
	}`)
}

// TestCompileAndValidate covers the four acceptance criteria from T-PLAT-07a.
func TestCompileAndValidate(t *testing.T) {
	t.Parallel()

	strictSchema := schemaWith(true)
	permissiveSchema := schemaWith(false)

	tests := []struct {
		name      string
		schema    json.RawMessage
		input     json.RawMessage
		wantError bool
	}{
		{
			name:      "missing required field returns error",
			schema:    strictSchema,
			input:     json.RawMessage(`{}`),
			wantError: true,
		},
		{
			name:      "extra field under additionalProperties false returns error",
			schema:    strictSchema,
			input:     json.RawMessage(`{"name": "alice", "extra": "boom"}`),
			wantError: true,
		},
		{
			name:      "extra field under permissive schema is ok",
			schema:    permissiveSchema,
			input:     json.RawMessage(`{"name": "alice", "extra": "allowed"}`),
			wantError: false,
		},
		{
			name:      "valid input returns nil",
			schema:    strictSchema,
			input:     json.RawMessage(`{"name": "alice"}`),
			wantError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			compiled, err := jsonschema.Compile(tc.schema)
			if err != nil {
				t.Fatalf("Compile failed unexpectedly: %v", err)
			}

			var decoded map[string]any
			if err := json.Unmarshal(tc.input, &decoded); err != nil {
				t.Fatalf("test setup: unmarshal input: %v", err)
			}

			err = compiled.Validate(decoded)
			if tc.wantError && err == nil {
				t.Errorf("Validate returned nil; want an error")
			}
			if !tc.wantError && err != nil {
				t.Errorf("Validate returned error %v; want nil", err)
			}
		})
	}
}

// TestCompileInvalidSchema verifies that Compile surfaces a non-nil error
// rather than panicking when the raw bytes are not valid JSON.
func TestCompileInvalidSchema(t *testing.T) {
	t.Parallel()

	_, err := jsonschema.Compile(json.RawMessage(`not-json`))
	if err == nil {
		t.Error("Compile with invalid JSON returned nil; want an error")
	}
}

// TestValidateWithRawMessage exercises the RawMessage overload so callers
// need not pre-decode.
func TestValidateWithRawMessage(t *testing.T) {
	t.Parallel()

	compiled, err := jsonschema.Compile(schemaWith(true))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// Valid JSON should succeed.
	if err := compiled.ValidateRaw(json.RawMessage(`{"name":"bob"}`)); err != nil {
		t.Errorf("ValidateRaw returned error for valid input: %v", err)
	}

	// Missing required field should fail.
	if err := compiled.ValidateRaw(json.RawMessage(`{}`)); err == nil {
		t.Error("ValidateRaw returned nil for input missing required field")
	}

	// Malformed JSON should fail.
	if err := compiled.ValidateRaw(json.RawMessage(`{bad`)); err == nil {
		t.Error("ValidateRaw returned nil for malformed JSON")
	}
}

// TestValidationErrorIsReadable asserts that validation errors contain a
// human-readable message (not an empty string), as required for FR-TOOL-01.
func TestValidationErrorIsReadable(t *testing.T) {
	t.Parallel()

	compiled, err := jsonschema.Compile(schemaWith(true))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(`{"extra": 1}`), &decoded); err != nil {
		t.Fatal(err)
	}

	err = compiled.Validate(decoded)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if err.Error() == "" {
		t.Error("validation error message is empty; want a readable description")
	}
}
