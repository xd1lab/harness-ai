package secret

import (
	"regexp"
	"strings"
)

// RegistryRedactor is the concrete [Redactor] used for log/telemetry hygiene. It
// holds a registry of known sensitive literal values (for example resolved
// provider API keys) plus optional regular-expression patterns, and replaces any
// occurrence of them in a string with [Redacted].
//
// IMPORTANT: like every [Redactor], it is defense-in-depth for log hygiene ONLY,
// never a containment boundary (see the package doc and ADR-0013). It masks only
// the exact literals and patterns it has been told about; it cannot catch a value
// the model has transformed (base64/hex), split across calls, or paraphrased. The
// real exfiltration control is egress restriction.
//
// Registration (Register / RegisterSecret / RegisterPattern) is intended to run
// once at wiring time, before the redactor is shared. After that, Redact is safe
// for concurrent use by multiple goroutines; concurrent registration is not.
type RegistryRedactor struct {
	// literals are exact sensitive substrings to mask. They are stored longest
	// first so a value that is a superstring of another is redacted whole before
	// its substring is considered.
	literals []string
	// patterns are compiled regular expressions whose matches are masked.
	patterns []*regexp.Regexp
}

// NewRegistryRedactor returns an empty [RegistryRedactor]. Populate it with
// [RegistryRedactor.Register], [RegistryRedactor.RegisterSecret], and
// [RegistryRedactor.RegisterPattern] during wiring.
func NewRegistryRedactor() *RegistryRedactor { return &RegistryRedactor{} }

// Register adds an exact sensitive literal to the registry. Empty values are
// ignored (an empty literal would otherwise match at every position). Registering
// the same value twice is harmless.
func (r *RegistryRedactor) Register(value string) {
	if value == "" {
		return
	}
	r.literals = insertLongestFirst(r.literals, value)
}

// RegisterSecret adds the plaintext wrapped by s to the registry. It is a
// convenience over [RegistryRedactor.Register] for callers that already hold a
// [Secret] (e.g. one returned by [SecretsPort.Get]); the zero/empty Secret is
// ignored. This is the single place the plaintext is revealed, and it is revealed
// only to teach the redactor what to mask — never logged.
func (r *RegistryRedactor) RegisterSecret(s Secret) {
	r.Register(s.Reveal())
}

// RegisterPattern compiles expr and adds it to the registry so any substring it
// matches is masked. It returns the compile error (and registers nothing) when
// expr is not a valid regular expression. Patterns catch structured secrets whose
// exact value is not known ahead of time (e.g. an "AKIA…" access-key shape).
func (r *RegistryRedactor) RegisterPattern(expr string) error {
	re, err := regexp.Compile(expr)
	if err != nil {
		return err
	}
	r.patterns = append(r.patterns, re)
	return nil
}

// Redact returns s with every registered literal and every pattern match replaced
// by [Redacted]. It returns s unchanged when nothing is recognized. Literals are
// applied before patterns; longer literals are applied first so an overlapping
// shorter literal cannot leave part of a longer secret in place. It is pure and
// safe for concurrent use.
func (r *RegistryRedactor) Redact(s string) string {
	if s == "" {
		return s
	}
	out := s
	for _, lit := range r.literals {
		if lit == "" {
			continue
		}
		out = strings.ReplaceAll(out, lit, Redacted)
	}
	for _, re := range r.patterns {
		out = re.ReplaceAllString(out, Redacted)
	}
	return out
}

// insertLongestFirst inserts value into vals keeping the slice ordered by
// descending length (and skipping exact duplicates), so Redact masks longer
// secrets before their substrings.
func insertLongestFirst(vals []string, value string) []string {
	for _, v := range vals {
		if v == value {
			return vals
		}
	}
	idx := len(vals)
	for i, v := range vals {
		if len(value) > len(v) {
			idx = i
			break
		}
	}
	vals = append(vals, "")
	copy(vals[idx+1:], vals[idx:])
	vals[idx] = value
	return vals
}

// Compile-time assertion that RegistryRedactor satisfies the Redactor port.
var _ Redactor = (*RegistryRedactor)(nil)
