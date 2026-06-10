// Package secrettest provides deterministic fakes for the secret package ports:
// [secret.SecretsPort] and [secret.Redactor].
//
// FakeSecrets is a scripted in-memory secrets store. FakeRedactor replaces
// configured sensitive substrings with [secret.Redacted].
package secrettest

import (
	"context"
	"fmt"

	"github.com/xd1lab/harness-ai/internal/platform/secret"
)

// Compile-time assertions.
var (
	_ secret.SecretsPort = (*FakeSecrets)(nil)
	_ secret.Redactor    = (*FakeRedactor)(nil)
)

// FakeSecrets is a simple in-memory [secret.SecretsPort] for tests. Secrets
// are set in the map at construction time or via Put.
type FakeSecrets struct {
	store map[string]secret.Secret
}

// NewFakeSecrets returns a FakeSecrets pre-populated with the given name→value
// mapping. Values are wrapped in [secret.New] so they carry redaction behavior.
func NewFakeSecrets(nameValues map[string]string) *FakeSecrets {
	fs := &FakeSecrets{store: make(map[string]secret.Secret, len(nameValues))}
	for k, v := range nameValues {
		fs.store[k] = secret.New(v)
	}
	return fs
}

// Put adds or replaces a secret by name. The value is wrapped in [secret.New].
func (f *FakeSecrets) Put(name, value string) {
	if f.store == nil {
		f.store = make(map[string]secret.Secret)
	}
	f.store[name] = secret.New(value)
}

// Get returns the Secret for name, or [secret.ErrNotFound] if the name is not
// in the store.
func (f *FakeSecrets) Get(_ context.Context, name string) (secret.Secret, error) {
	if s, ok := f.store[name]; ok {
		return s, nil
	}
	return secret.Secret{}, fmt.Errorf("%w: %q", secret.ErrNotFound, name)
}

// FakeRedactor is a [secret.Redactor] that replaces exact occurrences of any
// registered sensitive string with [secret.Redacted].
type FakeRedactor struct {
	sensitive []string
}

// NewFakeRedactor returns a FakeRedactor that redacts any of the provided
// sensitive strings.
func NewFakeRedactor(sensitive ...string) *FakeRedactor {
	return &FakeRedactor{sensitive: append([]string(nil), sensitive...)}
}

// Redact replaces exact occurrences of every registered sensitive string in s
// with [secret.Redacted]. It returns s unchanged when nothing matches.
func (r *FakeRedactor) Redact(s string) string {
	result := s
	for _, sen := range r.sensitive {
		if sen == "" {
			continue
		}
		result = replaceAll(result, sen, secret.Redacted)
	}
	return result
}

// replaceAll replaces all non-overlapping occurrences of old with newStr in s.
func replaceAll(s, old, newStr string) string {
	if len(old) == 0 || len(s) == 0 {
		return s
	}
	var buf []byte
	start := 0
	oldLen := len(old)
	for i := 0; i <= len(s)-oldLen; {
		if s[i:i+oldLen] == old {
			buf = append(buf, s[start:i]...)
			buf = append(buf, newStr...)
			i += oldLen
			start = i
		} else {
			i++
		}
	}
	if buf == nil {
		return s // no replacements
	}
	buf = append(buf, s[start:]...)
	return string(buf)
}
