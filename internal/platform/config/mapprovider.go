package config

import (
	"github.com/knadh/koanf/maps"
	koanf "github.com/knadh/koanf/v2"
)

// mapProviderImpl is a koanf.Provider that serves an in-memory map[string]any of
// configuration values (the built-in defaults and the parsed flag overrides).
//
// # Why this exists instead of koanf's confmap provider
//
// The plan names "defaults via confmap.Provider", but the confmap provider
// (github.com/knadh/koanf/providers/confmap) is NOT among the modules pinned in
// go.sum — only the koanf v2 core, the yaml parser, and the env/file providers
// are. The project rule forbids running `go get`/`go mod tidy` to add a module.
// Rather than block on a 15-line dependency, this type reproduces confmap's exact,
// trivial behavior against the already-vendored koanf/maps package: it takes a flat
// (dot-delimited) or nested map and presents it through the public
// [koanf.Provider] interface. Behavior and the resulting precedence are identical
// to confmap; only the import is local. If the confmap module is later added to
// go.sum, callers can switch to it without any change to load semantics.
type mapProviderImpl struct {
	mp map[string]any
}

// mapProvider returns a [koanf.Provider] over mp. When delim is non-empty the map
// is treated as flat with dot-delimited keys (e.g. "postgres.dsn") and is
// unflattened into the nested shape koanf expects; when delim is empty the map is
// used as-is. The input is deep-copied so the provider never aliases the caller's
// map (mirroring confmap.Provider).
func mapProvider(mp map[string]any, delim string) koanf.Provider {
	cp := maps.Copy(mp)
	maps.IntfaceKeysToStrings(cp)
	if delim != "" {
		cp = maps.Unflatten(cp, delim)
	}
	return &mapProviderImpl{mp: cp}
}

// ReadBytes is unsupported: this provider yields a parsed map directly, so koanf
// calls [mapProviderImpl.Read] instead (Parser is nil at the call site).
func (p *mapProviderImpl) ReadBytes() ([]byte, error) {
	return nil, errUnsupportedReadBytes
}

// Read returns the (already nested) configuration map for koanf to merge.
func (p *mapProviderImpl) Read() (map[string]any, error) {
	return p.mp, nil
}
