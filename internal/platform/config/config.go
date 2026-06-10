// Package config loads and validates the typed service configuration for every
// Boltrope binary, using knadh/koanf with a fixed precedence of
// flags > env > file > defaults and a fail-fast validation pass (NFR-OPS-04).
//
// # Why this package
//
// Every cmd/ main (orchestratord, modelgwd, toolruntimed, projectord, migrate)
// needs the same cross-cutting knobs — listen addresses, the PostgreSQL DSN, the
// OTLP endpoint, the log level, the BOLTROPE_DEV_INSECURE escape hatch, the blob
// directory, and the model-gateway endpoint plus the NAME of the env var holding
// its API key. Centralizing the load+validate here keeps that logic in one tested
// place and guarantees identical precedence and error reporting across services
// (architecture §5.1 infra/config, §"NFR-OPS-04").
//
// # Precedence (flags > env > file > defaults)
//
// koanf merges sources in load order with a last-writer-wins policy, so [Load]
// loads them lowest-priority-first: built-in [Defaults] via a koanf provider, then
// the optional YAML file, then BOLTROPE_-prefixed environment variables, then the
// parsed command-line flags. Each later layer overrides the same key in the
// earlier ones. Flags are merged from stdlib [flag] (we deliberately do NOT pull in
// spf13/pflag) and only the flags the operator actually set are merged, so an
// unset flag never clobbers an env or file value.
//
// # Fail-fast validation
//
// [Load] always calls [Config.Validate] and returns the aggregated error (and a nil
// *Config) when anything required is missing or invalid, so a misconfigured
// process exits non-zero with one human-readable message naming every offending
// field rather than crashing later at first use (NFR-OPS-04). The PostgreSQL major
// version is rejected below 13 because xid8 / pg_current_xact_id() require it
// (NFR-PORT-03).
//
// # Secrets
//
// Secret material (provider API keys) is NEVER stored in this config or in the
// YAML file. The config carries only the NAME of the environment variable that
// holds a key (for example [ModelGatewayConfig.APIKeyEnv]); the value is resolved
// at the trusted boundary through
// [github.com/boltrope/boltrope/internal/platform/secret.SecretsPort], whose v1
// backend is env-only (ADR-0013; architecture §8.10). Keeping only the env-var
// name here means a leaked config file or a logged Config never exposes a credential.
package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	kenv "github.com/knadh/koanf/providers/env/v2"
	kfile "github.com/knadh/koanf/providers/file"
	koanf "github.com/knadh/koanf/v2"
)

// errUnsupportedReadBytes is returned by the in-memory [mapProvider] from
// ReadBytes, since that provider yields an already-parsed map via Read and is
// always loaded with a nil koanf.Parser. It matches confmap's behavior of
// rejecting the bytes path.
var errUnsupportedReadBytes = errors.New("config: map provider does not support ReadBytes")

const (
	// envPrefix is the required prefix for every Boltrope environment variable
	// (NFR-OPS-04, NFR-SEC-01 BOLTROPE_DEV_INSECURE). Only variables beginning
	// with it are considered, so unrelated host environment never bleeds in.
	envPrefix = "BOLTROPE_"

	// keyDelim is the key-path delimiter koanf uses internally to express the
	// nested config structure (for example "postgres.dsn").
	keyDelim = "."

	// envNestSep is the env-var token that maps to keyDelim. A DOUBLE underscore
	// separates nesting levels (BOLTROPE_POSTGRES__DSN -> postgres.dsn) so that a
	// SINGLE underscore can remain a literal word separator inside a leaf key
	// (BOLTROPE_SERVER__GRPC_ADDR -> server.grpc_addr, BOLTROPE_LOG_LEVEL ->
	// log_level). Without this split, "_" would be ambiguous between nesting and
	// word boundaries.
	envNestSep = "__"

	// minPostgresVersion is the pinned PostgreSQL floor (NFR-PORT-03): xid8 and
	// pg_current_xact_id() — the basis of the event store's transaction id column
	// and the projector's xmin-bounded cursor — require PostgreSQL >= 13.
	minPostgresVersion = 13
)

// validLogLevels is the closed set of accepted slog levels, used by validation to
// reject typos that would otherwise silently fall back to a default at runtime.
var validLogLevels = map[string]struct{}{
	"debug": {},
	"info":  {},
	"warn":  {},
	"error": {},
}

// Config is the fully resolved, validated service configuration shared by all
// Boltrope binaries. Fields are grouped by concern; every field carries a `koanf`
// tag matching its key path so the same struct is populated identically from
// defaults, file, env, and flags. A populated Config is safe to log: it holds no
// secret values, only an env-var NAME for credentials (see the package doc).
type Config struct {
	// Server holds the listen addresses for this service's gRPC and HTTP
	// (health/readiness, and the REST facade on the orchestrator) endpoints.
	Server ServerConfig `koanf:"server"`

	// Postgres holds the event-store connection string and the declared server
	// major version that validation pins to >= 13 (NFR-PORT-03).
	Postgres PostgresConfig `koanf:"postgres"`

	// OTLP holds the OpenTelemetry collector endpoint used for trace/metric
	// export (FR-OBS-01; architecture §"observability").
	OTLP OTLPConfig `koanf:"otlp"`

	// ModelGateway holds the orchestrator's connection to the model-gateway and
	// the NAME of the env var carrying the upstream provider API key (env-only
	// secret resolution; ADR-0013).
	ModelGateway ModelGatewayConfig `koanf:"model_gateway"`

	// Blob holds the filesystem directory for the default blob-store backend
	// (FR-STATE-05; architecture §6.4).
	Blob BlobConfig `koanf:"blob"`

	// LogLevel is the minimum slog level emitted ("debug"|"info"|"warn"|"error").
	// It defaults to "info" and is validated against the closed set above.
	LogLevel string `koanf:"log_level"`

	// DevInsecure is the BOLTROPE_DEV_INSECURE escape hatch. When true it permits
	// the static-cert mTLS fallback to start (NFR-SEC-01); it MUST be false in
	// production images, where the SPIFFE provider is mandatory. Defaults to
	// false so the secure posture is the default.
	DevInsecure bool `koanf:"dev_insecure"`
}

// ServerConfig is the network listen configuration for a service.
type ServerConfig struct {
	// GRPCAddr is the gRPC listen address (host:port or :port). Required.
	GRPCAddr string `koanf:"grpc_addr"`
	// HTTPAddr is the HTTP listen address used for /livez, /readyz, and (on the
	// orchestrator) the REST/SSE facade. Required.
	HTTPAddr string `koanf:"http_addr"`
}

// PostgresConfig is the event-store database configuration.
type PostgresConfig struct {
	// DSN is the PostgreSQL connection string (libpq URL or keyword form).
	// Required. It is treated as sensitive at log sites by callers; this package
	// does not log it.
	DSN string `koanf:"dsn"`
	// Version is the declared PostgreSQL major version. Validation rejects any
	// value below 13 (NFR-PORT-03). Required (must be > 0).
	Version int `koanf:"version"`
}

// OTLPConfig is the OpenTelemetry export configuration.
type OTLPConfig struct {
	// Endpoint is the OTLP collector endpoint (host:port). Required.
	Endpoint string `koanf:"endpoint"`
	// Insecure disables transport security to the collector. It defaults to false
	// and is independent of DevInsecure.
	Insecure bool `koanf:"insecure"`
}

// ModelGatewayConfig is the orchestrator's view of the model-gateway plus the
// credential indirection for provider keys.
type ModelGatewayConfig struct {
	// Endpoint is the model-gateway gRPC address the orchestrator dials.
	// Required.
	Endpoint string `koanf:"endpoint"`
	// APIKeyEnv is the NAME of the environment variable that holds the upstream
	// provider API key. The key VALUE is never stored here; it is resolved via
	// the SecretsPort at the trusted boundary (ADR-0013). Optional in this
	// package's validation because not every binary needs it; the consuming
	// service enforces presence when it must dial a hosted provider.
	APIKeyEnv string `koanf:"api_key_env"`
}

// BlobConfig is the filesystem blob-store configuration.
type BlobConfig struct {
	// Dir is the root directory for the filesystem blob backend. Required. The
	// backend tenant-prefixes paths beneath this root (architecture §6.4, §8.5).
	Dir string `koanf:"dir"`
}

// Defaults returns the built-in configuration defaults — the lowest-priority
// layer in the precedence chain. Only safe, non-secret, non-environment-specific
// values are defaulted (a sensible log level and the secure-by-default posture);
// everything that must be operator-supplied (DSN, addresses, endpoints, blob dir)
// is intentionally left zero so validation forces an explicit value.
func Defaults() map[string]any {
	return map[string]any{
		"log_level":     "info",
		"dev_insecure":  false,
		"otlp.insecure": false,
	}
}

// Options controls a single [Load]. The zero value loads defaults only (then
// fails validation because required fields are unset), which is the documented
// behavior when no file/env/flags are supplied.
type Options struct {
	// ConfigFile is the path to an optional YAML config file. Empty means "no
	// file"; a non-empty path that does not exist is reported as a load error.
	ConfigFile string

	// Environ is the environment slice in "KEY=VALUE" form (as from os.Environ).
	// It is injectable so precedence is unit-testable without mutating the
	// process environment. A nil Environ defaults to [os.Environ].
	Environ []string

	// Args are the command-line arguments to parse for flags (as from
	// os.Args[1:]). It is injectable for the same reason. A nil Args defaults to
	// os.Args[1:]. Only flags actually present in Args override lower layers.
	Args []string
}

// Load resolves the configuration from all four sources in precedence order
// (flags > env > file > defaults), validates it, and returns it. On any load or
// validation problem it returns a nil *Config and a single human-readable error;
// callers (the cmd/ mains) treat a non-nil error as fatal and exit non-zero
// (NFR-OPS-04 fail-fast). It is safe to call concurrently; it mutates no package
// state and parses flags into a private FlagSet rather than the global one.
func Load(opts Options) (*Config, error) {
	k := koanf.New(keyDelim)

	// 1) Defaults (lowest priority).
	if err := k.Load(mapProvider(Defaults(), keyDelim), nil); err != nil {
		return nil, fmt.Errorf("config: loading defaults: %w", err)
	}

	// 2) File (optional). A configured-but-unreadable file is a hard error so a
	// typo in the path fails fast rather than silently running on defaults.
	if opts.ConfigFile != "" {
		if err := k.Load(kfile.Provider(opts.ConfigFile), yaml.Parser()); err != nil {
			return nil, fmt.Errorf("config: reading file %q: %w", opts.ConfigFile, err)
		}
	}

	// 3) Environment (BOLTROPE_-prefixed). Strip the prefix, lowercase, and map
	// the double-underscore nesting token to the key delimiter.
	environ := opts.Environ
	if environ == nil {
		environ = os.Environ()
	}
	envProvider := kenv.Provider(keyDelim, kenv.Opt{
		Prefix:        envPrefix,
		EnvironFunc:   func() []string { return environ },
		TransformFunc: transformEnvKey,
	})
	if err := k.Load(envProvider, nil); err != nil {
		return nil, fmt.Errorf("config: loading environment: %w", err)
	}

	// 4) Flags (highest priority). Only set flags are merged.
	flagMap, err := parseFlags(opts.Args)
	if err != nil {
		return nil, err
	}
	if len(flagMap) > 0 {
		if err := k.Load(mapProvider(flagMap, keyDelim), nil); err != nil {
			return nil, fmt.Errorf("config: loading flags: %w", err)
		}
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("config: decoding into struct: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate checks every required field and cross-field rule, accumulating ALL
// problems into one aggregated, human-readable error (via [errors.Join]) so an
// operator sees every misconfiguration at once instead of fixing them one
// reload at a time. It returns nil when the configuration is complete and valid.
// It is exported so it can be unit-tested directly and re-run by a hot-reload path.
func (c *Config) Validate() error {
	var problems []error

	require := func(value, key string) {
		if strings.TrimSpace(value) == "" {
			problems = append(problems, fmt.Errorf("missing required field %s", key))
		}
	}

	require(c.Postgres.DSN, "postgres.dsn")
	require(c.Server.GRPCAddr, "server.grpc_addr")
	require(c.Server.HTTPAddr, "server.http_addr")
	require(c.OTLP.Endpoint, "otlp.endpoint")
	require(c.Blob.Dir, "blob.dir")
	require(c.ModelGateway.Endpoint, "model_gateway.endpoint")

	// PostgreSQL version: required and pinned to >= 13 (NFR-PORT-03).
	switch {
	case c.Postgres.Version == 0:
		problems = append(problems, errors.New("missing required field postgres.version"))
	case c.Postgres.Version < minPostgresVersion:
		problems = append(problems, fmt.Errorf(
			"postgres.version %d is below the minimum supported PostgreSQL major version %d "+
				"(xid8/pg_current_xact_id require >= %d)",
			c.Postgres.Version, minPostgresVersion, minPostgresVersion))
	}

	// Log level must be one of the closed set so a typo fails fast.
	if lvl := strings.TrimSpace(c.LogLevel); lvl != "" {
		if _, ok := validLogLevels[lvl]; !ok {
			problems = append(problems, fmt.Errorf(
				"invalid log_level %q (must be one of debug, info, warn, error)", c.LogLevel))
		}
	}

	if len(problems) == 0 {
		return nil
	}
	// Prefix the joined detail with a stable, human-readable header so the
	// message reads as a configuration validation failure (NFR-OPS-04).
	return fmt.Errorf("invalid configuration:\n%w", errors.Join(problems...))
}

// transformEnvKey maps a BOLTROPE_-prefixed environment variable name to a koanf
// key path: it strips the prefix, lowercases, and replaces the double-underscore
// nesting token with the key delimiter while leaving single underscores intact as
// literal word separators within a leaf key. The value is returned unchanged (as a
// string); koanf/mapstructure performs weak typing into the target field, so
// "true"/"15" decode correctly into bool/int fields.
func transformEnvKey(key, value string) (string, any) {
	k := strings.TrimPrefix(key, envPrefix)
	k = strings.ToLower(k)
	k = strings.ReplaceAll(k, envNestSep, keyDelim)
	return k, value
}

// flagSpec declares one command-line flag and the koanf key path it maps to.
// Keeping the mapping in a table (rather than scattered flag.Var calls) makes the
// flag surface auditable and keeps [parseFlags] free of per-field branching.
type flagSpec struct {
	name string // flag name as typed on the command line (e.g. "log-level")
	key  string // koanf key path it sets (e.g. "log_level")
	kind flagKind
	help string
}

// flagKind distinguishes how a flag's parsed value is interpreted when mapped
// into the koanf map.
type flagKind int

const (
	flagString flagKind = iota
	flagInt
	flagBool
)

// flagSpecs is the full command-line flag surface. It deliberately covers the
// commonly-overridden operational knobs; everything else is set via file or env.
// Nesting is expressed with the same dotted key paths the struct tags use.
var flagSpecs = []flagSpec{
	{name: "log-level", key: "log_level", kind: flagString, help: "log level: debug|info|warn|error"},
	{name: "dev-insecure", key: "dev_insecure", kind: flagBool, help: "enable the dev-only static-cert mTLS fallback (BOLTROPE_DEV_INSECURE)"},
	{name: "grpc-addr", key: "server.grpc_addr", kind: flagString, help: "gRPC listen address (host:port)"},
	{name: "http-addr", key: "server.http_addr", kind: flagString, help: "HTTP (health/readiness/REST) listen address (host:port)"},
	{name: "postgres-dsn", key: "postgres.dsn", kind: flagString, help: "PostgreSQL DSN"},
	{name: "postgres-version", key: "postgres.version", kind: flagInt, help: "declared PostgreSQL major version (must be >= 13)"},
	{name: "otlp-endpoint", key: "otlp.endpoint", kind: flagString, help: "OTLP collector endpoint (host:port)"},
	{name: "blob-dir", key: "blob.dir", kind: flagString, help: "filesystem blob-store root directory"},
	{name: "model-gateway-endpoint", key: "model_gateway.endpoint", kind: flagString, help: "model-gateway gRPC endpoint (host:port)"},
}

// parseFlags parses args into a private [flag.FlagSet] and returns a map of ONLY
// the flags that were actually set, keyed by their koanf key path. Returning only
// set flags is what makes flags override env/file without an unset flag's zero
// value clobbering a lower layer.
//
// Before parsing, args are filtered to the config flag surface (see
// [filterKnownFlags]), so the loader is a well-behaved citizen: flags it does not
// own — the Go test runner's -test.* flags, or flags a host binary defines on its
// own FlagSet — are ignored rather than turned into a fatal "flag provided but not
// defined" error. A bad VALUE for a known flag (e.g. a non-integer
// postgres-version) is still a load error. Using a private FlagSet (not
// flag.CommandLine) keeps Load free of global state and safe to call repeatedly
// and concurrently.
func parseFlags(args []string) (map[string]any, error) {
	if args == nil {
		args = os.Args[1:]
	}

	fs := flag.NewFlagSet("boltrope", flag.ContinueOnError)
	// Suppress the default usage dump on error; the caller renders our error.
	fs.SetOutput(devNull{})

	strVals := make(map[string]*string)
	intVals := make(map[string]*int)
	boolVals := make(map[string]*bool)
	byName := make(map[string]flagSpec, len(flagSpecs))

	for _, spec := range flagSpecs {
		byName[spec.name] = spec
		switch spec.kind {
		case flagString:
			strVals[spec.name] = fs.String(spec.name, "", spec.help)
		case flagInt:
			intVals[spec.name] = fs.Int(spec.name, 0, spec.help)
		case flagBool:
			boolVals[spec.name] = fs.Bool(spec.name, false, spec.help)
		}
	}

	if err := fs.Parse(filterKnownFlags(args, byName)); err != nil {
		return nil, fmt.Errorf("config: parsing flags: %w", err)
	}

	out := make(map[string]any)
	// flag.Visit visits ONLY flags that were set, which is exactly the override
	// set we want to merge over env/file.
	fs.Visit(func(f *flag.Flag) {
		spec, ok := byName[f.Name]
		if !ok {
			return
		}
		switch spec.kind {
		case flagString:
			out[spec.key] = *strVals[spec.name]
		case flagInt:
			out[spec.key] = *intVals[spec.name]
		case flagBool:
			out[spec.key] = *boolVals[spec.name]
		}
	})
	return out, nil
}

// filterKnownFlags returns the subset of args that correspond to config's own
// flags (those present in byName), preserving order and value association, and
// drops everything else. This lets the loader coexist with foreign flags — the Go
// test runner's -test.* flags or a host binary's own flags — without erroring on
// them, while still surfacing a bad value for one of OUR flags via the subsequent
// parse.
//
// It understands the stdlib flag forms: -name, --name, -name=value, --name=value,
// and the space-separated -name value / --name value form for non-bool flags
// (a bool flag never consumes a following token, matching stdlib semantics). The
// "--" terminator and any positional/foreign tokens after it are dropped, since
// config consumes no positional arguments.
func filterKnownFlags(args []string, byName map[string]flagSpec) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		// "--" terminates flag parsing; nothing after it is a config flag.
		if arg == "--" {
			break
		}
		if len(arg) < 2 || arg[0] != '-' {
			// A positional/foreign token (no leading dash). Skip it.
			continue
		}
		// Normalize the leading dashes to extract "name" / "name=value".
		body := strings.TrimLeft(arg, "-")
		name, _, hasEq := strings.Cut(body, "=")

		spec, known := byName[name]
		if !known {
			// Foreign flag. If it is the "-name value" form we cannot tell its
			// arity, but since it is not ours we simply skip this token; a value
			// token that follows (if any) has no leading dash and is dropped by
			// the positional branch above on the next iteration.
			continue
		}

		out = append(out, arg)
		// A known non-bool flag in the space-separated form consumes the next
		// token as its value; carry it along so Parse sees the pair.
		if !hasEq && spec.kind != flagBool && i+1 < len(args) {
			next := args[i+1]
			if next != "--" {
				out = append(out, next)
				i++
			}
		}
	}
	return out
}

// devNull is an io.Writer that discards everything, used to silence the FlagSet's
// built-in error/usage output so configuration errors are reported solely through
// the returned error.
type devNull struct{}

// Write implements io.Writer by discarding p and reporting full success.
func (devNull) Write(p []byte) (int, error) { return len(p), nil }
