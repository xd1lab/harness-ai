package config_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/platform/config"
)

// writeYAML writes a temporary YAML config file and returns its path.
func writeYAML(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// --- precedence -----------------------------------------------------------

// A minimal valid YAML file the precedence tests layer flags/env on top of.
// It sets every required field so Load succeeds, plus a log_level we override.
const validFileBody = `
postgres:
  dsn: "postgres://file-host/db"
  version: 15
server:
  grpc_addr: ":9000"
  http_addr: ":9001"
otlp:
  endpoint: "file-otlp:4317"
log_level: "info"
blob:
  dir: "/var/lib/boltrope/blobs-from-file"
model_gateway:
  endpoint: "file-gw:7000"
`

func TestLoad_Precedence_FlagOverridesEnvOverridesFileOverridesDefault(t *testing.T) {
	file := writeYAML(t, validFileBody)

	t.Run("default wins when nothing else sets the key", func(t *testing.T) {
		// dev_insecure is not set in file/env/flags here -> default (false).
		cfg, err := config.Load(config.Options{
			ConfigFile: file,
			Environ:    nil,
			Args:       nil,
		})
		require.NoError(t, err)
		assert.False(t, cfg.DevInsecure, "default for dev_insecure must be false")
	})

	t.Run("file overrides default", func(t *testing.T) {
		cfg, err := config.Load(config.Options{
			ConfigFile: file,
			Environ:    nil,
			Args:       nil,
		})
		require.NoError(t, err)
		// The default log level differs from the file's "info"... ensure the
		// file value is taken over the built-in default.
		assert.Equal(t, "info", cfg.LogLevel)
		assert.Equal(t, "postgres://file-host/db", cfg.Postgres.DSN)
	})

	t.Run("env overrides file", func(t *testing.T) {
		cfg, err := config.Load(config.Options{
			ConfigFile: file,
			Environ: []string{
				"BOLTROPE_LOG_LEVEL=debug",
				"BOLTROPE_POSTGRES__DSN=postgres://env-host/db",
			},
			Args: nil,
		})
		require.NoError(t, err)
		assert.Equal(t, "debug", cfg.LogLevel, "env must override file log_level")
		assert.Equal(t, "postgres://env-host/db", cfg.Postgres.DSN, "env must override file DSN")
	})

	t.Run("flag overrides env (and file and default)", func(t *testing.T) {
		cfg, err := config.Load(config.Options{
			ConfigFile: file,
			Environ: []string{
				"BOLTROPE_LOG_LEVEL=debug",
			},
			Args: []string{"--log-level=warn"},
		})
		require.NoError(t, err)
		assert.Equal(t, "warn", cfg.LogLevel, "flag must win over env, file, and default")
	})

	t.Run("full ladder on one key: flag>env>file>default", func(t *testing.T) {
		// log_level: default=<built-in>, file=info, env=debug, flag=error.
		// Assert the flag wins; then peel each layer and assert the next wins.
		cfgFlag, err := config.Load(config.Options{
			ConfigFile: file,
			Environ:    []string{"BOLTROPE_LOG_LEVEL=debug"},
			Args:       []string{"--log-level=error"},
		})
		require.NoError(t, err)
		assert.Equal(t, "error", cfgFlag.LogLevel)

		cfgEnv, err := config.Load(config.Options{
			ConfigFile: file,
			Environ:    []string{"BOLTROPE_LOG_LEVEL=debug"},
			Args:       nil,
		})
		require.NoError(t, err)
		assert.Equal(t, "debug", cfgEnv.LogLevel)

		cfgFile, err := config.Load(config.Options{
			ConfigFile: file,
			Environ:    nil,
			Args:       nil,
		})
		require.NoError(t, err)
		assert.Equal(t, "info", cfgFile.LogLevel)
	})
}

func TestLoad_DevInsecure_FlagAndEnv(t *testing.T) {
	file := writeYAML(t, validFileBody)

	// Bool via env.
	cfgEnv, err := config.Load(config.Options{
		ConfigFile: file,
		Environ:    []string{"BOLTROPE_DEV_INSECURE=true"},
	})
	require.NoError(t, err)
	assert.True(t, cfgEnv.DevInsecure)

	// Bool via flag overrides env=false.
	cfgFlag, err := config.Load(config.Options{
		ConfigFile: file,
		Environ:    []string{"BOLTROPE_DEV_INSECURE=false"},
		Args:       []string{"--dev-insecure"},
	})
	require.NoError(t, err)
	assert.True(t, cfgFlag.DevInsecure, "presence of --dev-insecure flag must override env=false")
}

// --- required-field validation (fail-fast, aggregated, human-readable) -----

func TestLoad_MissingRequiredField_ReturnsReadableError(t *testing.T) {
	// File omits the PostgreSQL DSN and the server gRPC address.
	body := `
postgres:
  version: 15
server:
  http_addr: ":9001"
otlp:
  endpoint: "file-otlp:4317"
blob:
  dir: "/blobs"
model_gateway:
  endpoint: "file-gw:7000"
`
	file := writeYAML(t, body)

	cfg, err := config.Load(config.Options{ConfigFile: file})
	require.Error(t, err, "missing required fields must fail fast")
	assert.Nil(t, cfg)

	msg := err.Error()
	// Aggregated: BOTH missing fields are named in one error, not just the first.
	assert.Contains(t, msg, "postgres.dsn", "error must name the missing DSN field")
	assert.Contains(t, msg, "server.grpc_addr", "error must name the missing gRPC addr field")
	// Human-readable: mentions it is a configuration/validation problem.
	assert.True(t,
		strings.Contains(strings.ToLower(msg), "config") ||
			strings.Contains(strings.ToLower(msg), "invalid") ||
			strings.Contains(strings.ToLower(msg), "required"),
		"error should read as a configuration validation failure, got: %q", msg)
}

func TestValidate_AggregatesMultipleErrors(t *testing.T) {
	// Build a Config directly with several invalid/missing fields and assert the
	// returned error names all of them (aggregation), not just the first.
	var c config.Config // zero value: everything missing
	err := c.Validate()
	require.Error(t, err)

	msg := err.Error()
	for _, want := range []string{"postgres.dsn", "server.grpc_addr", "otlp.endpoint", "blob.dir", "model_gateway.endpoint"} {
		assert.Contains(t, msg, want, "aggregated validation error must name %q", want)
	}
}

func TestLoad_MissingConfigFile_DoesNotPanicAndStillValidates(t *testing.T) {
	// With no file and no env/flags, required fields are absent -> readable error,
	// not a panic and not a file-not-found surfaced as the only message.
	cfg, err := config.Load(config.Options{ConfigFile: ""})
	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "postgres.dsn")
}

// --- PostgreSQL minimum version (NFR-PORT-03) ------------------------------

func TestValidate_RejectsPostgresVersionBelow13(t *testing.T) {
	body := `
postgres:
  dsn: "postgres://h/db"
  version: 12
server:
  grpc_addr: ":9000"
  http_addr: ":9001"
otlp:
  endpoint: "o:4317"
log_level: "info"
blob:
  dir: "/blobs"
model_gateway:
  endpoint: "gw:7000"
`
	file := writeYAML(t, body)

	cfg, err := config.Load(config.Options{ConfigFile: file})
	require.Error(t, err, "PostgreSQL major version < 13 must be rejected at validate time (NFR-PORT-03)")
	assert.Nil(t, cfg)
	msg := strings.ToLower(err.Error())
	assert.Contains(t, msg, "postgres", "error must point at the postgres version")
	assert.Contains(t, msg, "13", "error must mention the minimum version 13")
}

func TestValidate_AcceptsPostgresVersion13AndAbove(t *testing.T) {
	for _, v := range []int{13, 14, 15, 16} {
		body := `
postgres:
  dsn: "postgres://h/db"
  version: ` + strconv.Itoa(v) + `
server:
  grpc_addr: ":9000"
  http_addr: ":9001"
otlp:
  endpoint: "o:4317"
log_level: "info"
blob:
  dir: "/blobs"
model_gateway:
  endpoint: "gw:7000"
`
		file := writeYAML(t, body)
		cfg, err := config.Load(config.Options{ConfigFile: file})
		require.NoErrorf(t, err, "PG version %d must be accepted", v)
		require.NotNil(t, cfg)
		assert.Equal(t, v, cfg.Postgres.Version)
	}
}

func TestValidate_RejectsUnknownLogLevel(t *testing.T) {
	body := `
postgres:
  dsn: "postgres://h/db"
  version: 15
server:
  grpc_addr: ":9000"
  http_addr: ":9001"
otlp:
  endpoint: "o:4317"
log_level: "loud"
blob:
  dir: "/blobs"
model_gateway:
  endpoint: "gw:7000"
`
	file := writeYAML(t, body)
	cfg, err := config.Load(config.Options{ConfigFile: file})
	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, strings.ToLower(err.Error()), "log_level")
}

// --- secrets come from env only --------------------------------------------

func TestLoad_FileCannotSetModelGatewayKeysEnvVar(t *testing.T) {
	// The model-gateway key material is referenced by the NAME of an env var
	// (the secret value itself never lives in config). A file may name the env
	// var; the resolved Config exposes the env-var name, not a secret value.
	body := `
postgres:
  dsn: "postgres://h/db"
  version: 15
server:
  grpc_addr: ":9000"
  http_addr: ":9001"
otlp:
  endpoint: "o:4317"
log_level: "info"
blob:
  dir: "/blobs"
model_gateway:
  endpoint: "gw:7000"
  api_key_env: "BOLTROPE_ANTHROPIC_API_KEY"
`
	file := writeYAML(t, body)
	cfg, err := config.Load(config.Options{ConfigFile: file})
	require.NoError(t, err)
	assert.Equal(t, "BOLTROPE_ANTHROPIC_API_KEY", cfg.ModelGateway.APIKeyEnv,
		"config carries the NAME of the env var holding the key, never the key itself")
}
