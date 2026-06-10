package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// protocolVersion is the MCP protocol version this client advertises in the
// initialize handshake. It is the date-stamped revision string MCP uses.
const protocolVersion = "2025-06-18"

// transportStdio is the [app.MCPServerRef.Transport] value this adapter
// implements. http-transport servers are handled by a separate adapter; an
// http ref reaching this client is rejected.
const transportStdio = "stdio"

// ErrVersionPinMismatch is returned when a server's reported identity/version
// does not match the pin configured on [app.MCPServerRef.VersionPin]. The
// server is gated until re-approved (ADR-0013 §"MCP server confinement";
// architecture §8.11). Recover it with [errors.Is].
var ErrVersionPinMismatch = errors.New("mcp: server version-pin mismatch")

// ErrUnsupportedTransport is returned when a [app.MCPServerRef] names a
// transport this stdio adapter does not implement (e.g. "http").
var ErrUnsupportedTransport = errors.New("mcp: unsupported transport")

// Compile-time assertion that Client satisfies the consumer-defined port.
var _ app.MCPClientPort = (*Client)(nil)

// transport is the byte-stream abstraction the JSON-RPC [conn] runs over: the
// spawned MCP server's stdout (Read) and stdin (Write), closed together. The
// default implementation wraps an os/exec subprocess; tests inject an
// in-process pipe pair.
type transport interface {
	io.Reader
	io.Writer
	io.Closer
}

// ServerRefSpawn is the resolved description handed to a [spawnFunc]: the
// configured command/args for a named server plus the SCRUBBED environment the
// subprocess is allowed to see. It is distinct from [app.MCPServerRef] (the
// caller-facing identity/version handle) so the spawn layer gets exactly the
// confinement-relevant fields and nothing else.
type ServerRefSpawn struct {
	// Name is the local server name (for diagnostics).
	Name string
	// Command is the executable to run for a stdio server.
	Command string
	// Args are the command arguments.
	Args []string
	// Env is the COMPLETE environment for the subprocess (KEY=VALUE strings).
	// It is intentionally not derived from os.Environ(): the subprocess
	// inherits nothing unless explicitly configured, so service credentials and
	// the SPIRE Workload API socket/SVID never leak into the server's namespace
	// (ADR-0013 §"MCP server confinement").
	Env []string
}

// spawnFunc launches an MCP server and returns a [transport] bound to its
// stdio. The default ([execSpawn]) uses os/exec; tests override it.
type spawnFunc func(ctx context.Context, ref ServerRefSpawn) (transport, error)

// serverConfig is the per-server launch configuration (command/args/env)
// resolved from operator configuration and keyed by server name.
type serverConfig struct {
	// Command is the executable to spawn for a stdio server.
	Command string
	// Args are the command arguments.
	Args []string
	// Env is the explicit, scrubbed environment for the subprocess. Empty means
	// the subprocess inherits NOTHING (the safe default).
	Env []string
}

// Client is the minimal MCP client implementing [app.MCPClientPort] over a
// stdio transport. It spawns a confined subprocess per call, performs the
// initialize handshake, and issues a single tools/list or tools/call. Tool
// names/descriptions/schemas from the server are treated as untrusted data and
// surfaced verbatim for approval; discovered tools default to the fail-safe
// classifications. It is safe for concurrent use.
type Client struct {
	servers map[string]serverConfig
	spawn   spawnFunc
}

// Option configures a [Client].
type Option func(*Client)

// WithServer registers a stdio MCP server's launch configuration under name.
// env is the COMPLETE, scrubbed environment for the subprocess; pass nil for a
// fully clean environment (the subprocess inherits nothing).
func WithServer(name, command string, args, env []string) Option {
	return func(c *Client) {
		c.servers[name] = serverConfig{Command: command, Args: args, Env: env}
	}
}

// New constructs a [Client]. The default spawn launches the configured command
// as an os/exec subprocess with a scrubbed environment; tests override the
// spawn to run an in-process fake server.
func New(opts ...Option) *Client {
	c := &Client{
		servers: make(map[string]serverConfig),
		spawn:   execSpawn,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// ListTools performs the initialize handshake then a single tools/list, and
// maps each advertised tool to a [domain.ToolSpec] with the fail-safe
// classifications. It is the LAZY load path: it contacts the server only when
// invoked. A configured [app.MCPServerRef.VersionPin] is enforced against the
// server's reported identity before any tool is returned.
func (c *Client) ListTools(ctx context.Context, server app.MCPServerRef) ([]domain.ToolSpec, error) {
	cn, err := c.dial(ctx, server)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cn.close() }()

	if err := c.handshake(ctx, cn, server); err != nil {
		return nil, err
	}

	raw, err := cn.call(ctx, "tools/list", map[string]any{})
	if err != nil {
		return nil, fmt.Errorf("mcp: tools/list on %q: %w", server.Name, err)
	}
	var res toolsListResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("mcp: decode tools/list result from %q: %w", server.Name, err)
	}

	specs := make([]domain.ToolSpec, 0, len(res.Tools))
	for _, t := range res.Tools {
		specs = append(specs, specFromWire(t))
	}
	return specs, nil
}

// CallTool performs the initialize handshake then a single tools/call, and maps
// the result's content blocks to a [domain.Observation]. A tool that ran but
// reported a failure (isError:true) is returned as an Observation with
// IsError set and a nil Go error; transport/protocol failures return a Go
// error. The session's egress (for any server-initiated network) is constrained
// by the broker elsewhere; this client implements the stdio transport.
func (c *Client) CallTool(ctx context.Context, server app.MCPServerRef, _ /*sessionID*/, name string, args map[string]any) (domain.Observation, error) {
	cn, err := c.dial(ctx, server)
	if err != nil {
		return domain.Observation{}, err
	}
	defer func() { _ = cn.close() }()

	if err := c.handshake(ctx, cn, server); err != nil {
		return domain.Observation{}, err
	}

	if args == nil {
		args = map[string]any{}
	}
	raw, err := cn.call(ctx, "tools/call", callParams{Name: name, Arguments: args})
	if err != nil {
		return domain.Observation{}, fmt.Errorf("mcp: tools/call %q on %q: %w", name, server.Name, err)
	}
	var res callResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return domain.Observation{}, fmt.Errorf("mcp: decode tools/call result for %q: %w", name, err)
	}
	return observationFromResult(res), nil
}

// dial resolves the server's launch configuration and spawns a stdio session.
func (c *Client) dial(ctx context.Context, server app.MCPServerRef) (*conn, error) {
	if server.Transport != "" && server.Transport != transportStdio {
		return nil, fmt.Errorf("%w: %q (this adapter implements stdio)", ErrUnsupportedTransport, server.Transport)
	}
	cfg, ok := c.servers[server.Name]
	if !ok {
		// Allow a default empty config so tests (which inject the spawn) and
		// callers that wire the command out-of-band still function; the spawn
		// is the authority on what actually launches.
		cfg = serverConfig{}
	}
	t, err := c.spawn(ctx, ServerRefSpawn{
		Name:    server.Name,
		Command: cfg.Command,
		Args:    cfg.Args,
		Env:     cfg.Env, // scrubbed: nil/empty ⇒ no inherited environment
	})
	if err != nil {
		return nil, fmt.Errorf("mcp: spawn server %q: %w", server.Name, err)
	}
	return newConn(t), nil
}

// handshake performs initialize, enforces the version pin against the reported
// serverInfo, and sends the notifications/initialized notification.
func (c *Client) handshake(ctx context.Context, cn *conn, server app.MCPServerRef) error {
	raw, err := cn.call(ctx, "initialize", initializeParams{
		ProtocolVersion: protocolVersion,
		Capabilities:    map[string]any{},
		ClientInfo:      clientInfo{Name: "boltrope-toolruntime", Version: "v1"},
	})
	if err != nil {
		return fmt.Errorf("mcp: initialize %q: %w", server.Name, err)
	}
	var res initializeResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return fmt.Errorf("mcp: decode initialize result from %q: %w", server.Name, err)
	}

	// Pin server identity/version (untrusted server; ADR-0013). A configured
	// pin that does not match the reported serverInfo gates the server.
	if server.VersionPin != "" {
		got := PinFor(res.ServerInfo.Name, res.ServerInfo.Version)
		if got != server.VersionPin {
			return fmt.Errorf("%w: server %q reported %s/%s (pin %s), expected pin %s",
				ErrVersionPinMismatch, server.Name, res.ServerInfo.Name, res.ServerInfo.Version, got, server.VersionPin)
		}
	}

	// MCP requires the client to confirm initialization with a notification.
	if err := cn.notify("notifications/initialized", map[string]any{}); err != nil {
		return fmt.Errorf("mcp: send initialized notification to %q: %w", server.Name, err)
	}
	return nil
}

// PinFor computes the canonical version-pin string for a server's reported
// identity (serverInfo name + version). Operators record this value in
// [app.MCPServerRef.VersionPin]; a server whose reported identity hashes to a
// different value is gated by [ErrVersionPinMismatch].
func PinFor(name, version string) string {
	sum := sha256.Sum256([]byte(name + "\x00" + version))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// ---------------------------------------------------------------------------
// wire shapes (MCP JSON-RPC payloads)
// ---------------------------------------------------------------------------

type initializeParams struct {
	ProtocolVersion string         `json:"protocolVersion"`
	Capabilities    map[string]any `json:"capabilities"`
	ClientInfo      clientInfo     `json:"clientInfo"`
}

type clientInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type initializeResult struct {
	ProtocolVersion string     `json:"protocolVersion"`
	ServerInfo      clientInfo `json:"serverInfo"`
}

type toolsListResult struct {
	Tools []wireTool `json:"tools"`
}

// wireTool is one entry of a tools/list result. Name/Description/InputSchema
// are UNTRUSTED data from the server and are carried verbatim onto the spec.
type wireTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

type callParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

// callResult is a tools/call result. content is a list of typed blocks; this
// client surfaces text blocks. isError marks a tool-level failure.
type callResult struct {
	Content []contentBlock `json:"content"`
	IsError bool           `json:"isError"`
}

type contentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// ---------------------------------------------------------------------------
// mapping to domain types
// ---------------------------------------------------------------------------

// specFromWire maps an untrusted wire tool to a [domain.ToolSpec] with the
// fail-safe classifications. The name/description/schema are carried verbatim:
// they are DATA the registry's approval-on-first-use gate reviews, never
// instructions this package acts on.
func specFromWire(t wireTool) domain.ToolSpec {
	schema := t.InputSchema
	if len(schema) == 0 {
		// A missing schema is treated as the permissive empty object schema;
		// the registry still validates inputs against it before execution.
		schema = json.RawMessage(`{"type":"object"}`)
	}
	return domain.ToolSpec{
		Name:        t.Name,
		Description: t.Description,
		JSONSchema:  cloneRaw(schema),
		// Fail-safe: an unannotated MCP tool is maximally gated (architecture
		// §8.11; ADR-0013). MCP does not carry these classifications, so they
		// are never derived from the untrusted server.
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassExternal,
	}
}

// observationFromResult maps a tools/call result to a [domain.Observation],
// concatenating text content blocks. A non-text block contributes nothing to
// the textual content (it is not interpreted here).
func observationFromResult(res callResult) domain.Observation {
	var content string
	for i, b := range res.Content {
		if b.Type != "text" {
			continue
		}
		if i > 0 && content != "" {
			content += "\n"
		}
		content += b.Text
	}
	return domain.Observation{
		Content: content,
		IsError: res.IsError,
	}
}

// cloneRaw returns a copy of raw so the spec does not alias decoder-owned
// memory. Nil in, nil out.
func cloneRaw(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	out := make(json.RawMessage, len(raw))
	copy(out, raw)
	return out
}

// ---------------------------------------------------------------------------
// default subprocess spawn (os/exec)
// ---------------------------------------------------------------------------

// execSpawn is the default [spawnFunc]: it launches the configured command as a
// subprocess and binds the conn to its stdin/stdout. The subprocess is confined
// — it is given ONLY ref.Env (no inherited os.Environ), so service credentials
// and the SPIRE socket never enter its namespace (ADR-0013 §"MCP server
// confinement"). ctx cancellation kills the subprocess.
func execSpawn(ctx context.Context, ref ServerRefSpawn) (transport, error) {
	if ref.Command == "" {
		return nil, fmt.Errorf("mcp: no command configured for server %q", ref.Name)
	}
	cmd := exec.CommandContext(ctx, ref.Command, ref.Args...) //nolint:gosec // command comes from trusted operator config, not the model
	// Confinement: a nil Env on exec.Cmd would inherit the parent's; set it
	// explicitly (empty slice ⇒ empty environment) so nothing leaks in.
	if ref.Env == nil {
		cmd.Env = []string{}
	} else {
		cmd.Env = ref.Env
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("mcp: stdin pipe for %q: %w", ref.Name, err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("mcp: stdout pipe for %q: %w", ref.Name, err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("mcp: start %q: %w", ref.Name, err)
	}
	return &execTransport{cmd: cmd, stdin: stdin, stdout: stdout}, nil
}

// execTransport binds the conn to a subprocess's stdio and tears the process
// down on Close.
type execTransport struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
}

func (t *execTransport) Read(b []byte) (int, error)  { return t.stdout.Read(b) }
func (t *execTransport) Write(b []byte) (int, error) { return t.stdin.Write(b) }

// Close closes the subprocess's stdin (signaling EOF) and waits for it to exit,
// killing it if necessary. It is safe to call once per session.
func (t *execTransport) Close() error {
	_ = t.stdin.Close()
	_ = t.stdout.Close()
	if t.cmd.Process != nil {
		_ = t.cmd.Process.Kill()
	}
	_ = t.cmd.Wait()
	return nil
}
