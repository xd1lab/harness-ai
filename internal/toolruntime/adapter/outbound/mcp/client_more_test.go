package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/boltrope/boltrope/internal/toolruntime/app"
)

// ---------------------------------------------------------------------------
// cannedServer — a JSON-RPC server whose per-method replies a test can corrupt
// one at a time. Methods absent from both maps get a protocol-valid default, so
// each test breaks exactly one step (initialize, tools/list, tools/call) while
// the rest of the exchange stays well-formed. The server is UNTRUSTED input to
// the client (ADR-0013), so these malformed-reply paths are security-relevant,
// not hypothetical.
// ---------------------------------------------------------------------------

type cannedServer struct {
	// results maps a method to a raw JSON result to return verbatim.
	results map[string]string
	// errors maps a method to a JSON-RPC error object to return instead.
	errors map[string]*jsonrpcError
}

func (s *cannedServer) serve(ctx context.Context, r io.Reader, w io.WriteCloser) {
	defer func() { _ = w.Close() }()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		if ctx.Err() != nil {
			return
		}
		var req jsonrpcRequest
		if err := json.Unmarshal(sc.Bytes(), &req); err != nil || req.ID == nil {
			continue // notification or noise: no reply
		}
		resp := jsonrpcResponse{JSONRPC: jsonrpcVersion, ID: req.ID}
		switch {
		case s.errors[req.Method] != nil:
			resp.Error = s.errors[req.Method]
		case s.results[req.Method] != "":
			resp.Result = json.RawMessage(s.results[req.Method])
		default:
			resp.Result = cannedDefaultResult(req.Method)
		}
		out, _ := json.Marshal(resp)
		if _, err := w.Write(append(out, '\n')); err != nil {
			return
		}
	}
}

// cannedDefaultResult returns a minimal protocol-valid result for each method
// the client issues, so unbroken steps succeed.
func cannedDefaultResult(method string) json.RawMessage {
	switch method {
	case "initialize":
		return json.RawMessage(`{"protocolVersion":"` + protocolVersion + `","serverInfo":{"name":"canned","version":"1"}}`)
	case "tools/list":
		return json.RawMessage(`{"tools":[]}`)
	case "tools/call":
		return json.RawMessage(`{"content":[{"type":"text","text":"ok"}],"isError":false}`)
	default:
		return json.RawMessage(`{}`)
	}
}

func newCannedClient(t *testing.T, s *cannedServer) *Client {
	t.Helper()
	c := New()
	c.spawn = func(ctx context.Context, _ ServerRefSpawn) (transport, error) {
		clientReader, serverWriter := io.Pipe()
		serverReader, clientWriter := io.Pipe()
		go s.serve(ctx, serverReader, serverWriter)
		return &pipeTransport{r: clientReader, w: clientWriter, closes: []io.Closer{clientWriter, clientReader}}, nil
	}
	return c
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestHandshake_MalformedInitializeResult asserts a server whose initialize
// result does not decode is rejected with a decode error — the client never
// proceeds to use a server it could not even handshake with.
func TestHandshake_MalformedInitializeResult(t *testing.T) {
	c := newCannedClient(t, &cannedServer{results: map[string]string{"initialize": `[1,2,3]`}})

	_, err := c.ListTools(context.Background(), testServerRef())
	if err == nil {
		t.Fatalf("a malformed initialize result must fail the handshake")
	}
	if !strings.Contains(err.Error(), "decode initialize result") {
		t.Fatalf("want a decode error naming the step, got %v", err)
	}
}

// TestListTools_MalformedResult asserts a tools/list result that does not match
// the wire shape surfaces a decode error instead of silently yielding no tools.
func TestListTools_MalformedResult(t *testing.T) {
	c := newCannedClient(t, &cannedServer{results: map[string]string{"tools/list": `{"tools":42}`}})

	_, err := c.ListTools(context.Background(), testServerRef())
	if err == nil {
		t.Fatalf("a malformed tools/list result must surface an error")
	}
	if !strings.Contains(err.Error(), "decode tools/list result") {
		t.Fatalf("want a decode error naming the step, got %v", err)
	}
}

// TestListTools_ServerError asserts a JSON-RPC error from tools/list is wrapped
// with the server context and remains recoverable as a *jsonrpcError.
func TestListTools_ServerError(t *testing.T) {
	c := newCannedClient(t, &cannedServer{errors: map[string]*jsonrpcError{
		"tools/list": {Code: -32000, Message: "catalog unavailable"},
	}})

	_, err := c.ListTools(context.Background(), testServerRef())
	if err == nil {
		t.Fatalf("a tools/list server error must surface")
	}
	var rpcErr *jsonrpcError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32000 {
		t.Fatalf("want the wrapped *jsonrpcError (code -32000), got %v", err)
	}
	if !strings.Contains(err.Error(), "tools/list on") {
		t.Fatalf("the error must name the failing step and server, got %v", err)
	}
}

// TestCallTool_MalformedResult asserts a tools/call result that does not decode
// is a transport-level Go error, never a fabricated Observation.
func TestCallTool_MalformedResult(t *testing.T) {
	c := newCannedClient(t, &cannedServer{results: map[string]string{"tools/call": `"nope"`}})

	_, err := c.CallTool(context.Background(), testServerRef(), "sess", "echo", nil)
	if err == nil {
		t.Fatalf("a malformed tools/call result must surface an error")
	}
	if !strings.Contains(err.Error(), "decode tools/call result") {
		t.Fatalf("want a decode error naming the step, got %v", err)
	}
}

// TestCallTool_ServerError asserts a protocol-level JSON-RPC error from
// tools/call is a Go error — distinct from the isError:true case, which is a
// tool that RAN and failed (covered in mcp_test.go).
func TestCallTool_ServerError(t *testing.T) {
	c := newCannedClient(t, &cannedServer{errors: map[string]*jsonrpcError{
		"tools/call": {Code: -32602, Message: "invalid params"},
	}})

	obs, err := c.CallTool(context.Background(), testServerRef(), "sess", "echo", map[string]any{"x": 1})
	if err == nil {
		t.Fatalf("a tools/call protocol error must be a Go error, got obs=%+v", obs)
	}
	var rpcErr *jsonrpcError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32602 {
		t.Fatalf("want the wrapped *jsonrpcError (code -32602), got %v", err)
	}
}

// TestListTools_MissingSchemaDefaultsToObject asserts a tool advertised without
// an inputSchema gets the permissive empty-object schema, so the registry still
// has something to validate inputs against (never a nil schema).
func TestListTools_MissingSchemaDefaultsToObject(t *testing.T) {
	c := newCannedClient(t, &cannedServer{results: map[string]string{
		"tools/list": `{"tools":[{"name":"bare","description":"no schema"}]}`,
	}})

	specs, err := c.ListTools(context.Background(), testServerRef())
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("want 1 spec, got %d", len(specs))
	}
	if got := string(specs[0].JSONSchema); got != `{"type":"object"}` {
		t.Fatalf("missing schema must default to the empty object schema, got %s", got)
	}
}

// TestDial_TransportGate asserts this stdio adapter refuses refs naming any
// other transport (gated with ErrUnsupportedTransport before a spawn is even
// attempted) while accepting both "stdio" and the empty default.
func TestDial_TransportGate(t *testing.T) {
	t.Run("http transport is refused", func(t *testing.T) {
		spawned := false
		c := New()
		c.spawn = func(context.Context, ServerRefSpawn) (transport, error) {
			spawned = true
			return nil, errors.New("must not be reached")
		}

		ref := app.MCPServerRef{Name: "fake", Transport: "http"}
		if _, err := c.ListTools(context.Background(), ref); !errors.Is(err, ErrUnsupportedTransport) {
			t.Fatalf("want ErrUnsupportedTransport from ListTools, got %v", err)
		}
		if _, err := c.CallTool(context.Background(), ref, "s", "echo", nil); !errors.Is(err, ErrUnsupportedTransport) {
			t.Fatalf("want ErrUnsupportedTransport from CallTool, got %v", err)
		}
		if spawned {
			t.Fatalf("an unsupported transport must be rejected before spawning")
		}
	})

	t.Run("empty transport defaults to stdio", func(t *testing.T) {
		c := newCannedClient(t, &cannedServer{})
		if _, err := c.ListTools(context.Background(), app.MCPServerRef{Name: "fake"}); err != nil {
			t.Fatalf("an empty transport must be treated as stdio, got %v", err)
		}
	})
}

// TestObservationFromResult_FoldsTextBlocks pins the content-folding rules: only
// text blocks contribute, joined by newlines, with non-text blocks skipped
// (they are typed data this client does not interpret), and isError is carried.
func TestObservationFromResult_FoldsTextBlocks(t *testing.T) {
	tests := []struct {
		name        string
		res         callResult
		wantContent string
		wantIsError bool
	}{
		{
			name:        "no content",
			res:         callResult{},
			wantContent: "",
		},
		{
			name:        "single text block",
			res:         callResult{Content: []contentBlock{{Type: "text", Text: "hello"}}},
			wantContent: "hello",
		},
		{
			name: "non-text block between text blocks is skipped, texts joined",
			res: callResult{Content: []contentBlock{
				{Type: "text", Text: "a"},
				{Type: "image", Text: "ignored"},
				{Type: "text", Text: "b"},
			}},
			wantContent: "a\nb",
		},
		{
			name: "leading non-text block adds no leading newline",
			res: callResult{Content: []contentBlock{
				{Type: "image", Text: "ignored"},
				{Type: "text", Text: "only"},
			}},
			wantContent: "only",
		},
		{
			name:        "isError carried through",
			res:         callResult{Content: []contentBlock{{Type: "text", Text: "failed"}}, IsError: true},
			wantContent: "failed",
			wantIsError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obs := observationFromResult(tc.res)
			if obs.Content != tc.wantContent {
				t.Fatalf("content: want %q, got %q", tc.wantContent, obs.Content)
			}
			if obs.IsError != tc.wantIsError {
				t.Fatalf("isError: want %v, got %v", tc.wantIsError, obs.IsError)
			}
		})
	}
}

// TestCloneRaw asserts the aliasing guard: nil stays nil, and the clone shares
// no memory with the decoder-owned input.
func TestCloneRaw(t *testing.T) {
	if got := cloneRaw(nil); got != nil {
		t.Fatalf("nil in must be nil out, got %v", got)
	}

	src := json.RawMessage(`{"a":1}`)
	clone := cloneRaw(src)
	if string(clone) != string(src) {
		t.Fatalf("clone must equal the source, got %s", clone)
	}
	src[2] = 'X' // mutate the original; the clone must be unaffected
	if string(clone) != `{"a":1}` {
		t.Fatalf("clone must not alias the source buffer, got %s", clone)
	}
}
