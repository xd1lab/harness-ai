package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// jsonrpcVersion is the JSON-RPC protocol version string carried on every
// request and response (the only version JSON-RPC 2.0 defines).
const jsonrpcVersion = "2.0"

// jsonrpcRequest is a JSON-RPC 2.0 request object. A request with a nil ID is a
// notification (no response is expected).
type jsonrpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *int64          `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// jsonrpcResponse is a JSON-RPC 2.0 response object. Exactly one of Result or
// Error is set per the spec.
type jsonrpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *int64          `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonrpcError   `json:"error,omitempty"`
}

// jsonrpcError is a JSON-RPC 2.0 error object returned in a response.
type jsonrpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Error implements error so a server-returned JSON-RPC error can be propagated
// and inspected with errors.As.
func (e *jsonrpcError) Error() string {
	return fmt.Sprintf("mcp: jsonrpc error %d: %s", e.Code, e.Message)
}

// conn is a minimal JSON-RPC 2.0 client connection over a stdio transport. It
// frames messages as newline-delimited JSON (one JSON object per line), which
// is sufficient for the line-oriented stdio servers v1 targets and avoids the
// Content-Length header framing of the LSP base protocol.
//
// conn multiplexes request IDs over a single transport: each call assigns a
// fresh monotonically-increasing ID, registers a waiter, and the background
// reader routes responses back to the matching waiter by ID. This keeps the
// transport usable even though v1 issues one in-flight request at a time.
type conn struct {
	t       transport
	enc     *json.Encoder
	writeMu sync.Mutex

	nextID atomic.Int64

	mu      sync.Mutex
	waiters map[int64]chan jsonrpcResponse
	closed  bool
	readErr error
}

// newConn starts a conn over t and launches its background reader.
func newConn(t transport) *conn {
	c := &conn{
		t:       t,
		enc:     json.NewEncoder(t),
		waiters: make(map[int64]chan jsonrpcResponse),
	}
	go c.readLoop()
	return c
}

// readLoop reads newline-delimited responses and dispatches each to its waiter
// by ID. It exits when the transport is closed or returns an error; on exit it
// fails any outstanding waiters so callers do not hang.
func (c *conn) readLoop() {
	sc := bufio.NewScanner(c.t)
	// Allow large tool-result payloads (up to 4 MiB per line).
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var resp jsonrpcResponse
		if err := json.Unmarshal(line, &resp); err != nil {
			// A malformed line is skipped rather than tearing down the conn;
			// servers occasionally emit log noise on stdout.
			continue
		}
		if resp.ID == nil {
			continue // a server notification or unmatchable frame
		}
		c.deliver(*resp.ID, resp)
	}
	err := sc.Err()
	if err == nil {
		err = io.EOF
	}
	c.fail(err)
}

// deliver routes a response to the matching waiter, if any.
func (c *conn) deliver(id int64, resp jsonrpcResponse) {
	c.mu.Lock()
	ch, ok := c.waiters[id]
	if ok {
		delete(c.waiters, id)
	}
	c.mu.Unlock()
	if ok {
		ch <- resp
	}
}

// fail records the read error and unblocks every outstanding waiter.
func (c *conn) fail(err error) {
	c.mu.Lock()
	if c.readErr == nil {
		c.readErr = err
	}
	c.closed = true
	waiters := c.waiters
	c.waiters = make(map[int64]chan jsonrpcResponse)
	c.mu.Unlock()
	for _, ch := range waiters {
		close(ch)
	}
}

// notify sends a JSON-RPC notification (a request without an ID). No response
// is awaited.
func (c *conn) notify(method string, params any) error {
	req := jsonrpcRequest{JSONRPC: jsonrpcVersion, Method: method}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("mcp: marshal params for %s: %w", method, err)
		}
		req.Params = raw
	}
	return c.write(req)
}

// call sends a request and blocks until the matching response arrives, the
// context is cancelled, or the connection fails. A server-returned JSON-RPC
// error is returned as a *jsonrpcError. On success the raw result bytes are
// returned for the caller to unmarshal.
func (c *conn) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := c.nextID.Add(1)
	req := jsonrpcRequest{JSONRPC: jsonrpcVersion, ID: &id, Method: method}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("mcp: marshal params for %s: %w", method, err)
		}
		req.Params = raw
	}

	ch := make(chan jsonrpcResponse, 1)
	c.mu.Lock()
	if c.closed {
		err := c.readErr
		c.mu.Unlock()
		if err == nil {
			err = errors.New("mcp: connection closed")
		}
		return nil, fmt.Errorf("mcp: call %s on closed connection: %w", method, err)
	}
	c.waiters[id] = ch
	c.mu.Unlock()

	if err := c.write(req); err != nil {
		c.mu.Lock()
		delete(c.waiters, id)
		c.mu.Unlock()
		return nil, err
	}

	select {
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.waiters, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("mcp: call %s: %w", method, ctx.Err())
	case resp, ok := <-ch:
		if !ok {
			// Waiter closed by fail(): the connection died.
			c.mu.Lock()
			err := c.readErr
			c.mu.Unlock()
			if err == nil {
				err = io.EOF
			}
			return nil, fmt.Errorf("mcp: call %s: connection closed: %w", method, err)
		}
		if resp.Error != nil {
			return nil, resp.Error
		}
		return resp.Result, nil
	}
}

// write serializes and flushes one JSON-RPC message under the write lock. The
// json.Encoder appends a newline after each value, giving the line framing the
// reader expects.
func (c *conn) write(req jsonrpcRequest) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.enc.Encode(req); err != nil {
		return fmt.Errorf("mcp: write %s: %w", req.Method, err)
	}
	return nil
}

// close tears down the underlying transport. The background reader observes the
// closed transport and fails any outstanding waiters.
func (c *conn) close() error {
	return c.t.Close()
}
