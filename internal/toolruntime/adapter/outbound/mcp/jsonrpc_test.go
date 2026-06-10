package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// transport fakes for conn-level tests. They isolate single failure modes the
// full fakeServer pipes cannot produce deterministically (write failures,
// torn-down reads, oversized frames).
// ---------------------------------------------------------------------------

// blockedTransport accepts writes and blocks reads until closed (then EOF). It
// simulates a server that never answers.
type blockedTransport struct {
	done chan struct{}
	once sync.Once
}

func newBlockedTransport() *blockedTransport {
	return &blockedTransport{done: make(chan struct{})}
}

func (b *blockedTransport) Read([]byte) (int, error) {
	<-b.done
	return 0, io.EOF
}

func (b *blockedTransport) Write(p []byte) (int, error) { return len(p), nil }

func (b *blockedTransport) Close() error {
	b.once.Do(func() { close(b.done) })
	return nil
}

// failWriteTransport rejects every write; reads block until closed.
type failWriteTransport struct {
	*blockedTransport
	writeErr error
}

func (f *failWriteTransport) Write([]byte) (int, error) { return 0, f.writeErr }

// signalTransport reads from r and signals wrote once the first request has
// been written, so a test can synchronize "the call is in flight" before
// injecting a failure. Writes are swallowed.
type signalTransport struct {
	r     io.ReadCloser
	wrote chan struct{}
	once  sync.Once
}

func (s *signalTransport) Read(p []byte) (int, error) { return s.r.Read(p) }

func (s *signalTransport) Write(p []byte) (int, error) {
	s.once.Do(func() { close(s.wrote) })
	return len(p), nil
}

func (s *signalTransport) Close() error { return s.r.Close() }

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestConn_Call_MarshalParamsError asserts unmarshalable params fail the call
// (and a notification) up front — before a waiter is registered or anything
// hits the wire — so a bad payload cannot wedge the connection.
func TestConn_Call_MarshalParamsError(t *testing.T) {
	tr := newBlockedTransport()
	c := newConn(tr)
	t.Cleanup(func() { _ = c.close() })

	_, err := c.call(context.Background(), "tools/call", map[string]any{"bad": make(chan int)})
	if err == nil {
		t.Fatalf("call with unmarshalable params must fail")
	}
	if got := err.Error(); !strings.Contains(got, "marshal params") {
		t.Fatalf("want marshal params error, got %q", got)
	}

	if err := c.notify("notify", func() {}); err == nil {
		t.Fatalf("notify with unmarshalable params must fail")
	}

	c.mu.Lock()
	n := len(c.waiters)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("no waiter must be left behind after a marshal failure, got %d", n)
	}
}

// TestConn_Call_WriteErrorDeregistersWaiter asserts a transport write failure
// surfaces to the caller AND removes the registered waiter, so the waiters map
// cannot leak entries for requests that never reached the server.
func TestConn_Call_WriteErrorDeregistersWaiter(t *testing.T) {
	sink := errors.New("sink failure")
	tr := &failWriteTransport{blockedTransport: newBlockedTransport(), writeErr: sink}
	c := newConn(tr)
	t.Cleanup(func() { _ = c.close() })

	_, err := c.call(context.Background(), "tools/list", nil)
	if err == nil {
		t.Fatalf("call must fail when the transport write fails")
	}
	if !errors.Is(err, sink) {
		t.Fatalf("the transport error must be wrapped, got %v", err)
	}

	c.mu.Lock()
	n := len(c.waiters)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("the waiter must be deregistered after a write failure, got %d", n)
	}
}

// TestConn_Call_CancelledContext asserts an already-cancelled context aborts
// the call immediately (no response will ever arrive from the silent server)
// and deregisters the waiter.
func TestConn_Call_CancelledContext(t *testing.T) {
	tr := newBlockedTransport()
	c := newConn(tr)
	t.Cleanup(func() { _ = c.close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.call(ctx, "tools/list", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}

	c.mu.Lock()
	n := len(c.waiters)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("the waiter must be deregistered after cancellation, got %d", n)
	}
}

// TestConn_Call_ConnectionDiesMidCall asserts that when the transport read side
// fails while a call is in flight, (a) the in-flight call is unblocked with the
// read error rather than hanging forever, and (b) subsequent calls are refused
// up front on the now-closed connection with the same root cause.
func TestConn_Call_ConnectionDiesMidCall(t *testing.T) {
	pr, pw := io.Pipe()
	tr := &signalTransport{r: pr, wrote: make(chan struct{})}
	c := newConn(tr)
	t.Cleanup(func() { _ = c.close() })

	torn := errors.New("transport torn down")
	errCh := make(chan error, 1)
	go func() {
		_, err := c.call(context.Background(), "tools/list", nil)
		errCh <- err
	}()

	// Wait until the request has been written — the waiter is registered
	// strictly before the write, so the call is provably in flight — then kill
	// the read side.
	<-tr.wrote
	_ = pw.CloseWithError(torn)

	select {
	case err := <-errCh:
		if !errors.Is(err, torn) {
			t.Fatalf("the in-flight call must surface the read error, got %v", err)
		}
		if !strings.Contains(err.Error(), "connection closed") {
			t.Fatalf("want a connection-closed error, got %q", err.Error())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("in-flight call hung after the connection died")
	}

	// The conn is now closed: a new call must be refused immediately, citing
	// the original failure.
	_, err := c.call(context.Background(), "tools/call", nil)
	if !errors.Is(err, torn) {
		t.Fatalf("a call on the dead connection must carry the original error, got %v", err)
	}
	if !strings.Contains(err.Error(), "closed connection") {
		t.Fatalf("want a closed-connection refusal, got %q", err.Error())
	}
}

// TestConn_ReadLoop_SkipsNoiseAndUnmatchedFrames asserts the reader tolerates
// the garbage real stdio servers emit — blank lines, non-JSON log noise,
// notifications without an ID, and responses for IDs nobody is waiting on —
// and still routes the genuine response to its caller.
func TestConn_ReadLoop_SkipsNoiseAndUnmatchedFrames(t *testing.T) {
	clientReader, serverWriter := io.Pipe()
	serverReader, clientWriter := io.Pipe()
	tr := &pipeTransport{r: clientReader, w: clientWriter, closes: []io.Closer{clientWriter, clientReader}}
	c := newConn(tr)
	t.Cleanup(func() { _ = c.close() })

	go func() {
		defer func() { _ = serverWriter.Close() }()
		sc := bufio.NewScanner(serverReader)
		if !sc.Scan() {
			return
		}
		var req jsonrpcRequest
		if err := json.Unmarshal(sc.Bytes(), &req); err != nil || req.ID == nil {
			return
		}
		// Noise first, real answer last.
		_, _ = io.WriteString(serverWriter, "\n")                                             // blank line
		_, _ = io.WriteString(serverWriter, "log: server starting up\n")                      // non-JSON noise
		_, _ = io.WriteString(serverWriter, `{"jsonrpc":"2.0","method":"notify/x"}`+"\n")     // no ID
		_, _ = io.WriteString(serverWriter, `{"jsonrpc":"2.0","id":999999,"result":{}}`+"\n") // nobody waiting
		_, _ = fmt.Fprintf(serverWriter, `{"jsonrpc":"2.0","id":%d,"result":{"ok":true}}`+"\n", *req.ID)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := c.call(ctx, "ping", map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("call must survive interleaved noise: %v", err)
	}
	if string(raw) != `{"ok":true}` {
		t.Fatalf("want the genuine result routed back, got %s", raw)
	}
}

// TestConn_ReadLoop_OversizedLineFailsPendingCalls asserts a frame exceeding
// the 4 MiB line limit is a connection-fatal scanner error: the pending call is
// unblocked with bufio.ErrTooLong instead of hanging on a reader that can make
// no further progress.
func TestConn_ReadLoop_OversizedLineFailsPendingCalls(t *testing.T) {
	pr, pw := io.Pipe()
	tr := &signalTransport{r: pr, wrote: make(chan struct{})}
	c := newConn(tr)
	t.Cleanup(func() { _ = c.close() })

	errCh := make(chan error, 1)
	go func() {
		_, err := c.call(context.Background(), "tools/list", nil)
		errCh <- err
	}()

	go func() {
		<-tr.wrote
		// One byte past the reader's 4 MiB cap, with no newline in sight.
		big := bytes.Repeat([]byte{'a'}, 4*1024*1024+1)
		_, _ = pw.Write(big)
		_ = pw.Close()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, bufio.ErrTooLong) {
			t.Fatalf("want bufio.ErrTooLong to surface, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("pending call hung on an oversized frame")
	}
}
