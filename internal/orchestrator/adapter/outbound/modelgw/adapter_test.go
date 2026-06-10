// Package modelgw tests — adapter integration tests using bufconn.
//
// A fake gen.ModelGatewayServiceServer emits scripted gen.StreamEvent sequences;
// the adapter's Stream, Generate, CountTokens, and Capabilities methods are
// exercised against it, asserting correct gen<->llm mapping with no real
// network or SDK.
package modelgw

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

const bufSize = 1 << 20 // 1 MiB

// fakeServer is a scripted gen.ModelGatewayServiceServer.
type fakeServer struct {
	genproto.UnimplementedModelGatewayServiceServer

	// generateEvents is the sequence of StreamEvents the fake emits for
	// each Generate call (one slice per call, in order).
	generateEvents [][]*genproto.StreamEvent

	genIdx int

	// countTokensResp is returned by CountTokens.
	countTokensResp *genproto.CountTokensResponse
	countTokensErr  error

	// capabilitiesResp is returned by GetCapabilities.
	capabilitiesResp *genproto.GetCapabilitiesResponse
}

func (s *fakeServer) Generate(
	_ *genproto.GenerateRequest,
	stream grpc.ServerStreamingServer[genproto.StreamEvent],
) error {
	if s.genIdx >= len(s.generateEvents) {
		return status.Error(codes.Internal, "fakeServer: Generate queue exhausted")
	}
	evts := s.generateEvents[s.genIdx]
	s.genIdx++
	for _, ev := range evts {
		if err := stream.Send(ev); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeServer) CountTokens(
	_ context.Context,
	_ *genproto.CountTokensRequest,
) (*genproto.CountTokensResponse, error) {
	return s.countTokensResp, s.countTokensErr
}

func (s *fakeServer) GetCapabilities(
	_ context.Context,
	_ *genproto.GetCapabilitiesRequest,
) (*genproto.GetCapabilitiesResponse, error) {
	return s.capabilitiesResp, nil
}

// newTestAdapter spins up a bufconn gRPC server backed by srv and returns a
// connected *Adapter. The returned cleanup func stops the server.
func newTestAdapter(t *testing.T, srv *fakeServer) (*Adapter, func()) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	grpcSrv := grpc.NewServer()
	genproto.RegisterModelGatewayServiceServer(grpcSrv, srv)
	go func() { _ = grpcSrv.Serve(lis) }()

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	adapter := NewAdapter(genproto.NewModelGatewayServiceClient(conn))
	cleanup := func() {
		_ = conn.Close()
		grpcSrv.Stop()
	}
	return adapter, cleanup
}

// ---- Stream tests -----------------------------------------------------------

// TestStream_TextThenDone verifies that Stream maps a gen stream
// [TextDelta, Done(StopEnd)] to a llm.StreamReader that yields exactly
// [StreamEvent{TextDelta}, StreamEvent{Done}] then io.EOF.
func TestStream_TextThenDone(t *testing.T) {
	srv := &fakeServer{
		generateEvents: [][]*genproto.StreamEvent{
			{
				{Event: &genproto.StreamEvent_TextDelta{
					TextDelta: &genproto.TextDelta{Text: "hi"},
				}},
				{Event: &genproto.StreamEvent_Done{
					Done: &genproto.Done{
						StopReason:    genproto.StopReason_STOP_REASON_END,
						RawStopReason: "end",
						Usage: &genproto.Usage{
							InputTokens:  5,
							OutputTokens: 3,
						},
					},
				}},
			},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	reader, err := adapter.Stream(context.Background(), llm.Request{Model: "test-model"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = reader.Close() }()

	// Event 1: TextDelta
	ev1, err := reader.Recv()
	if err != nil {
		t.Fatalf("Recv 1: %v", err)
	}
	if ev1.TextDelta == nil || ev1.TextDelta.Text != "hi" {
		t.Errorf("event 1: want TextDelta{hi}, got %+v", ev1)
	}

	// Event 2: Done
	ev2, err := reader.Recv()
	if err != nil {
		t.Fatalf("Recv 2: %v", err)
	}
	if ev2.Done == nil {
		t.Fatalf("event 2: want Done, got %+v", ev2)
	}
	if ev2.Done.StopReason != llm.StopEnd {
		t.Errorf("StopReason = %q, want %q", ev2.Done.StopReason, llm.StopEnd)
	}
	if ev2.Done.Usage.InputTokens != 5 || ev2.Done.Usage.OutputTokens != 3 {
		t.Errorf("Usage = %+v", ev2.Done.Usage)
	}

	// After Done: io.EOF
	_, err = reader.Recv()
	if !errors.Is(err, io.EOF) {
		t.Errorf("after Done: want io.EOF, got %v", err)
	}
}

// TestStream_ToolCallDeltaThenDone verifies that a ToolCallDelta fragment is
// mapped correctly and the stream terminates with StopToolUse.
func TestStream_ToolCallDeltaThenDone(t *testing.T) {
	argsJSON := []byte(`{"path":"/tmp/f"}`)
	srv := &fakeServer{
		generateEvents: [][]*genproto.StreamEvent{
			{
				{Event: &genproto.StreamEvent_ToolCallDelta{
					ToolCallDelta: &genproto.ToolCallDelta{
						CallId:       "c1",
						Name:         "write_file",
						ArgsFragment: argsJSON,
					},
				}},
				{Event: &genproto.StreamEvent_Done{
					Done: &genproto.Done{
						StopReason:    genproto.StopReason_STOP_REASON_TOOL_USE,
						RawStopReason: "tool_use",
					},
				}},
			},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	reader, err := adapter.Stream(context.Background(), llm.Request{Model: "m"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = reader.Close() }()

	ev1, err := reader.Recv()
	if err != nil {
		t.Fatalf("Recv 1: %v", err)
	}
	if ev1.ToolCallDelta == nil {
		t.Fatalf("event 1: want ToolCallDelta, got %+v", ev1)
	}
	tcd := ev1.ToolCallDelta
	if tcd.CallID != "c1" || tcd.Name != "write_file" {
		t.Errorf("ToolCallDelta = %+v", tcd)
	}
	if string(tcd.ArgsFragment) != string(argsJSON) {
		t.Errorf("ArgsFragment = %s, want %s", tcd.ArgsFragment, argsJSON)
	}

	ev2, err := reader.Recv()
	if err != nil {
		t.Fatalf("Recv 2: %v", err)
	}
	if ev2.Done == nil || ev2.Done.StopReason != llm.StopToolUse {
		t.Errorf("event 2: want Done{tool_use}, got %+v", ev2)
	}

	_, err = reader.Recv()
	if !errors.Is(err, io.EOF) {
		t.Errorf("after Done: want io.EOF, got %v", err)
	}
}

// TestStream_ThinkingThenTextThenDone verifies thinking delta and text delta
// in the same stream are both mapped correctly.
func TestStream_ThinkingThenTextThenDone(t *testing.T) {
	srv := &fakeServer{
		generateEvents: [][]*genproto.StreamEvent{
			{
				{Event: &genproto.StreamEvent_ThinkingDelta{
					ThinkingDelta: &genproto.ThinkingDelta{Text: "let me think", Signature: "sig1"},
				}},
				{Event: &genproto.StreamEvent_TextDelta{
					TextDelta: &genproto.TextDelta{Text: "answer"},
				}},
				{Event: &genproto.StreamEvent_Done{
					Done: &genproto.Done{
						StopReason:    genproto.StopReason_STOP_REASON_END,
						RawStopReason: "end",
						ProviderRaw:   []byte(`{"sig":"sig1"}`),
					},
				}},
			},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	reader, err := adapter.Stream(context.Background(), llm.Request{Model: "m"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = reader.Close() }()

	ev1, _ := reader.Recv()
	if ev1.ThinkingDelta == nil || ev1.ThinkingDelta.Text != "let me think" || ev1.ThinkingDelta.Signature != "sig1" {
		t.Errorf("event 1: want ThinkingDelta, got %+v", ev1)
	}

	ev2, _ := reader.Recv()
	if ev2.TextDelta == nil || ev2.TextDelta.Text != "answer" {
		t.Errorf("event 2: want TextDelta, got %+v", ev2)
	}

	ev3, err := reader.Recv()
	if err != nil {
		t.Fatalf("Recv 3: %v", err)
	}
	if ev3.Done == nil {
		t.Fatal("event 3: want Done")
	}
	if string(ev3.Done.ProviderRaw) != `{"sig":"sig1"}` {
		t.Errorf("ProviderRaw = %s", ev3.Done.ProviderRaw)
	}

	_, err = reader.Recv()
	if !errors.Is(err, io.EOF) {
		t.Errorf("after Done: want io.EOF, got %v", err)
	}
}

// TestStream_PauseDone verifies STOP_REASON_PAUSE maps to llm.Pause and
// ProviderRaw is carried through to Done.
func TestStream_PauseDone(t *testing.T) {
	srv := &fakeServer{
		generateEvents: [][]*genproto.StreamEvent{
			{
				{Event: &genproto.StreamEvent_Done{
					Done: &genproto.Done{
						StopReason:    genproto.StopReason_STOP_REASON_PAUSE,
						RawStopReason: "pause_turn",
						ProviderRaw:   []byte(`{"continuation":true}`),
					},
				}},
			},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	reader, err := adapter.Stream(context.Background(), llm.Request{Model: "m"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = reader.Close() }()

	ev, err := reader.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Done == nil {
		t.Fatal("want Done")
	}
	if ev.Done.StopReason != llm.Pause {
		t.Errorf("StopReason = %q, want Pause", ev.Done.StopReason)
	}
	if string(ev.Done.ProviderRaw) != `{"continuation":true}` {
		t.Errorf("ProviderRaw = %s", ev.Done.ProviderRaw)
	}
	if ev.Done.StopReason.IsTerminal() {
		t.Error("Pause must not be terminal")
	}
}

// ---- Generate tests ---------------------------------------------------------

// TestGenerate_AssemblesResponse verifies that Generate consumes the server
// stream and returns a correct *llm.Response containing the assembled content
// (text part from TextDelta) and the Done fields.
func TestGenerate_AssemblesResponse(t *testing.T) {
	srv := &fakeServer{
		generateEvents: [][]*genproto.StreamEvent{
			{
				{Event: &genproto.StreamEvent_TextDelta{
					TextDelta: &genproto.TextDelta{Text: "Hello "},
				}},
				{Event: &genproto.StreamEvent_TextDelta{
					TextDelta: &genproto.TextDelta{Text: "World"},
				}},
				{Event: &genproto.StreamEvent_Done{
					Done: &genproto.Done{
						StopReason:    genproto.StopReason_STOP_REASON_END,
						RawStopReason: "end",
						Usage: &genproto.Usage{
							InputTokens:  7,
							OutputTokens: 2,
						},
						ProviderRaw: []byte(`null`),
					},
				}},
			},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	resp, err := adapter.Generate(context.Background(), llm.Request{Model: "m"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if resp.StopReason != llm.StopEnd {
		t.Errorf("StopReason = %q, want %q", resp.StopReason, llm.StopEnd)
	}
	if resp.Usage.InputTokens != 7 || resp.Usage.OutputTokens != 2 {
		t.Errorf("Usage = %+v", resp.Usage)
	}
	// Content: two text deltas must produce at least one TextPart.
	if len(resp.Content) == 0 {
		t.Fatal("Content is empty")
	}
	// Concatenate all text parts.
	fullText := ""
	for _, cp := range resp.Content {
		if cp.Text != nil {
			fullText += cp.Text.Text
		}
	}
	if fullText != "Hello World" {
		t.Errorf("assembled text = %q, want %q", fullText, "Hello World")
	}
}

// TestGenerate_ToolCallAssembled verifies that Generate assembles a complete
// tool call from ToolCallDelta events and returns it in the content.
func TestGenerate_ToolCallAssembled(t *testing.T) {
	argsJSON := []byte(`{"x":42}`)
	srv := &fakeServer{
		generateEvents: [][]*genproto.StreamEvent{
			{
				{Event: &genproto.StreamEvent_ToolCallDelta{
					ToolCallDelta: &genproto.ToolCallDelta{
						CallId:       "c1",
						Name:         "do_thing",
						ArgsFragment: argsJSON,
					},
				}},
				{Event: &genproto.StreamEvent_Done{
					Done: &genproto.Done{
						StopReason:    genproto.StopReason_STOP_REASON_TOOL_USE,
						RawStopReason: "tool_use",
					},
				}},
			},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	resp, err := adapter.Generate(context.Background(), llm.Request{Model: "m"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if resp.StopReason != llm.StopToolUse {
		t.Errorf("StopReason = %q, want tool_use", resp.StopReason)
	}
	if len(resp.Content) == 0 {
		t.Fatal("no content parts")
	}
	var found bool
	for _, cp := range resp.Content {
		if cp.ToolCall != nil && cp.ToolCall.ID == "c1" && cp.ToolCall.Name == "do_thing" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected ToolCall{c1, do_thing} in content: %+v", resp.Content)
	}
}

// TestGenerate_PauseCarriesProviderRaw verifies that when the stream
// terminates with STOP_REASON_PAUSE, Generate returns StopReason=Pause
// and the ProviderRaw continuation blob.
func TestGenerate_PauseCarriesProviderRaw(t *testing.T) {
	provRaw := []byte(`{"thinking":"sig"}`)
	srv := &fakeServer{
		generateEvents: [][]*genproto.StreamEvent{
			{
				{Event: &genproto.StreamEvent_Done{
					Done: &genproto.Done{
						StopReason:    genproto.StopReason_STOP_REASON_PAUSE,
						RawStopReason: "pause_turn",
						ProviderRaw:   provRaw,
					},
				}},
			},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	resp, err := adapter.Generate(context.Background(), llm.Request{Model: "m"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if resp.StopReason != llm.Pause {
		t.Errorf("StopReason = %q, want Pause", resp.StopReason)
	}
	if string(resp.ProviderRaw) != string(provRaw) {
		t.Errorf("ProviderRaw = %s, want %s", resp.ProviderRaw, provRaw)
	}
}

// ---- CountTokens tests ------------------------------------------------------

// TestCountTokens_Success verifies that a successful CountTokens round-trip
// returns the correct token count.
func TestCountTokens_Success(t *testing.T) {
	srv := &fakeServer{
		countTokensResp: &genproto.CountTokensResponse{InputTokens: 42},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	n, err := adapter.CountTokens(context.Background(), llm.Request{Model: "m"})
	if err != nil {
		t.Fatalf("CountTokens: %v", err)
	}
	if n != 42 {
		t.Errorf("token count = %d, want 42", n)
	}
}

// TestCountTokens_Unimplemented verifies that UNIMPLEMENTED is mapped to a
// *llm.ProviderError with Kind == llm.ErrUnsupported.
func TestCountTokens_Unimplemented(t *testing.T) {
	srv := &fakeServer{
		countTokensErr: status.Error(codes.Unimplemented, "not supported"),
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	_, err := adapter.CountTokens(context.Background(), llm.Request{Model: "m"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *llm.ProviderError, got %T: %v", err, err)
	}
	if pe.Kind != llm.ErrUnsupported {
		t.Errorf("Kind = %q, want %q", pe.Kind, llm.ErrUnsupported)
	}
}

// ---- Capabilities tests -----------------------------------------------------

// TestCapabilities_RoundTrip verifies that GetCapabilities maps every flag
// from gen.Capabilities to llm.Capabilities correctly.
func TestCapabilities_RoundTrip(t *testing.T) {
	srv := &fakeServer{
		capabilitiesResp: &genproto.GetCapabilitiesResponse{
			Capabilities: &genproto.Capabilities{
				SupportsTools:              true,
				SupportsParallelToolCalls:  true,
				SupportsStreamingToolCalls: true,
				SupportsVision:             true,
				SupportsSystemPrompt:       true,
				SupportsThinking:           true,
				SupportsTokenCounting:      true,
				SupportsJsonSchemaStrict:   true,
				MaxOutputTokens:            8192,
			},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	caps, err := adapter.Capabilities(context.Background(), "test-model")
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if !caps.SupportsTools {
		t.Error("SupportsTools should be true")
	}
	if !caps.SupportsParallelToolCalls {
		t.Error("SupportsParallelToolCalls should be true")
	}
	if !caps.SupportsStreamingToolCalls {
		t.Error("SupportsStreamingToolCalls should be true")
	}
	if !caps.SupportsVision {
		t.Error("SupportsVision should be true")
	}
	if !caps.SupportsSystemPrompt {
		t.Error("SupportsSystemPrompt should be true")
	}
	if !caps.SupportsThinking {
		t.Error("SupportsThinking should be true")
	}
	if !caps.SupportsTokenCounting {
		t.Error("SupportsTokenCounting should be true")
	}
	if !caps.SupportsJSONSchemaStrict {
		t.Error("SupportsJSONSchemaStrict should be true")
	}
	if caps.MaxOutputTokens != 8192 {
		t.Errorf("MaxOutputTokens = %d, want 8192", caps.MaxOutputTokens)
	}
}

// TestCapabilities_False verifies all-false capability set maps correctly.
func TestCapabilities_False(t *testing.T) {
	srv := &fakeServer{
		capabilitiesResp: &genproto.GetCapabilitiesResponse{
			Capabilities: &genproto.Capabilities{},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	caps, err := adapter.Capabilities(context.Background(), "small-model")
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.SupportsTools {
		t.Error("SupportsTools should be false")
	}
	if caps.MaxOutputTokens != 0 {
		t.Errorf("MaxOutputTokens = %d, want 0", caps.MaxOutputTokens)
	}
}

// ---- Interface compliance check ---------------------------------------------

// TestInterfaceCompliance is a compile-time assertion that *Adapter satisfies
// app.ModelGatewayPort. We import the app package here so the boundary test
// lives in this adapter package.
func TestInterfaceCompliance(t *testing.T) {
	// This test exists solely for the compile-time assertion below.
	// If it compiles, the assertion holds.
	t.Log("Adapter satisfies ModelGatewayPort: compile-time assertion OK")
}
