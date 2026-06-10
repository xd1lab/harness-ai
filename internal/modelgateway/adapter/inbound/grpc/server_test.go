// Package grpc tests — model-gateway inbound gRPC server tests using bufconn.
//
// A FAKE llm.Provider (llmtest) is injected behind the app.Service; the
// generated ModelGatewayServiceServer is exercised over an in-memory bufconn
// connection. No real SDK or network is used. The tests assert:
//   - Generate streams gen.StreamEvents matching the llm.StreamEvents the fake
//     provider scripts (text/thinking/tool-call/done with usage, provider_raw,
//     and the open stop-reason set incl. Pause);
//   - cost is computed on the Done event from usage + pricing and reported once;
//   - the retry decorator is applied (errors-then-success re-opens the stream);
//   - CountTokens returns the count, and UNIMPLEMENTED when unsupported;
//   - GetCapabilities maps every flag and forces supports_server_side_tools=false
//     (the §8.12 hard policy switch).
package grpc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	genproto "github.com/boltrope/boltrope/gen/boltrope/v1"
	"github.com/boltrope/boltrope/internal/modelgateway/app"
	"github.com/boltrope/boltrope/internal/modelgateway/app/capabilities"
	"github.com/boltrope/boltrope/internal/modelgateway/app/retry"
	"github.com/boltrope/boltrope/internal/platform/llm"
	"github.com/boltrope/boltrope/internal/platform/llm/llmtest"
	"github.com/boltrope/boltrope/internal/platform/pricing"
)

const bufSize = 1 << 20 // 1 MiB

// fakeClock fires its After channel immediately so retry waits do not sleep.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	c.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- now
	return ch
}

type zeroRand struct{}

func (zeroRand) Float64() float64 { return 0 }

// recordingCostSink captures the cost reports made on Done.
type recordingCostSink struct {
	mu    sync.Mutex
	calls []app.CostReport
}

func (s *recordingCostSink) Record(_ context.Context, r app.CostReport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, r)
}

func (s *recordingCostSink) snapshot() []app.CostReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]app.CostReport, len(s.calls))
	copy(out, s.calls)
	return out
}

// testEnv bundles a running bufconn server, a client, the injected fake
// provider, and the cost sink for assertions.
type testEnv struct {
	client genproto.ModelGatewayServiceClient
	fp     *llmtest.FakeProvider
	sink   *recordingCostSink
}

// newTestEnv builds an app.Service around fp (wrapped by the deterministic
// retry decorator), registers the inbound Server on a bufconn gRPC server, and
// returns a connected client. The optional reg overrides capability resolution;
// pass nil for the built-in table.
func newTestEnv(t *testing.T, fp *llmtest.FakeProvider, reg *capabilities.Registry) *testEnv {
	t.Helper()
	if reg == nil {
		reg = capabilities.NewRegistry(nil)
	}
	sink := &recordingCostSink{}
	retrying := retry.New(fp, retry.Config{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Second}, &fakeClock{}, zeroRand{})
	svc, err := app.NewService(app.Config{
		Provider:     retrying,
		Endpoint:     "anthropic",
		Capabilities: reg,
		Cost:         pricing.Cost,
		CostSink:     sink,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	grpcSrv := grpc.NewServer()
	genproto.RegisterModelGatewayServiceServer(grpcSrv, NewServer(svc))
	go func() { _ = grpcSrv.Serve(lis) }()

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		grpcSrv.Stop()
	})
	return &testEnv{
		client: genproto.NewModelGatewayServiceClient(conn),
		fp:     fp,
		sink:   sink,
	}
}

// recvAll drains a Generate server stream into a slice of gen.StreamEvents.
func recvAll(t *testing.T, stream grpc.ServerStreamingClient[genproto.StreamEvent]) []*genproto.StreamEvent {
	t.Helper()
	var out []*genproto.StreamEvent
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("stream.Recv: %v", err)
		}
		out = append(out, ev)
	}
}

// TestGenerate_StreamsMappedEvents asserts that the gen StreamEvents the server
// emits match the llm.StreamEvents the fake provider scripts: a thinking delta,
// a text delta, a tool-call delta, and a terminal Done carrying the open
// stop-reason, usage, and provider_raw.
func TestGenerate_StreamsMappedEvents(t *testing.T) {
	provRaw := []byte(`{"thinking":"sig1"}`)
	argsFrag := []byte(`{"path":"/tmp/x"}`)
	fp := llmtest.NewFakeProvider()
	fp.AddStream([]llm.StreamEvent{
		{ThinkingDelta: &llm.ThinkingDelta{Text: "reason", Signature: "sig1"}},
		{TextDelta: &llm.TextDelta{Text: "hello"}},
		{ToolCallDelta: &llm.ToolCallDelta{CallID: "c1", Name: "write", ArgsFragment: argsFrag}},
		{Done: &llm.Done{
			StopReason:    llm.StopToolUse,
			RawStopReason: "tool_use",
			Usage:         llm.Usage{InputTokens: 100, OutputTokens: 50, CacheReadTokens: 10, CacheWriteTokens: 5, ReasoningTokens: 7},
			ProviderRaw:   provRaw,
		}},
	}, nil)

	env := newTestEnv(t, fp, nil)
	stream, err := env.client.Generate(context.Background(), &genproto.GenerateRequest{
		Params: &genproto.GenerationParams{Model: "claude-3-5-sonnet-20241022"},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	events := recvAll(t, stream)
	if len(events) != 4 {
		t.Fatalf("got %d events, want 4: %+v", len(events), events)
	}

	// Event 0: thinking delta.
	if td := events[0].GetThinkingDelta(); td == nil || td.GetText() != "reason" || td.GetSignature() != "sig1" {
		t.Errorf("event 0 = %+v, want ThinkingDelta{reason,sig1}", events[0])
	}
	// Event 1: text delta.
	if td := events[1].GetTextDelta(); td == nil || td.GetText() != "hello" {
		t.Errorf("event 1 = %+v, want TextDelta{hello}", events[1])
	}
	// Event 2: tool-call delta.
	tcd := events[2].GetToolCallDelta()
	if tcd == nil || tcd.GetCallId() != "c1" || tcd.GetName() != "write" || string(tcd.GetArgsFragment()) != string(argsFrag) {
		t.Errorf("event 2 = %+v, want ToolCallDelta{c1,write,args}", events[2])
	}
	// Event 3: Done.
	done := events[3].GetDone()
	if done == nil {
		t.Fatalf("event 3 = %+v, want Done", events[3])
	}
	if done.GetStopReason() != genproto.StopReason_STOP_REASON_TOOL_USE {
		t.Errorf("Done.StopReason = %v, want TOOL_USE", done.GetStopReason())
	}
	if done.GetRawStopReason() != "tool_use" {
		t.Errorf("Done.RawStopReason = %q", done.GetRawStopReason())
	}
	if string(done.GetProviderRaw()) != string(provRaw) {
		t.Errorf("Done.ProviderRaw = %s, want %s", done.GetProviderRaw(), provRaw)
	}
	u := done.GetUsage()
	if u == nil || u.GetInputTokens() != 100 || u.GetOutputTokens() != 50 ||
		u.GetCacheReadTokens() != 10 || u.GetCacheWriteTokens() != 5 || u.GetReasoningTokens() != 7 {
		t.Errorf("Done.Usage = %+v", u)
	}
}

// TestGenerate_ComputesCostOnDone asserts cost is computed from the Done usage
// and the pricing table, reported once to the CostSink.
func TestGenerate_ComputesCostOnDone(t *testing.T) {
	const model = "claude-3-5-sonnet-20241022"
	fp := llmtest.NewFakeProvider()
	fp.AddStream([]llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: "hi"}},
		{Done: &llm.Done{StopReason: llm.StopEnd, Usage: llm.Usage{InputTokens: 1000, OutputTokens: 500}}},
	}, nil)

	env := newTestEnv(t, fp, nil)
	stream, err := env.client.Generate(context.Background(), &genproto.GenerateRequest{
		Params: &genproto.GenerationParams{Model: model},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	_ = recvAll(t, stream)

	calls := env.sink.snapshot()
	if len(calls) != 1 {
		t.Fatalf("CostSink called %d times, want 1", len(calls))
	}
	want, err := pricing.Cost(model, llm.Usage{InputTokens: 1000, OutputTokens: 500})
	if err != nil {
		t.Fatalf("pricing.Cost: %v", err)
	}
	if calls[0].CostUSD != want {
		t.Errorf("CostUSD = %v, want %v", calls[0].CostUSD, want)
	}
	if calls[0].Model != model {
		t.Errorf("Model = %q, want %q", calls[0].Model, model)
	}
}

// TestGenerate_RetryThenSuccess asserts the retry decorator re-opens the stream
// after transient provider errors so the eventual success is streamed.
func TestGenerate_RetryThenSuccess(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddStream(nil, &llm.ProviderError{Kind: llm.ErrOverloaded})
	fp.AddStream(nil, &llm.ProviderError{Kind: llm.ErrServer})
	fp.AddStream([]llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: "recovered"}},
		{Done: &llm.Done{StopReason: llm.StopEnd, Usage: llm.Usage{InputTokens: 1}}},
	}, nil)

	env := newTestEnv(t, fp, nil)
	stream, err := env.client.Generate(context.Background(), &genproto.GenerateRequest{
		Params: &genproto.GenerationParams{Model: "claude-3-haiku-20240307"},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	events := recvAll(t, stream)

	if env.fp.StreamCalls() != 3 {
		t.Errorf("provider Stream called %d times, want 3", env.fp.StreamCalls())
	}
	if len(events) != 2 || events[0].GetTextDelta().GetText() != "recovered" || events[1].GetDone() == nil {
		t.Errorf("events = %+v, want [TextDelta{recovered}, Done]", events)
	}
}

// TestGenerate_NonRetryableErrorStatus asserts a non-retryable provider error
// opening the stream surfaces as a gRPC status (INVALID_ARGUMENT for
// ErrInvalidRequest) rather than hanging or a silent empty stream.
func TestGenerate_NonRetryableErrorStatus(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddStream(nil, &llm.ProviderError{Kind: llm.ErrInvalidRequest})

	env := newTestEnv(t, fp, nil)
	stream, err := env.client.Generate(context.Background(), &genproto.GenerateRequest{
		Params: &genproto.GenerationParams{Model: "claude-3-haiku-20240307"},
	})
	if err != nil {
		t.Fatalf("Generate (open): %v", err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error from Recv, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error is not a gRPC status: %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("status code = %v, want INVALID_ARGUMENT", st.Code())
	}
}

// TestGenerate_PauseIsNonTerminal asserts STOP_REASON_PAUSE round-trips on the
// terminal Done with its provider_raw continuation blob.
func TestGenerate_PauseIsNonTerminal(t *testing.T) {
	provRaw := []byte(`{"continuation":true}`)
	fp := llmtest.NewFakeProvider()
	fp.AddStream([]llm.StreamEvent{
		{Done: &llm.Done{StopReason: llm.Pause, RawStopReason: "pause_turn", ProviderRaw: provRaw}},
	}, nil)

	env := newTestEnv(t, fp, nil)
	stream, err := env.client.Generate(context.Background(), &genproto.GenerateRequest{
		Params: &genproto.GenerationParams{Model: "claude-3-5-sonnet-20241022"},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	events := recvAll(t, stream)
	if len(events) != 1 || events[0].GetDone() == nil {
		t.Fatalf("events = %+v, want [Done]", events)
	}
	done := events[0].GetDone()
	if done.GetStopReason() != genproto.StopReason_STOP_REASON_PAUSE {
		t.Errorf("StopReason = %v, want PAUSE", done.GetStopReason())
	}
	if string(done.GetProviderRaw()) != string(provRaw) {
		t.Errorf("ProviderRaw = %s, want %s", done.GetProviderRaw(), provRaw)
	}
}

// TestGenerate_RequestMappedToProvider asserts the incoming gen GenerationParams
// are mapped to the llm.Request the provider receives (model, system, messages,
// tools, tool-choice, temperature, provider_raw).
func TestGenerate_RequestMappedToProvider(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddStream([]llm.StreamEvent{{Done: &llm.Done{StopReason: llm.StopEnd}}}, nil)

	env := newTestEnv(t, fp, nil)
	temp := 0.7
	stream, err := env.client.Generate(context.Background(), &genproto.GenerateRequest{
		Params: &genproto.GenerationParams{
			Model:  "claude-3-5-sonnet-20241022",
			System: "you are a bot",
			Messages: []*genproto.Message{
				{
					Role: genproto.Role_ROLE_USER,
					Content: []*genproto.ContentPart{
						{Part: &genproto.ContentPart_Text{Text: &genproto.TextPart{Text: "hi"}}},
					},
				},
			},
			Tools: []*genproto.ToolDefinition{
				{Name: "write", Description: "writes", JsonSchema: `{"type":"object"}`},
			},
			ToolChoice:  genproto.ToolChoice_TOOL_CHOICE_AUTO,
			MaxTokens:   256,
			Temperature: &temp,
			ProviderRaw: []byte(`{"prev":1}`),
		},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	_ = recvAll(t, stream)

	if len(env.fp.RecordedRequests) != 1 {
		t.Fatalf("recorded %d requests, want 1", len(env.fp.RecordedRequests))
	}
	req := env.fp.RecordedRequests[0]
	if req.Model != "claude-3-5-sonnet-20241022" {
		t.Errorf("Model = %q", req.Model)
	}
	if req.System != "you are a bot" {
		t.Errorf("System = %q", req.System)
	}
	if len(req.Messages) != 1 || req.Messages[0].Role != llm.RoleUser {
		t.Fatalf("Messages = %+v", req.Messages)
	}
	if len(req.Messages[0].Content) != 1 || req.Messages[0].Content[0].Text == nil ||
		req.Messages[0].Content[0].Text.Text != "hi" {
		t.Errorf("Message content = %+v", req.Messages[0].Content)
	}
	if len(req.Tools) != 1 || req.Tools[0].Name != "write" || string(req.Tools[0].JSONSchema) != `{"type":"object"}` {
		t.Errorf("Tools = %+v", req.Tools)
	}
	if req.ToolChoice != llm.ToolChoiceAuto {
		t.Errorf("ToolChoice = %q, want auto", req.ToolChoice)
	}
	if req.MaxTokens != 256 {
		t.Errorf("MaxTokens = %d, want 256", req.MaxTokens)
	}
	if req.Temperature == nil || *req.Temperature != 0.7 {
		t.Errorf("Temperature = %v, want 0.7", req.Temperature)
	}
	if string(req.ProviderRaw) != `{"prev":1}` {
		t.Errorf("ProviderRaw = %s", req.ProviderRaw)
	}
}

// TestGenerate_SpecificToolChoice asserts TOOL_CHOICE_TOOL + tool_name maps to
// the specific-tool-name form of llm.ToolChoice.
func TestGenerate_SpecificToolChoice(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddStream([]llm.StreamEvent{{Done: &llm.Done{StopReason: llm.StopEnd}}}, nil)

	env := newTestEnv(t, fp, nil)
	stream, err := env.client.Generate(context.Background(), &genproto.GenerateRequest{
		Params: &genproto.GenerationParams{
			Model:      "claude-3-5-sonnet-20241022",
			ToolChoice: genproto.ToolChoice_TOOL_CHOICE_TOOL,
			ToolName:   "write",
		},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	_ = recvAll(t, stream)

	req := env.fp.RecordedRequests[0]
	if req.ToolChoice != llm.ToolChoice("write") {
		t.Errorf("ToolChoice = %q, want specific tool %q", req.ToolChoice, "write")
	}
}

// TestCountTokens_Success asserts a successful CountTokens round-trip.
func TestCountTokens_Success(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddTokenCount(77, nil)

	env := newTestEnv(t, fp, nil)
	resp, err := env.client.CountTokens(context.Background(), &genproto.CountTokensRequest{
		Params: &genproto.GenerationParams{Model: "claude-3-5-sonnet-20241022"},
	})
	if err != nil {
		t.Fatalf("CountTokens: %v", err)
	}
	if resp.GetInputTokens() != 77 {
		t.Errorf("InputTokens = %d, want 77", resp.GetInputTokens())
	}
}

// TestCountTokens_Unimplemented asserts an ErrUnsupported from the provider maps
// to gRPC UNIMPLEMENTED (architecture §11.6).
func TestCountTokens_Unimplemented(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddTokenCount(0, &llm.ProviderError{Kind: llm.ErrUnsupported})

	env := newTestEnv(t, fp, nil)
	_, err := env.client.CountTokens(context.Background(), &genproto.CountTokensRequest{
		Params: &genproto.GenerationParams{Model: "gpt-4o"},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("not a gRPC status: %v", err)
	}
	if st.Code() != codes.Unimplemented {
		t.Errorf("code = %v, want UNIMPLEMENTED", st.Code())
	}
}

// TestGetCapabilities_MapsFlags asserts every capability flag round-trips and
// supports_server_side_tools is forced false by the gateway hard switch (§8.12).
func TestGetCapabilities_MapsFlags(t *testing.T) {
	caps := llm.Capabilities{
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           true,
		SupportsTokenCounting:      true,
		SupportsJSONSchemaStrict:   true,
		MaxOutputTokens:            8192,
	}
	reg := capabilities.NewRegistry(map[string]capabilities.EndpointOverride{
		"anthropic": {PerModel: map[string]*llm.Capabilities{"m": &caps}},
	})
	fp := llmtest.NewFakeProvider()
	env := newTestEnv(t, fp, reg)

	resp, err := env.client.GetCapabilities(context.Background(), &genproto.GetCapabilitiesRequest{Model: "m"})
	if err != nil {
		t.Fatalf("GetCapabilities: %v", err)
	}
	c := resp.GetCapabilities()
	if c == nil {
		t.Fatal("nil capabilities")
	}
	if !c.GetSupportsTools() || !c.GetSupportsParallelToolCalls() || !c.GetSupportsStreamingToolCalls() ||
		!c.GetSupportsVision() || !c.GetSupportsSystemPrompt() || !c.GetSupportsThinking() ||
		!c.GetSupportsTokenCounting() || !c.GetSupportsJsonSchemaStrict() {
		t.Errorf("capability flags not all true: %+v", c)
	}
	if c.GetMaxOutputTokens() != 8192 {
		t.Errorf("MaxOutputTokens = %d, want 8192", c.GetMaxOutputTokens())
	}
	if c.GetSupportsServerSideTools() {
		t.Error("supports_server_side_tools must be forced false in v1 (§8.12)")
	}
}

// TestGetCapabilities_ServerSideToolsAlwaysOff asserts that even if a (future)
// resolver were to report server-side tools, the gateway forces the flag off.
// We exercise it via a model whose built-in default has tools enabled.
func TestGetCapabilities_ServerSideToolsAlwaysOff(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	env := newTestEnv(t, fp, nil) // built-in table

	resp, err := env.client.GetCapabilities(context.Background(), &genproto.GetCapabilitiesRequest{
		Model: "claude-3-5-sonnet-20241022",
	})
	if err != nil {
		t.Fatalf("GetCapabilities: %v", err)
	}
	if resp.GetCapabilities().GetSupportsServerSideTools() {
		t.Error("supports_server_side_tools must be false")
	}
	if !resp.GetCapabilities().GetSupportsTools() {
		t.Error("expected built-in claude-3-5-sonnet to support tools")
	}
}
