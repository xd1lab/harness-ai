package anthropic

import (
	"encoding/json"
	"testing"

	sdk "github.com/anthropics/anthropic-sdk-go"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// mustEvent constructs a synthetic Anthropic stream event by unmarshaling JSON
// into the SDK union type — exactly the path the SDK takes over the wire — so the
// normalizer is exercised against faithful provider events with no network.
func mustEvent(t *testing.T, raw string) sdk.MessageStreamEventUnion {
	t.Helper()
	var ev sdk.MessageStreamEventUnion
	if err := json.Unmarshal([]byte(raw), &ev); err != nil {
		t.Fatalf("unmarshal synthetic event %q: %v", raw, err)
	}
	return ev
}

// runNormalizer feeds a sequence of synthetic event JSON strings through a fresh
// normalizer and returns all emitted events.
func runNormalizer(t *testing.T, rawEvents []string) []llm.StreamEvent {
	t.Helper()
	n := newStreamNormalizer()
	var out []llm.StreamEvent
	for _, raw := range rawEvents {
		evs, err := n.next(mustEvent(t, raw))
		if err != nil {
			t.Fatalf("normalizer.next(%s): unexpected error %v", raw, err)
		}
		out = append(out, evs...)
	}
	return out
}

// TestNormalizer_TextStream golden-tests the canonical text-only SSE sequence,
// including a text_delta split mid-word, asserting the assembled text and the
// terminal Done shape.
func TestNormalizer_TextStream(t *testing.T) {
	events := runNormalizer(t, []string{
		`{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-4-8","content":[],"stop_reason":null,"usage":{"input_tokens":10,"output_tokens":1}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hel"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"lo"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":10,"output_tokens":5}}`,
		`{"type":"message_stop"}`,
	})

	// Expect: two TextDeltas, then Done.
	wantText := []string{"Hel", "lo"}
	var gotText []string
	var done *llm.Done
	for _, ev := range events {
		switch {
		case ev.TextDelta != nil:
			gotText = append(gotText, ev.TextDelta.Text)
		case ev.Done != nil:
			done = ev.Done
		default:
			t.Fatalf("unexpected event variant: %+v", ev)
		}
	}
	if len(gotText) != len(wantText) {
		t.Fatalf("text deltas = %v, want %v", gotText, wantText)
	}
	for i := range wantText {
		if gotText[i] != wantText[i] {
			t.Errorf("text delta %d = %q, want %q", i, gotText[i], wantText[i])
		}
	}
	if done == nil {
		t.Fatal("no Done event emitted")
	}
	if done.StopReason != llm.StopEnd {
		t.Errorf("stop reason = %q, want %q", done.StopReason, llm.StopEnd)
	}
	if done.RawStopReason != "end_turn" {
		t.Errorf("raw stop reason = %q, want end_turn", done.RawStopReason)
	}
	if done.Usage.InputTokens != 10 || done.Usage.OutputTokens != 5 {
		t.Errorf("usage = %+v, want input=10 output=5", done.Usage)
	}
}

// TestNormalizer_InputJSONDeltaAccumulation golden-tests a tool_use block whose
// arguments arrive as multiple input_json_delta fragments. It asserts a name-only
// ToolCallDelta is emitted at content_block_start, each fragment is emitted with
// the resolved CallID, concatenating the fragments yields valid JSON, and the
// terminal Done maps tool_use -> StopToolUse with the assembled call in
// ProviderRaw.
func TestNormalizer_InputJSONDeltaAccumulation(t *testing.T) {
	events := runNormalizer(t, []string{
		`{"type":"message_start","message":{"id":"msg_2","type":"message","role":"assistant","model":"claude-opus-4-8","content":[],"stop_reason":null,"usage":{"input_tokens":20,"output_tokens":1}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_abc","name":"get_weather","input":{}}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"city\":"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":" \"Paris\"}"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"input_tokens":20,"output_tokens":15}}`,
		`{"type":"message_stop"}`,
	})

	var nameDelta *llm.ToolCallDelta
	var argFrags []string
	var callIDs []string
	var done *llm.Done
	for _, ev := range events {
		switch {
		case ev.ToolCallDelta != nil:
			d := ev.ToolCallDelta
			if d.Name != "" {
				nameDelta = d
			}
			if len(d.ArgsFragment) > 0 {
				argFrags = append(argFrags, string(d.ArgsFragment))
				callIDs = append(callIDs, d.CallID)
			}
		case ev.Done != nil:
			done = ev.Done
		default:
			t.Fatalf("unexpected event variant: %+v", ev)
		}
	}

	if nameDelta == nil {
		t.Fatal("no name-only ToolCallDelta emitted at content_block_start")
	}
	if nameDelta.Name != "get_weather" || nameDelta.CallID != "toolu_abc" {
		t.Errorf("name delta = %+v, want name=get_weather callID=toolu_abc", nameDelta)
	}
	// Each fragment delta must carry the resolved CallID.
	for i, id := range callIDs {
		if id != "toolu_abc" {
			t.Errorf("arg fragment %d CallID = %q, want toolu_abc", i, id)
		}
	}
	// Concatenated fragments parse to the full object.
	assembled := ""
	for _, f := range argFrags {
		assembled += f
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(assembled), &args); err != nil {
		t.Fatalf("assembled args %q not valid JSON: %v", assembled, err)
	}
	if args["city"] != "Paris" {
		t.Errorf("assembled args city = %v, want Paris", args["city"])
	}

	if done == nil {
		t.Fatal("no Done event")
	}
	if done.StopReason != llm.StopToolUse {
		t.Errorf("stop reason = %q, want %q", done.StopReason, llm.StopToolUse)
	}
	// ProviderRaw must carry the tool_use block with the assembled input.
	assertContinuationToolCall(t, done.ProviderRaw, "toolu_abc", "get_weather", "Paris")
}

// TestNormalizer_ThinkingAndSignature golden-tests a thinking block with both
// thinking_delta and signature_delta, asserting they surface as ThinkingDeltas
// and the final signature is preserved in the Done.ProviderRaw continuation blob.
func TestNormalizer_ThinkingAndSignature(t *testing.T) {
	events := runNormalizer(t, []string{
		`{"type":"message_start","message":{"id":"msg_3","type":"message","role":"assistant","model":"claude-opus-4-8","content":[],"stop_reason":null,"usage":{"input_tokens":5,"output_tokens":1}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Let me think"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sigABC=="}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Answer"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":5,"output_tokens":20}}`,
		`{"type":"message_stop"}`,
	})

	var thinkingText, sigText string
	var done *llm.Done
	for _, ev := range events {
		switch {
		case ev.ThinkingDelta != nil:
			thinkingText += ev.ThinkingDelta.Text
			sigText += ev.ThinkingDelta.Signature
		case ev.TextDelta != nil:
			// allowed
		case ev.Done != nil:
			done = ev.Done
		default:
			t.Fatalf("unexpected event variant: %+v", ev)
		}
	}
	if thinkingText != "Let me think" {
		t.Errorf("thinking text = %q, want %q", thinkingText, "Let me think")
	}
	if sigText != "sigABC==" {
		t.Errorf("signature = %q, want sigABC==", sigText)
	}
	if done == nil {
		t.Fatal("no Done event")
	}
	// The continuation blob must carry the thinking block with its signature.
	assertContinuationSignature(t, done.ProviderRaw, "sigABC==")
}

// TestNormalizer_PauseTurn golden-tests pause_turn -> non-terminal Pause with the
// continuation state captured in ProviderRaw (architecture §11.1).
func TestNormalizer_PauseTurn(t *testing.T) {
	events := runNormalizer(t, []string{
		`{"type":"message_start","message":{"id":"msg_4","type":"message","role":"assistant","model":"claude-opus-4-8","content":[],"stop_reason":null,"usage":{"input_tokens":30,"output_tokens":1}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"working"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"pause_turn","stop_sequence":null},"usage":{"input_tokens":30,"output_tokens":40}}`,
		`{"type":"message_stop"}`,
	})

	var done *llm.Done
	for _, ev := range events {
		if ev.Done != nil {
			done = ev.Done
		}
	}
	if done == nil {
		t.Fatal("no terminal event")
	}
	if done.StopReason != llm.Pause {
		t.Errorf("stop reason = %q, want %q (Pause)", done.StopReason, llm.Pause)
	}
	if done.StopReason.IsTerminal() {
		t.Error("Pause must report IsTerminal()==false")
	}
	if done.RawStopReason != "pause_turn" {
		t.Errorf("raw stop reason = %q, want pause_turn", done.RawStopReason)
	}
	if len(done.ProviderRaw) == 0 {
		t.Error("Pause must carry continuation state in ProviderRaw")
	}
	// The continuation blob must round-trip and contain the produced text.
	var blob continuationBlob
	if err := json.Unmarshal(done.ProviderRaw, &blob); err != nil {
		t.Fatalf("ProviderRaw not valid continuation blob: %v", err)
	}
	if blob.Role != "assistant" || len(blob.Content) == 0 || blob.Content[0].Text != "working" {
		t.Errorf("continuation blob = %+v, want assistant text 'working'", blob)
	}
}

// TestNormalizer_UnknownStopReason asserts an unrecognized provider stop reason
// passes through as StopOther with the raw string preserved.
func TestNormalizer_UnknownStopReason(t *testing.T) {
	events := runNormalizer(t, []string{
		`{"type":"message_start","message":{"id":"msg_5","type":"message","role":"assistant","model":"claude-opus-4-8","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"some_future_reason","stop_sequence":null},"usage":{"input_tokens":1,"output_tokens":2}}`,
		`{"type":"message_stop"}`,
	})
	var done *llm.Done
	for _, ev := range events {
		if ev.Done != nil {
			done = ev.Done
		}
	}
	if done == nil {
		t.Fatal("no Done event")
	}
	if done.StopReason != llm.StopOther {
		t.Errorf("stop reason = %q, want %q (StopOther)", done.StopReason, llm.StopOther)
	}
	if done.RawStopReason != "some_future_reason" {
		t.Errorf("raw stop reason = %q, want some_future_reason", done.RawStopReason)
	}
}

// TestNormalizer_DuplicateMessageStop asserts a duplicate message_stop does not
// emit a second Done (idempotent terminal).
func TestNormalizer_DuplicateMessageStop(t *testing.T) {
	events := runNormalizer(t, []string{
		`{"type":"message_start","message":{"id":"msg_6","type":"message","role":"assistant","model":"claude-opus-4-8","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":1,"output_tokens":1}}`,
		`{"type":"message_stop"}`,
		`{"type":"message_stop"}`,
	})
	doneCount := 0
	for _, ev := range events {
		if ev.Done != nil {
			doneCount++
		}
	}
	if doneCount != 1 {
		t.Errorf("Done events = %d, want exactly 1", doneCount)
	}
}

// TestNormalizer_ParallelToolCalls asserts two tool_use blocks at distinct
// indices keep their input_json_delta fragments attributed to the correct CallID.
func TestNormalizer_ParallelToolCalls(t *testing.T) {
	events := runNormalizer(t, []string{
		`{"type":"message_start","message":{"id":"msg_7","type":"message","role":"assistant","model":"claude-opus-4-8","content":[],"stop_reason":null,"usage":{"input_tokens":10,"output_tokens":1}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_A","name":"alpha","input":{}}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"a\":1}"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_B","name":"beta","input":{}}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"b\":2}"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"input_tokens":10,"output_tokens":12}}`,
		`{"type":"message_stop"}`,
	})

	fragByCall := map[string]string{}
	for _, ev := range events {
		if d := ev.ToolCallDelta; d != nil && len(d.ArgsFragment) > 0 {
			fragByCall[d.CallID] += string(d.ArgsFragment)
		}
	}
	if fragByCall["toolu_A"] != `{"a":1}` {
		t.Errorf("toolu_A fragments = %q, want {\"a\":1}", fragByCall["toolu_A"])
	}
	if fragByCall["toolu_B"] != `{"b":2}` {
		t.Errorf("toolu_B fragments = %q, want {\"b\":2}", fragByCall["toolu_B"])
	}
}

// --- continuation-blob assertions -----------------------------------------

func assertContinuationToolCall(t *testing.T, raw llm.ProviderRaw, wantID, wantName, wantCity string) {
	t.Helper()
	if len(raw) == 0 {
		t.Fatal("ProviderRaw is empty, want tool-call continuation")
	}
	var blob continuationBlob
	if err := json.Unmarshal(raw, &blob); err != nil {
		t.Fatalf("ProviderRaw not a continuation blob: %v", err)
	}
	for _, b := range blob.Content {
		if b.Type == "tool_use" && b.ID == wantID {
			if b.Name != wantName {
				t.Errorf("continuation tool name = %q, want %q", b.Name, wantName)
			}
			var args map[string]any
			if err := json.Unmarshal(b.Input, &args); err != nil {
				t.Fatalf("continuation tool input not valid JSON: %v", err)
			}
			if args["city"] != wantCity {
				t.Errorf("continuation tool input city = %v, want %q", args["city"], wantCity)
			}
			return
		}
	}
	t.Errorf("continuation blob has no tool_use block with id %q: %+v", wantID, blob)
}

func assertContinuationSignature(t *testing.T, raw llm.ProviderRaw, wantSig string) {
	t.Helper()
	if len(raw) == 0 {
		t.Fatal("ProviderRaw is empty, want thinking continuation")
	}
	var blob continuationBlob
	if err := json.Unmarshal(raw, &blob); err != nil {
		t.Fatalf("ProviderRaw not a continuation blob: %v", err)
	}
	for _, b := range blob.Content {
		if b.Type == "thinking" && b.Signature == wantSig {
			return
		}
	}
	t.Errorf("continuation blob has no thinking block with signature %q: %+v", wantSig, blob)
}
