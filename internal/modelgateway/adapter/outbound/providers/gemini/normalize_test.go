package gemini

import (
	"encoding/json"
	"reflect"
	"testing"

	"google.golang.org/genai"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// TestMapStopReason is the stop-reason table required by the task: STOP->StopEnd,
// MAX_TOKENS->StopMaxTokens, SAFETY/RECITATION->StopContentFilter, everything else
// -> StopOther carrying the verbatim provider string.
func TestMapStopReason(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		in      genai.FinishReason
		want    llm.StopReason
		wantRaw string
	}{
		{"stop", genai.FinishReasonStop, llm.StopEnd, "STOP"},
		{"max_tokens", genai.FinishReasonMaxTokens, llm.StopMaxTokens, "MAX_TOKENS"},
		{"safety", genai.FinishReasonSafety, llm.StopContentFilter, "SAFETY"},
		{"recitation", genai.FinishReasonRecitation, llm.StopContentFilter, "RECITATION"},
		{"other", genai.FinishReasonOther, llm.StopOther, "OTHER"},
		{"blocklist", genai.FinishReasonBlocklist, llm.StopOther, "BLOCKLIST"},
		{"prohibited", genai.FinishReasonProhibitedContent, llm.StopOther, "PROHIBITED_CONTENT"},
		{"malformed_fn", genai.FinishReasonMalformedFunctionCall, llm.StopOther, "MALFORMED_FUNCTION_CALL"},
		{"language", genai.FinishReasonLanguage, llm.StopOther, "LANGUAGE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, raw := mapStopReason(tc.in)
			if got != tc.want {
				t.Errorf("mapStopReason(%q) reason = %q, want %q", tc.in, got, tc.want)
			}
			if raw != tc.wantRaw {
				t.Errorf("mapStopReason(%q) raw = %q, want %q", tc.in, raw, tc.wantRaw)
			}
		})
	}
}

// TestNormalizeUsage asserts usageMetadata -> llm.Usage normalization, including the
// cached-content split and the thoughts (reasoning) token carry-through. Gemini's
// promptTokenCount is the TOTAL effective prompt including cached tokens, so the
// normalizer subtracts the cached count out of InputTokens (which excludes cache
// reads by contract).
func TestNormalizeUsage(t *testing.T) {
	t.Parallel()

	t.Run("nil is zero", func(t *testing.T) {
		t.Parallel()
		if got := normalizeUsage(nil); got != (llm.Usage{}) {
			t.Errorf("normalizeUsage(nil) = %+v, want zero", got)
		}
	})

	t.Run("full split", func(t *testing.T) {
		t.Parallel()
		md := &genai.GenerateContentResponseUsageMetadata{
			PromptTokenCount:        100, // includes the 30 cached tokens
			CachedContentTokenCount: 30,
			CandidatesTokenCount:    40,
			ThoughtsTokenCount:      12,
			TotalTokenCount:         152,
		}
		got := normalizeUsage(md)
		want := llm.Usage{
			InputTokens:     70, // 100 prompt - 30 cached
			OutputTokens:    40,
			CacheReadTokens: 30,
			ReasoningTokens: 12,
		}
		if got != want {
			t.Errorf("normalizeUsage = %+v, want %+v", got, want)
		}
	})

	t.Run("no cache", func(t *testing.T) {
		t.Parallel()
		md := &genai.GenerateContentResponseUsageMetadata{
			PromptTokenCount:     50,
			CandidatesTokenCount: 25,
		}
		got := normalizeUsage(md)
		want := llm.Usage{InputTokens: 50, OutputTokens: 25}
		if got != want {
			t.Errorf("normalizeUsage = %+v, want %+v", got, want)
		}
	})
}

// textResp builds a synthetic streamed chunk carrying a single text part.
func textResp(text string) *genai.GenerateContentResponse {
	return &genai.GenerateContentResponse{
		Candidates: []*genai.Candidate{{
			Content: &genai.Content{
				Role:  genai.RoleModel,
				Parts: []*genai.Part{{Text: text}},
			},
		}},
	}
}

// funcResp builds a synthetic streamed chunk carrying a single functionCall part.
func funcResp(name string, args map[string]any) *genai.GenerateContentResponse {
	return &genai.GenerateContentResponse{
		Candidates: []*genai.Candidate{{
			Content: &genai.Content{
				Role: genai.RoleModel,
				Parts: []*genai.Part{{
					FunctionCall: &genai.FunctionCall{Name: name, Args: args},
				}},
			},
		}},
	}
}

// finalResp builds a synthetic terminal chunk carrying a finishReason and usage.
func finalResp(reason genai.FinishReason, usage *genai.GenerateContentResponseUsageMetadata) *genai.GenerateContentResponse {
	return &genai.GenerateContentResponse{
		Candidates:    []*genai.Candidate{{FinishReason: reason}},
		UsageMetadata: usage,
	}
}

// TestNormalizerGolden drives a representative synthetic Gemini stream — text parts,
// a functionCall, then a terminal finishReason + usageMetadata — and asserts the full
// emitted []llm.StreamEvent matches the golden expectation. This is the network-free
// golden test the task requires: synthetic genai responses in, normalized events out.
func TestNormalizerGolden(t *testing.T) {
	t.Parallel()

	usage := &genai.GenerateContentResponseUsageMetadata{
		PromptTokenCount:     11,
		CandidatesTokenCount: 7,
		TotalTokenCount:      18,
	}

	chunks := []*genai.GenerateContentResponse{
		textResp("Hel"),
		textResp("lo"),
		funcResp("get_weather", map[string]any{"city": "Paris"}),
		finalResp(genai.FinishReasonStop, usage),
	}

	got := runNormalizer(t, chunks)

	wantArgs, _ := json.Marshal(map[string]any{"city": "Paris"})
	want := []llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: "Hel"}},
		{TextDelta: &llm.TextDelta{Text: "lo"}},
		{ToolCallDelta: &llm.ToolCallDelta{
			CallID:       "get_weather-0",
			Name:         "get_weather",
			ArgsFragment: json.RawMessage(wantArgs),
		}},
		{Done: &llm.Done{
			StopReason:    llm.StopEnd,
			RawStopReason: "STOP",
			Usage:         llm.Usage{InputTokens: 11, OutputTokens: 7},
		}},
	}

	assertEventsMatch(t, got, want)
}

// TestNormalizerTextOnly asserts a pure-text stream with a terminal chunk produces
// text deltas then exactly one Done.
func TestNormalizerTextOnly(t *testing.T) {
	t.Parallel()
	chunks := []*genai.GenerateContentResponse{
		textResp("hi"),
		finalResp(genai.FinishReasonStop, nil),
	}
	got := runNormalizer(t, chunks)
	want := []llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: "hi"}},
		{Done: &llm.Done{StopReason: llm.StopEnd, RawStopReason: "STOP"}},
	}
	assertEventsMatch(t, got, want)
}

// TestNormalizerSafetyStop asserts SAFETY finishReason maps to StopContentFilter and
// is still emitted as exactly one terminal Done.
func TestNormalizerSafetyStop(t *testing.T) {
	t.Parallel()
	chunks := []*genai.GenerateContentResponse{
		finalResp(genai.FinishReasonSafety, nil),
	}
	got := runNormalizer(t, chunks)
	if len(got) != 1 || got[0].Done == nil {
		t.Fatalf("want exactly one Done event, got %d events: %+v", len(got), got)
	}
	if got[0].Done.StopReason != llm.StopContentFilter || got[0].Done.RawStopReason != "SAFETY" {
		t.Errorf("Done = %+v, want StopContentFilter/SAFETY", got[0].Done)
	}
}

// TestNormalizerMissingFinishReason asserts a stream that ends WITHOUT any explicit
// finishReason still terminates with a single defensive Done (StopOther) so the loop
// never hangs waiting for a terminal event.
func TestNormalizerMissingFinishReason(t *testing.T) {
	t.Parallel()
	chunks := []*genai.GenerateContentResponse{
		textResp("partial"),
	}
	got := runNormalizer(t, chunks)
	if len(got) != 2 {
		t.Fatalf("want text delta + defensive Done, got %d events: %+v", len(got), got)
	}
	if got[1].Done == nil {
		t.Fatalf("last event is not Done: %+v", got[1])
	}
	if got[1].Done.StopReason != llm.StopOther {
		t.Errorf("defensive Done StopReason = %q, want StopOther", got[1].Done.StopReason)
	}
}

// TestNormalizerProviderRaw asserts the terminal Done carries a non-nil ProviderRaw
// continuation blob whenever the model produced content (so a turn can be replayed /
// continued byte-faithfully, per architecture §11.1).
func TestNormalizerProviderRaw(t *testing.T) {
	t.Parallel()
	chunks := []*genai.GenerateContentResponse{
		funcResp("do_it", map[string]any{"k": "v"}),
		finalResp(genai.FinishReasonStop, nil),
	}
	got := runNormalizer(t, chunks)
	done := got[len(got)-1].Done
	if done == nil {
		t.Fatalf("last event is not Done: %+v", got[len(got)-1])
	}
	if len(done.ProviderRaw) == 0 {
		t.Errorf("Done.ProviderRaw is empty; want continuation state captured")
	}
	// It must be valid JSON.
	if !json.Valid(done.ProviderRaw) {
		t.Errorf("Done.ProviderRaw is not valid JSON: %s", done.ProviderRaw)
	}
}

// runNormalizer feeds chunks through the stream normalizer and returns every emitted
// event, flushing any trailing terminal event.
func runNormalizer(t *testing.T, chunks []*genai.GenerateContentResponse) []llm.StreamEvent {
	t.Helper()
	n := newStreamNormalizer()
	var out []llm.StreamEvent
	for _, c := range chunks {
		evs, err := n.next(c)
		if err != nil {
			t.Fatalf("normalizer.next returned error: %v", err)
		}
		out = append(out, evs...)
	}
	out = append(out, n.finish()...)
	return out
}

// assertEventsMatch compares two []llm.StreamEvent slices field-by-field with a
// readable diff on mismatch. Done.ProviderRaw is opaque, provider-scoped continuation
// state and is NOT pinned byte-for-byte by these golden tests (it is asserted
// separately in TestNormalizerProviderRaw); it is zeroed before comparison so the
// golden focuses on the normalized stop reason, usage, and the delta events.
func assertEventsMatch(t *testing.T, got, want []llm.StreamEvent) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("event count = %d, want %d\n got: %s\nwant: %s", len(got), len(want), dump(got), dump(want))
	}
	for i := range want {
		g := got[i]
		if g.Done != nil {
			d := *g.Done
			d.ProviderRaw = nil
			g.Done = &d
		}
		if !reflect.DeepEqual(g, want[i]) {
			t.Errorf("event[%d] mismatch\n got: %s\nwant: %s", i, dump(got[i:i+1]), dump(want[i:i+1]))
		}
	}
}

func dump(evs []llm.StreamEvent) string {
	b, _ := json.Marshal(evs)
	return string(b)
}
