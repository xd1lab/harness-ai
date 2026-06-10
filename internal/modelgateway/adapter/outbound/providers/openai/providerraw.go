package openai

import (
	"encoding/json"

	"github.com/openai/openai-go/v3/responses"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// surfaceResponses is the continuation-blob surface tag for the OpenAI Responses
// API. It guards against a blob from a different provider being echoed back here.
const surfaceResponses = "openai.responses"

// continuationState is the openai (Responses) continuation blob carried in
// [llm.Response.ProviderRaw] / [llm.Done.ProviderRaw]. Per ADR-0016 / architecture
// §11.1 the adapter is pinned to STATELESS Item-passing: instead of relying on the
// server-side previous_response_id, it carries the prior turn's output Items here
// and replays them as input items on the next request. The blob is a stable,
// self-describing projection of the output Items rather than a raw SDK type, so it
// survives SDK upgrades and round-trips byte-faithfully for deterministic replay.
type continuationState struct {
	// Surface identifies the producing adapter surface.
	Surface string `json:"surface"`
	// Items is the ordered set of output items to replay as input items.
	Items []continuationItem `json:"items,omitempty"`
}

// continuationItem is one replayable output item. Exactly one of the typed payloads
// is populated according to Type.
type continuationItem struct {
	// Type mirrors the Responses output item type: "message", "reasoning", or
	// "function_call".
	Type string `json:"type"`
	// Text is the assistant text for a message item.
	Text string `json:"text,omitempty"`
	// ID is the opaque item id (used to round-trip reasoning items so signed
	// reasoning replays unmodified).
	ID string `json:"id,omitempty"`
	// EncryptedContent carries an opaque reasoning payload to replay verbatim when
	// present.
	EncryptedContent string `json:"encrypted_content,omitempty"`
	// CallID, Name, Arguments describe a function_call item.
	CallID    string `json:"call_id,omitempty"`
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

// continuationFromResponse projects a terminal Response's output Items into the
// stateless continuation blob. It returns nil when there are no items worth
// carrying so [llm.Done.ProviderRaw] stays nil.
func continuationFromResponse(resp responses.Response) llm.ProviderRaw {
	st := continuationState{Surface: surfaceResponses}
	for _, item := range resp.Output {
		switch item.Type {
		case itemTypeMessage:
			st.Items = append(st.Items, continuationItem{
				Type: itemTypeMessage,
				ID:   item.ID,
				Text: outputMessageText(item),
			})
		case itemTypeFunctionCall:
			st.Items = append(st.Items, continuationItem{
				Type:      itemTypeFunctionCall,
				ID:        item.ID,
				CallID:    item.CallID,
				Name:      item.Name,
				Arguments: item.Arguments.OfString,
			})
		case itemTypeReasoning:
			st.Items = append(st.Items, continuationItem{
				Type: itemTypeReasoning,
				ID:   item.ID,
			})
		}
	}
	if len(st.Items) == 0 {
		return nil
	}
	b, err := json.Marshal(st)
	if err != nil {
		return nil
	}
	return b
}

// outputMessageText concatenates the text parts of an output message item.
func outputMessageText(item responses.ResponseOutputItemUnion) string {
	var b []byte
	for _, c := range item.Content {
		if c.Text != "" {
			b = append(b, c.Text...)
		} else if c.Refusal != "" {
			b = append(b, c.Refusal...)
		}
	}
	return string(b)
}

// decodeContinuation parses a continuation blob previously emitted by this surface.
// It returns ok=false (without error) when the blob is empty or belongs to another
// surface, so a fresh turn or a cross-provider blob is simply ignored rather than
// failing the request.
func decodeContinuation(raw llm.ProviderRaw) (continuationState, bool, error) {
	if len(raw) == 0 {
		return continuationState{}, false, nil
	}
	var st continuationState
	if err := json.Unmarshal(raw, &st); err != nil {
		return continuationState{}, false, err
	}
	if st.Surface != surfaceResponses {
		return continuationState{}, false, nil
	}
	return st, true, nil
}
