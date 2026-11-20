package durable

import (
	"encoding/json"
	"testing"
)

// f8Order's fields aren't in alphabetical order, so its JSON and the JSON of the map
// it decodes to by default list them in a different order.
type f8Order struct {
	Zone string
	ID   int64
}

// TestServiceResult_JournalBytesAreWhatLiveDecoded pins that a live service result is
// encoded once. replayableOutcome encodes the ServiceFn's value and decodes those bytes;
// recordServiceOutcome journals the same bytes, so replay decodes what live decoded.
// They differ from the decoded value's own encoding for a struct decoded as a map (the
// keys come out sorted) and for an integer beyond 2^53 decoded as a float64 (the journal
// keeps digits the machine never saw), but replay decodes either to the value live saw,
// and only these bytes do for a decoder whose type doesn't survive its own round trip
// (see TestServiceResult_ReplayDecodesWhatLiveDecoded).
func TestServiceResult_JournalBytesAreWhatLiveDecoded(t *testing.T) {
	asOrder := func(_ string, data []byte) (any, error) { return DecodeAs[f8Order](data) }
	asInt64 := func(_ string, data []byte) (any, error) { return DecodeAs[int64](data) }
	cases := []struct {
		name     string
		result   any
		decode   EventDataDecoder[string]
		reencode bool   // whether the decoded value's own encoding matches the journal's
		recorded string // what the journal records
	}{
		{"string", "ok", decodeAny[string], true, `"ok"`},
		{"small int, generic", 42, decodeAny[string], true, `42`},
		{"int64 above 2^53, generic", int64(1<<53 + 1), decodeAny[string], false, `9007199254740993`},
		{"int64 above 2^53, typed", int64(1<<53 + 1), asInt64, true, `9007199254740993`},
		{"struct, generic", f8Order{Zone: "w", ID: 7}, decodeAny[string], false, `{"Zone":"w","ID":7}`},
		{"struct, typed", f8Order{Zone: "w", ID: 7}, asOrder, true, `{"Zone":"w","ID":7}`},
		{"map, generic", map[string]any{"b": 1.0, "a": "x"}, decodeAny[string], true, `{"a":"x","b":1}`},
		{"slice, generic", []any{"x", 2.5}, decodeAny[string], true, `["x",2.5]`},
		{"nil", nil, decodeAny[string], true, `null`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			settled := &settledJSON{}
			value, outErr, abort := replayableOutcome(tc.decode, nil, settled)("done", tc.result, nil)
			if outErr != nil || abort != nil {
				t.Fatalf("the transform returned %v, %v", outErr, abort)
			}
			entry, err := recordServiceOutcome("svc", settled.take(), nil)
			if err != nil {
				t.Fatalf("recordServiceOutcome: %v", err)
			}
			var sp serviceResultPayload
			if err = json.Unmarshal(entry.Payload, &sp); err != nil {
				t.Fatalf("decoding the entry: %v", err)
			}
			if got := string(sp.Result); got != tc.recorded {
				t.Fatalf("the journal records %s, want %s", got, tc.recorded)
			}
			reencoded, err := marshalPayload(value)
			if err != nil {
				t.Fatalf("encoding the decoded value: %v", err)
			}
			if same := string(reencoded) == string(sp.Result); same != tc.reencode {
				t.Fatalf("decoded value's encoding %s, journal %s: same = %v, want %v", reencoded, sp.Result, same, tc.reencode)
			}
			// Replay decodes the journal to the value live saw.
			replayed, _, rabort := replayableOutcome(tc.decode, nil, nil)("done", recordedResult{raw: sp.Result}, nil)
			if rabort != nil {
				t.Fatalf("replaying %s: %v", sp.Result, rabort)
			}
			if describeValue(replayed) != describeValue(value) {
				t.Fatalf("replaying %s gives %s, live saw %s", sp.Result, describeValue(replayed), describeValue(value))
			}
		})
	}
}

func describeValue(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
