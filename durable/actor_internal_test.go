package durable

import (
	"encoding/json"
	"testing"
)

// TestReplayActorData covers the three reconstruction branches and the
// surfaced-error path: an onError payload becomes an error, a valid onDone payload
// decodes to its value, an absent payload is nil, and a corrupt done-data payload
// is reported rather than silently dropped (so replay fails loudly instead of
// re-firing the parent with nil data it never saw live).
func TestReplayActorData(t *testing.T) {
	errMsg := "boom"

	t.Run("error", func(t *testing.T) {
		got, err := replayActorData(actorMessagePayload{Error: &errMsg})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		gotErr, ok := got.(error)
		if !ok || gotErr.Error() != errMsg {
			t.Fatalf("want reconstructed error %q, got %v", errMsg, got)
		}
	})

	t.Run("valid data", func(t *testing.T) {
		got, err := replayActorData(actorMessagePayload{Data: json.RawMessage(`{"k":1}`)})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		m, ok := got.(map[string]any)
		if !ok || m["k"] != float64(1) {
			t.Fatalf("want decoded map, got %#v", got)
		}
	})

	t.Run("no data", func(t *testing.T) {
		got, err := replayActorData(actorMessagePayload{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Fatalf("want nil for absent payload, got %#v", got)
		}
	})

	t.Run("corrupt data surfaces error", func(t *testing.T) {
		got, err := replayActorData(actorMessagePayload{Data: json.RawMessage(`{not json`)})
		if err == nil {
			t.Fatal("want an error for undecodable done-data, got nil")
		}
		if got != nil {
			t.Fatalf("want nil value alongside the error, got %#v", got)
		}
	})
}
