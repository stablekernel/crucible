package durable

import (
	"encoding/json"
	"errors"
	"testing"
)

// TestValidUTF8_MatchesAJSONRoundTrip pins validUTF8 to what a Store keeps: the string
// a JSON round trip gives back, byte for byte, for valid text, a lone bad byte, a
// truncated multi-byte sequence and an encoded surrogate.
func TestValidUTF8_MatchesAJSONRoundTrip(t *testing.T) {
	for _, in := range []string{"plain", "café   <&>", "bad \xff byte", "cut \xe2\x82", "\xed\xa0\x80 surrogate", "� kept"} {
		b, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("Marshal(%q): %v", in, err)
		}
		var want string
		if err = json.Unmarshal(b, &want); err != nil {
			t.Fatalf("Unmarshal(%s): %v", b, err)
		}
		if got := validUTF8(in); got != want {
			t.Fatalf("validUTF8(%q) = %q, want the JSON round trip's %q", in, got, want)
		}
	}
}

// panicError's Error method panics.
type panicError struct{}

func (panicError) Error() string { panic("boom") }

// TestNormalizedErrorMessage pins the shared helper encodeEventData and
// replayableOutcome use: a normalized message, or errorMessage's error naming what
// panicked.
func TestNormalizedErrorMessage(t *testing.T) {
	msg, err := normalizedErrorMessage(errors.New("bad \xff"), "error payload")
	if err != nil || msg != "bad �" {
		t.Fatalf("got %q, %v; want the normalized message", msg, err)
	}
	if _, err = normalizedErrorMessage(panicError{}, "service error"); err == nil || err.Error() != "service error panicked: boom" {
		t.Fatalf("got %v, want the panic named as a service error's", err)
	}
}
