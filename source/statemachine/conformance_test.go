// SPDX-License-Identifier: Apache-2.0

package statemachine_test

import (
	"testing"

	"github.com/stablekernel/crucible/source/statemachine"
)

func TestEventAlphabet(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	got := statemachine.EventAlphabet(m)
	// The turnstile declares transitions on coin and push only; maintenance is in
	// the event type but on no transition, so it is NOT in the alphabet.
	want := []turnstileEvent{coin, push}
	if len(got) != len(want) {
		t.Fatalf("alphabet = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("alphabet[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestCheckEvents(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()

	tests := []struct {
		name            string
		accepted        []turnstileEvent
		wantExhaustive  bool
		wantMissing     []turnstileEvent
		wantUnreachable []turnstileEvent
	}{
		{
			name:           "exhaustive",
			accepted:       []turnstileEvent{coin, push},
			wantExhaustive: true,
		},
		{
			name:        "missing an alphabet event",
			accepted:    []turnstileEvent{coin},
			wantMissing: []turnstileEvent{push},
		},
		{
			name:            "unreachable accepted event",
			accepted:        []turnstileEvent{coin, push, maintenance},
			wantUnreachable: []turnstileEvent{maintenance},
		},
		{
			name:            "both gaps",
			accepted:        []turnstileEvent{coin, maintenance},
			wantMissing:     []turnstileEvent{push},
			wantUnreachable: []turnstileEvent{maintenance},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := statemachine.CheckEvents(m, tc.accepted)
			if c.Exhaustive() != tc.wantExhaustive {
				t.Fatalf("Exhaustive() = %v, want %v (missing=%v unreachable=%v)",
					c.Exhaustive(), tc.wantExhaustive, c.Missing, c.Unreachable)
			}
			if !eqEvents(c.Missing, tc.wantMissing) {
				t.Fatalf("Missing = %v, want %v", c.Missing, tc.wantMissing)
			}
			if !eqEvents(c.Unreachable, tc.wantUnreachable) {
				t.Fatalf("Unreachable = %v, want %v", c.Unreachable, tc.wantUnreachable)
			}
			if tc.wantExhaustive && c.Err() != nil {
				t.Fatalf("Err() = %v, want nil", c.Err())
			}
			if !tc.wantExhaustive && c.Err() == nil {
				t.Fatal("Err() = nil, want non-nil")
			}
		})
	}
}

func eqEvents(a, b []turnstileEvent) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
