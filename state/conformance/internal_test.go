package conformance

import (
	"math"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestDiffTraces_Branches exercises the ordered/payload-aware branches of
// diffTraces directly, including the length-mismatch short circuit and the
// per-step effect-name and payload divergences (C1).
func TestDiffTraces_Branches(t *testing.T) {
	base := func(effects, payloads []string, outcome, to string) Trace {
		return Trace{Steps: []TraceStep{{
			Outcome: outcome, ToState: to,
			EffectsEmitted: effects, EffectPayloads: payloads,
		}}}
	}

	t.Run("length mismatch short-circuits", func(t *testing.T) {
		ref := Trace{Steps: []TraceStep{{}, {}}}
		sub := Trace{Steps: []TraceStep{{}}}
		ms := diffTraces("s", ref, sub)
		if len(ms) != 1 || ms[0].Field != "trace.len" {
			t.Fatalf("want a single trace.len mismatch, got %+v", ms)
		}
	})

	t.Run("identical steps yield no mismatch", func(t *testing.T) {
		ref := base([]string{"a", "b"}, []string{"a=1", "b=2"}, "Success", "X")
		if ms := diffTraces("s", ref, ref); len(ms) != 0 {
			t.Fatalf("identical traces must not diverge, got %+v", ms)
		}
	})

	t.Run("reordered effect names diverge", func(t *testing.T) {
		ref := base([]string{"a", "b"}, []string{"a=1", "b=2"}, "Success", "X")
		sub := base([]string{"b", "a"}, []string{"b=2", "a=1"}, "Success", "X")
		ms := diffTraces("s", ref, sub)
		if len(ms) != 1 || ms[0].Field != "trace.step[0].effects" {
			t.Fatalf("want a step[0].effects mismatch, got %+v", ms)
		}
	})

	t.Run("payload diverges with names equal", func(t *testing.T) {
		ref := base([]string{"a"}, []string{"a=1"}, "Success", "X")
		sub := base([]string{"a"}, []string{"a=2"}, "Success", "X")
		ms := diffTraces("s", ref, sub)
		if len(ms) != 1 || ms[0].Field != "trace.step[0].effects.payload" {
			t.Fatalf("want a step[0].effects.payload mismatch, got %+v", ms)
		}
	})

	t.Run("outcome and toState diverge", func(t *testing.T) {
		ref := base(nil, nil, "Success", "X")
		sub := base(nil, nil, "GuardFailed", "Y")
		ms := diffTraces("s", ref, sub)
		if len(ms) != 2 {
			t.Fatalf("want outcome+toState mismatches, got %+v", ms)
		}
	})
}

// TestRenderPayload covers the nil branch and a value rendering.
func TestRenderPayload(t *testing.T) {
	if got := renderPayload(nil); got != "<nil>" {
		t.Fatalf("renderPayload(nil) = %q, want <nil>", got)
	}
	if got := renderPayload(struct{ N int }{N: 3}); got == "" {
		t.Fatalf("renderPayload(struct) must render a non-empty value, got %q", got)
	}
}

// TestRenderContext covers nil, the JSON path, and the %+v fallback for a value
// that cannot be JSON-marshaled (a function).
func TestRenderContext(t *testing.T) {
	if got := renderContext(nil); got != "" {
		t.Fatalf("renderContext(nil) = %q, want empty", got)
	}
	if got := renderContext(map[string]int{"a": 1}); got != `{"a":1}` {
		t.Fatalf("renderContext(map) = %q, want JSON", got)
	}
	// json.Marshal of a NaN errors, forcing the fallback rendering.
	if got := renderContext(math.NaN()); got == "" {
		t.Fatalf("renderContext(NaN) must fall back to a non-empty rendering, got %q", got)
	}
	// A func-bearing struct cannot be JSON-marshaled, forcing the %+v fallback.
	type unmarshalable struct{ Fn func() }
	if got := renderContext(unmarshalable{}); got == "" {
		t.Fatalf("renderContext(func-bearing struct) must fall back, got %q", got)
	}
	// A nil pointer through the fallback path renders <nil>.
	if got := renderContext((*unmarshalable)(nil)); got != "null" && got != "<nil>" {
		t.Fatalf("renderContext(nil ptr) = %q, want null or <nil>", got)
	}
}

// TestOutcomeName_AllKernelOutcomes asserts outcomeName renders every kernel
// Outcome by a distinct stable name, so a conformance trace never collapses two
// different failure classes onto the same label (which would make a regression
// compare equal). An out-of-range value falls back to "Unknown".
func TestOutcomeName_AllKernelOutcomes(t *testing.T) {
	cases := []struct {
		outcome state.Outcome
		want    string
	}{
		{state.OutcomeSuccess, "Success"},
		{state.OutcomeInvalidTransition, "InvalidTransition"},
		{state.OutcomeGuardFailed, "GuardFailed"},
		{state.OutcomeGuardPanic, "GuardPanic"},
		{state.OutcomePolicyDenied, "PolicyDenied"},
		{state.OutcomeEffectError, "EffectError"},
		{state.OutcomeAssignFailed, "AssignFailed"},
		{state.Outcome(-1), "Unknown"},
		{state.Outcome(9999), "Unknown"},
	}

	seen := map[string]state.Outcome{}
	for _, tc := range cases {
		got := outcomeName(tc.outcome)
		if got != tc.want {
			t.Fatalf("outcomeName(%d) = %q, want %q", tc.outcome, got, tc.want)
		}
		// Every named (non-Unknown) outcome must be distinct from the others, so a
		// trace diff distinguishes the failure classes.
		if tc.want != "Unknown" {
			if prev, dup := seen[got]; dup {
				t.Fatalf("outcomeName collision: %d and %d both render %q", prev, tc.outcome, got)
			}
			seen[got] = tc.outcome
		}
	}
}
