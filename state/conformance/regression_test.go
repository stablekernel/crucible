package conformance_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/conformance"
)

// These tests pin the v1.0-freeze conformance correctness contract (C1–C3): the
// harness must FAIL a reordered effect sequence, a changed payload, a per-step
// trace divergence, and a context divergence. Under the previous order-insensitive,
// payload-stripped comparison every one of these regressions slipped through.

// twoEffectState/Event reuse the doc fixture's state/event spaces but exercise a
// transition that emits two DISTINCT effects in a fixed order, so a reordering is
// observable.

// alpha and beta are two distinct effect payloads emitted from one transition.
type (
	alpha struct{ tag string }
	beta  struct{ tag string }
)

func emitAlpha(state.ActionCtx[*document]) (state.Effect, error) { return alpha{tag: "a"}, nil }
func emitBeta(state.ActionCtx[*document]) (state.Effect, error)  { return beta{tag: "b"}, nil }

// emitParam emits an alpha whose payload carries the transition's "n" param, so a
// changed param value yields a changed payload (the timer-duration analog).
func emitParam(ctx state.ActionCtx[*document]) (state.Effect, error) {
	n, _ := ctx.Params["n"].(int)
	return alpha{tag: strings.Repeat("x", n)}, nil
}

// buildOrderedMachine fires submit and emits alpha THEN beta.
func buildOrderedMachine(t *testing.T) *state.Machine[docState, docEvent, *document] {
	t.Helper()
	return state.Forge[docState, docEvent, *document]("ordered").
		Action("a", emitAlpha).Action("b", emitBeta).
		State(draft).State(submitted).
		Initial(draft).
		CurrentStateFn(func(d *document) docState { return d.status }).
		Transition(draft).On(submit).GoTo(submitted).Do("a").Do("b").
		Quench()
}

// buildReorderedMachine fires submit and emits beta THEN alpha — a pure reordering
// of the same two effects.
func buildReorderedMachine(t *testing.T) *state.Machine[docState, docEvent, *document] {
	t.Helper()
	return state.Forge[docState, docEvent, *document]("ordered").
		Action("a", emitAlpha).Action("b", emitBeta).
		State(draft).State(submitted).
		Initial(draft).
		CurrentStateFn(func(d *document) docState { return d.status }).
		Transition(draft).On(submit).GoTo(submitted).Do("b").Do("a").
		Quench()
}

func orderedScenario() conformance.Scenario {
	return conformance.Scenario{
		MachineID: "ordered", Name: "submit", InitialState: "Draft",
		Events: []conformance.Event{{Event: "Submit"}},
	}
}

// C1: a reordered effect sequence must be reported as a mismatch by the oracle.
func TestCompareMachines_ReorderedEffectsFail(t *testing.T) {
	ref := buildOrderedMachine(t)
	sub := buildReorderedMachine(t)
	scs := []conformance.Scenario{orderedScenario()}
	err := conformance.CompareMachines(ref, sub, scs, docCodec(), draft, func() *document { return newDoc() })
	if err == nil {
		t.Fatal("reordered effect emission must be reported as a conformance mismatch")
	}
	var ce *conformance.ErrConformance
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v, want *ErrConformance", err)
	}
	// The divergence must name the effects field (ordered) or a per-step effects
	// field — not silently pass as an equivalent set.
	if !strings.Contains(err.Error(), "effects") {
		t.Fatalf("mismatch should cite effects ordering, got: %v", err)
	}
}

// C1: an ordered EffectsEmitted assertion must FAIL when the run reorders the
// emitted effects relative to the asserted order.
func TestRunAgainst_EffectsAssertionIsOrderSensitive(t *testing.T) {
	m := buildReorderedMachine(t) // emits b, a
	sc := orderedScenario()
	sc.Assertions = []conformance.Assertion{
		// Asserting a-then-b against a machine that emits b-then-a must fail now.
		{Type: conformance.AssertEffectsEmitted, Expected: []string{"a", "b"}},
	}
	res := conformance.RunAgainst(m, sc, newDoc(), docCodec(), draft)
	if res.Passed() {
		t.Fatal("an out-of-order EffectsEmitted assertion must fail under order-sensitive comparison")
	}

	// Positive control: the correct order passes.
	sc.Assertions = []conformance.Assertion{
		{Type: conformance.AssertEffectsEmitted, Expected: []string{"b", "a"}},
	}
	res = conformance.RunAgainst(m, sc, newDoc(), docCodec(), draft)
	if !res.Passed() {
		t.Fatalf("the in-order assertion must pass: %+v", res.Assertions)
	}
}

// C1: a changed payload (same effect name/order) must be caught by the
// payload-aware oracle comparison even though ref-name order is identical.
func TestCompareMachines_ChangedPayloadFails(t *testing.T) {
	build := func(n int) *state.Machine[docState, docEvent, *document] {
		return state.Forge[docState, docEvent, *document]("payload").
			Action("a", emitParam).
			State(draft).State(submitted).
			Initial(draft).
			CurrentStateFn(func(d *document) docState { return d.status }).
			Transition(draft).On(submit).GoTo(submitted).Do("a", state.P{"n": n}).
			Quench()
	}
	ref := build(1)
	sub := build(5) // same effect name "a", different payload value
	scs := []conformance.Scenario{{
		MachineID: "payload", Name: "submit", InitialState: "Draft",
		Events: []conformance.Event{{Event: "Submit"}},
	}}
	err := conformance.CompareMachines(ref, sub, scs, docCodec(), draft, func() *document { return newDoc() })
	if err == nil {
		t.Fatal("a changed effect payload must be reported even when the effect name is unchanged")
	}
	if !strings.Contains(err.Error(), "payload") {
		t.Fatalf("mismatch should cite the payload divergence, got: %v", err)
	}
}

// C1: the AssertEffectsPayloads assertion compares ordered, payload-aware
// renderings, so a changed payload fails the scenario directly.
func TestRunAgainst_PayloadAssertion(t *testing.T) {
	m := buildOrderedMachine(t)
	sc := orderedScenario()
	res := conformance.RunAgainst(m, sc, newDoc(), docCodec(), draft)
	// Capture the actual rendering the harness produced and assert it back.
	if len(res.EffectDetails) != 2 {
		t.Fatalf("want 2 effect details, got %v", res.EffectDetails)
	}
	sc.Assertions = []conformance.Assertion{
		{Type: conformance.AssertEffectsPayloads, Expected: res.EffectDetails},
	}
	if res2 := conformance.RunAgainst(m, sc, newDoc(), docCodec(), draft); !res2.Passed() {
		t.Fatalf("payload assertion against its own rendering must pass: %+v", res2.Assertions)
	}

	// A wrong payload rendering must fail.
	sc.Assertions = []conformance.Assertion{
		{Type: conformance.AssertEffectsPayloads, Expected: []string{"a=wrong", "b=wrong"}},
	}
	if res3 := conformance.RunAgainst(m, sc, newDoc(), docCodec(), draft); res3.Passed() {
		t.Fatal("a wrong payload assertion must fail")
	}
}

// C2/C3: the trace and final context are captured on the result and serialize, so
// a divergence in either is diffable.
func TestRunAgainst_CapturesTraceAndContext(t *testing.T) {
	m := buildOrderedMachine(t)
	res := conformance.RunAgainst(m, orderedScenario(), newDoc(), docCodec(), draft)
	if len(res.Trace.Steps) != 1 {
		t.Fatalf("want 1 trace step, got %d", len(res.Trace.Steps))
	}
	step := res.Trace.Steps[0]
	if len(step.EffectPayloads) != 2 {
		t.Fatalf("trace step must capture both effect payloads, got %v", step.EffectPayloads)
	}
	if res.FinalContext == "" || res.Trace.FinalContext == "" {
		t.Fatalf("final context must be captured on both result and trace: %q / %q",
			res.FinalContext, res.Trace.FinalContext)
	}
}

// C1: a per-step effect REORDERING is surfaced by diffTraces even when the
// whole-run effect multiset is identical — the trace-level guard, exercising the
// previously-uncovered ordered/payload paths of diffTraces.
func TestDiffTraces_PerStepReorderAndPayload(t *testing.T) {
	ref := buildOrderedMachine(t)   // a, b
	sub := buildReorderedMachine(t) // b, a
	scs := []conformance.Scenario{orderedScenario()}
	// Ignore the whole-run effects compare so ONLY the trace path can flag it; the
	// per-step ordered comparison must still catch the reorder.
	err := conformance.CompareMachines(ref, sub, scs, docCodec(), draft,
		func() *document { return newDoc() }, conformance.IgnoreEffects())
	if err == nil {
		t.Fatal("per-step trace comparison must catch a reordered emission even with whole-run effects ignored")
	}
	if !strings.Contains(err.Error(), "trace.step") {
		t.Fatalf("expected a per-step trace mismatch, got: %v", err)
	}
}

// C4: the additive snapshot seam captures a post-run snapshot without changing
// RunAgainst's existing call sites.
func TestRunAgainst_SnapshotSinkSeam(t *testing.T) {
	m := buildDocMachine()
	var snapped bool
	res := conformance.RunAgainst(
		m, conformance.Scenario{
			MachineID: "document", InitialState: "Draft",
			Events: []conformance.Event{{Event: "Submit"}},
		}, newDoc(), docCodec(), draft,
		conformance.WithSnapshotSink(func(snap state.Snapshot[docState, docEvent, *document]) {
			snapped = true
			if snap.Current != submitted {
				t.Errorf("snapshot current = %v, want Submitted", snap.Current)
			}
		}),
	)
	if !snapped {
		t.Fatal("snapshot sink must be invoked after the run")
	}
	if res.FinalState != submitted {
		t.Fatalf("final state = %v, want Submitted", res.FinalState)
	}
}
