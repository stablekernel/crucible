package state_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// This file golden-locks the kernel's emission and ordering contract (lock L9):
// the exit -> transition -> entry cascade, declaration order within each phase,
// parallel regions in declaration order, the run-to-completion microstep
// interleave, the assign fold order, and the effect emission order. A reorder
// regression — a map iteration leaking into any of these, or a phase swapped —
// surfaces as a golden mismatch here, which is what keeps a journal/replay safe.

// ordState is the state alphabet of the ordering machine. Names are chosen so the
// golden trace reads as a self-describing cascade.
type ordState int

const (
	ordStart ordState = iota // flat entry state

	ordWork    // compound; initial child is the parallel ordPar
	ordPar     // parallel state with two regions, "alpha" and "beta"
	ordAlpha1  // alpha region initial leaf
	ordAlphaF  // alpha region final leaf
	ordBeta1   // beta region initial leaf
	ordBetaF   // beta region final leaf
	ordWorkSub // a sibling leaf inside ordWork, not used by the happy path
	ordDone    // flat terminal state reached after the cross-region exit
	ordRTCMid  // eventless RTC waypoint between ordStart and ordWork
)

func (s ordState) String() string {
	switch s {
	case ordStart:
		return "Start"
	case ordWork:
		return "Work"
	case ordPar:
		return "Par"
	case ordAlpha1:
		return "Alpha1"
	case ordAlphaF:
		return "AlphaF"
	case ordBeta1:
		return "Beta1"
	case ordBetaF:
		return "BetaF"
	case ordWorkSub:
		return "WorkSub"
	case ordDone:
		return "Done"
	case ordRTCMid:
		return "RTCMid"
	default:
		return "ordState?"
	}
}

// ordEvent is the event alphabet of the ordering machine.
type ordEvent int

const (
	ordGo     ordEvent = iota // Start -> RTCMid, then RTC carries on into Work
	ordKick                   // internal (raised) event: RTCMid -> Work
	ordAlphaE                 // alpha region: Alpha1 -> AlphaF
	ordBetaE                  // beta region: Beta1 -> BetaF
	ordCross                  // cross-region: exits Par/Work, lands in Done
	ordTick                   // shared event BOTH regions handle in one macrostep
)

func (e ordEvent) String() string {
	switch e {
	case ordGo:
		return "Go"
	case ordKick:
		return "Kick"
	case ordAlphaE:
		return "AlphaE"
	case ordBetaE:
		return "BetaE"
	case ordCross:
		return "Cross"
	case ordTick:
		return "Tick"
	default:
		return "ordEvent?"
	}
}

// ordCtx is a VALUE context (not a pointer), per the determinism contract: under
// value semantics the assign fold is clean and replay-safe. log accumulates a tag
// per assign in fold order, so the committed context itself witnesses the order.
type ordCtx struct {
	Log []string
}

// tagAssign returns a reducer that appends its tag to the context log, so the fold
// order is observable in the committed context (the recorded data of the step).
func tagAssign(tag string) state.AssignFn[ordCtx] {
	return func(in state.AssignCtx[ordCtx]) ordCtx {
		next := ordCtx{Log: append(append([]string(nil), in.Entity.Log...), tag)}
		return next
	}
}

// tagEffect returns an action emitting a string effect, so the effect emission
// order is observable in FireResult.Effects and the trace.
func tagEffect(tag string) state.ActionFn[ordCtx] {
	return func(state.ActionCtx[ordCtx]) (state.Effect, error) { return tag, nil }
}

// forgeOrdering builds the machine that exercises every ordering dimension at
// once: an eventless RTC step, a raised internal event, nested compound entry, a
// parallel state with two regions firing in declaration order, multiple assigns
// and multiple effects per phase, and a cross-region exit cascade.
//
//	Start --Go--> RTCMid --(always, guarded by raised Kick)--> Work { Par[ alpha{Alpha1->AlphaF}, beta{Beta1->BetaF} ] }
//
// The Go transition raises Kick; RTCMid's eventless transition into Work is the
// RTC waypoint. Inside Work, Par's two regions each carry entry effects+assigns
// (declaration order: alpha before beta). AlphaE/BetaE advance each region; Cross
// is a cross-cutting transition on Work that exits both regions and lands in Done.
func forgeOrdering() *state.Machine[ordState, ordEvent, ordCtx] {
	return state.Forge[ordState, ordEvent, ordCtx]("ordering").
		Action("eff", tagEffect("eff")).
		Action("eff2", tagEffect("eff2")).
		Action("tickA", tagEffect("tickA")).
		Action("tickB", tagEffect("tickB")).
		Reducer("asg", tagAssign("asg")).
		Reducer("asg2", tagAssign("asg2")).
		Reducer("foldA", tagAssign("foldA")).
		Reducer("foldB", tagAssign("foldB")).

		// Flat entry state. Go emits two transition effects then folds two transition
		// assigns (effects-before-assigns within the transition phase), and raises an
		// internal Kick that the RTC loop drains in the same macrostep.
		State(ordStart).
		Transition(ordStart).On(ordGo).GoTo(ordRTCMid).
		Do("eff", state.P{}).Do("eff2", state.P{}).
		Assign("asg").Assign("asg2").
		Raise(ordKick).

		// RTC waypoint. Its eventless ("always") transition into Work fires only
		// after the raised Kick is drained (the RTC interleave: raised events first,
		// then eventless), so the golden microstep order pins raise-before-always.
		State(ordRTCMid).
		OnEntry("eff", state.P{}).
		OnEntryAssign("asg").
		Always().GoTo(ordWork).

		// Compound Work; its initial child is the parallel Par. Entering Work runs
		// Work's entry (outermost) then descends into Par and both regions.
		SuperState(ordWork).
		Initial(ordPar).
		OnEntry("eff", state.P{}).OnEntry("eff2", state.P{}).
		OnEntryAssign("asg").OnEntryAssign("asg2").
		OnExit("eff", state.P{}).
		OnExitAssign("asg").
		// Cross-cutting on Work: exits both regions (innermost-first) and Work,
		// then enters the flat Done — the cross-region exit cascade.
		Transition(ordWork).On(ordCross).GoTo(ordDone).
		Region("alpha").
		Initial(ordAlpha1).
		SubState(ordAlpha1).
		OnEntry("eff", state.P{}).
		OnEntryAssign("asg").
		OnExit("eff", state.P{}).
		OnExitAssign("asg").
		On(ordAlphaE).GoTo(ordAlphaF).
		// Tick is handled by BOTH regions in one macrostep; alpha's internal
		// self-transition emits tickA and folds foldA. Because regions broadcast in
		// declaration order (alpha before beta), the cross-region fold lands foldA
		// before foldB — the PR-4 cross-region assign-fold order, golden-locked.
		Transition(ordAlpha1).On(ordTick).GoTo(ordAlpha1).
		Do("tickA", state.P{}).Assign("foldA").
		SubState(ordAlphaF).Final().
		EndRegion().
		Region("beta").
		Initial(ordBeta1).
		SubState(ordBeta1).
		OnEntry("eff", state.P{}).
		OnEntryAssign("asg").
		OnExit("eff", state.P{}).
		OnExitAssign("asg").
		On(ordBetaE).GoTo(ordBetaF).
		Transition(ordBeta1).On(ordTick).GoTo(ordBeta1).
		Do("tickB", state.P{}).Assign("foldB").
		SubState(ordBetaF).Final().
		EndRegion().
		EndSuperState().
		State(ordDone).
		OnEntry("eff", state.P{}).
		OnEntryAssign("asg").
		Initial(ordStart).
		CurrentStateFn(func(c ordCtx) ordState {
			// Replay/derivation hook; the tests drive state explicitly via Cast.
			return ordStart
		}).
		Quench()
}

// orderTrace is the deterministic, serializable projection of a macrostep used as
// the golden: the ordered effect emission, assign fold, and entry/exit cascade,
// plus the committed context log. Every field is order-bearing; a reorder
// anywhere reorders one of these slices.
type orderTrace struct {
	Event          string   `json:"event"`
	From           string   `json:"from"`
	To             string   `json:"to"`
	Outcome        int      `json:"outcome"`
	ExitedStates   []string `json:"exitedStates,omitempty"`
	EnteredStates  []string `json:"enteredStates,omitempty"`
	EffectsEmitted []string `json:"effectsEmitted,omitempty"`
	AssignsApplied []string `json:"assignsApplied,omitempty"`
	Microsteps     []string `json:"microsteps,omitempty"`
	Effects        []string `json:"effects,omitempty"`
	ContextLog     []string `json:"contextLog,omitempty"`
}

// projectOrder folds a FireResult into the deterministic golden projection.
func projectOrder(res state.FireResult[ordState], cur ordCtx) orderTrace {
	effects := make([]string, 0, len(res.Effects))
	for _, e := range res.Effects {
		if s, ok := e.(string); ok {
			effects = append(effects, s)
		}
	}
	return orderTrace{
		Event:          res.Trace.Event,
		From:           res.Trace.FromState,
		To:             res.Trace.MatchedAt,
		Outcome:        int(res.Trace.Outcome),
		ExitedStates:   res.Trace.ExitedStates,
		EnteredStates:  res.Trace.EnteredStates,
		EffectsEmitted: res.Trace.EffectsEmitted,
		AssignsApplied: res.Trace.AssignsApplied,
		Microsteps:     res.Trace.Microsteps,
		Effects:        effects,
		ContextLog:     cur.Log,
	}
}

// runOrderingScenario drives the full ordering scenario from a fresh instance and
// returns the per-step deterministic projections. It is a pure replay: the same
// inputs always produce the same projection sequence (this is exactly the L9
// guarantee under test).
func runOrderingScenario(t *testing.T) []orderTrace {
	t.Helper()
	m := forgeOrdering()
	inst := m.Cast(ordCtx{}, state.WithInitialState[ordState](ordStart), state.WithFullTrace[ordState]())

	events := []ordEvent{ordGo, ordAlphaE, ordBetaE, ordCross}
	steps := make([]orderTrace, 0, len(events))
	for _, ev := range events {
		res := inst.Fire(context.Background(), ev)
		if res.Err != nil {
			t.Fatalf("Fire(%v) from %v: %v", ev, inst.Current(), res.Err)
		}
		steps = append(steps, projectOrder(res, inst.Entity()))
	}
	return steps
}

// TestGoldenEmissionOrder pins the full emission/ordering contract of a macrostep
// sequence against a checked-in golden. It is the §7 block-merge gate: a reorder
// of any emission/fold/transition/effect dimension fails this assertion. Run with
// -update-golden to refresh after an intended ordering change.
func TestGoldenEmissionOrder(t *testing.T) {
	steps := runOrderingScenario(t)
	assertGolden(t, filepath.Join("testdata", "order", "emission.json"), steps)
}

// TestGoldenCrossRegionFold pins the cross-region assign fold (the PR-4 concern):
// a SINGLE Tick event both regions handle in one macrostep. Regions broadcast in
// declaration order — alpha before beta — so the macrostep folds foldA before
// foldB and emits tickA before tickB. The committed context log witnesses the
// fold order directly. A region-order regression (a map leaking into region
// iteration) reorders EffectsEmitted / AssignsApplied / ContextLog and fails here.
func TestGoldenCrossRegionFold(t *testing.T) {
	m := forgeOrdering()
	inst := m.Cast(ordCtx{}, state.WithInitialState[ordState](ordStart), state.WithFullTrace[ordState]())
	// Drive into the parallel configuration, then fire the shared Tick.
	for _, ev := range []ordEvent{ordGo, ordTick} {
		res := inst.Fire(context.Background(), ev)
		if res.Err != nil {
			t.Fatalf("Fire(%v) from %v: %v", ev, inst.Current(), res.Err)
		}
		if ev == ordTick {
			fold := projectOrder(res, inst.Entity())
			assertGolden(t, filepath.Join("testdata", "order", "cross_region_fold.json"), fold)
		}
	}
}

// TestGoldenEmissionOrder_Stable proves the ordering is genuinely deterministic
// and not an accident of one map-iteration seed: it replays the whole scenario N
// times and asserts every replay yields byte-identical projections. Residual
// map-order nondeterminism in emission/fold/transition order surfaces here as a
// failure, not a flake. Combined with the -count stress on the Golden tests, this
// is the L9 stability proof.
func TestGoldenEmissionOrder_Stable(t *testing.T) {
	const replays = 50
	want, err := json.Marshal(runOrderingScenario(t))
	if err != nil {
		t.Fatalf("marshal baseline: %v", err)
	}
	for r := 0; r < replays; r++ {
		got, err := json.Marshal(runOrderingScenario(t))
		if err != nil {
			t.Fatalf("marshal replay %d: %v", r, err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("replay %d diverged from baseline — emission order is nondeterministic\n want=%s\n  got=%s", r, want, got)
		}
	}
}

// assertGolden marshals v to indented JSON and diffs it against the committed
// golden file, reusing the -update-golden flag defined in golden_test.go.
func assertGolden(t *testing.T, path string, v any) {
	t.Helper()
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("marshal golden: %v", err)
	}
	raw = append(raw, '\n')

	if *updateGolden {
		if mkErr := os.MkdirAll(filepath.Dir(path), 0o755); mkErr != nil {
			t.Fatalf("mkdir golden dir: %v", mkErr)
		}
		if wErr := os.WriteFile(path, raw, 0o644); wErr != nil {
			t.Fatalf("write golden: %v", wErr)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (run -update-golden to create): %v", path, err)
	}
	if !bytes.Equal(want, raw) {
		t.Errorf("golden mismatch for %s; run with -update-golden if intended\n want=%s\n  got=%s", path, want, raw)
	}
}
