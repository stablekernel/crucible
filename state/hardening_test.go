package state_test

import (
	"context"
	"errors"
	"go/build"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestImportGraph_StdlibOnly enforces the kernel's stdlib-only boundary: the
// state package must import nothing outside the Go standard library.
func TestImportGraph_StdlibOnly(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatalf("ImportDir: %v", err)
	}
	for _, imp := range pkg.Imports {
		// Standard-library import paths never contain a dot in their first
		// segment (no domain); third-party always does (e.g. github.com/...).
		first := imp
		if idx := strings.IndexByte(imp, '/'); idx >= 0 {
			first = imp[:idx]
		}
		if strings.Contains(first, ".") {
			t.Errorf("non-stdlib import in kernel: %q", imp)
		}
	}
}

// TestFireEach_FansAcrossInstances asserts FireEach drives one event across an
// explicit set of instances, preserving per-instance attribution.
func TestFireEach_FansAcrossInstances(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	a := m.Cast(&Document{Status: Draft})
	b := m.Cast(&Document{Status: Draft})
	results := state.FireEach(context.Background(), []*state.Instance[DocState, DocEvent, *Document]{a, b}, Submit)
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("instance %d: err = %v", i, r.Err)
		}
		if r.NewState != Submitted {
			t.Fatalf("instance %d: state = %v, want Submitted", i, r.NewState)
		}
	}
}

// TestFireEach_StopsAtFirstError asserts the default fail-fast batch semantics:
// firing an event invalid for the instances stops at the first error.
func TestFireEach_StopsAtFirstError(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	a := m.Cast(&Document{Status: Published}) // Submit is invalid from Published
	b := m.Cast(&Document{Status: Draft})
	results := state.FireEach(context.Background(), []*state.Instance[DocState, DocEvent, *Document]{a, b}, Submit)
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1 (stop-at-first-error)", len(results))
	}
	if results[0].Err == nil {
		t.Fatal("expected error on the first instance")
	}
}

// TestFireEach_CollectAll asserts CollectAll runs every instance.
func TestFireEach_CollectAll(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	a := m.Cast(&Document{Status: Published})
	b := m.Cast(&Document{Status: Draft})
	results := state.FireEach(
		context.Background(),
		[]*state.Instance[DocState, DocEvent, *Document]{a, b},
		Submit,
		state.CollectAll(),
	)
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2 (collect-all)", len(results))
	}
}

// TestRoundTrip_ByteIdentity asserts the IR survives a JSON round-trip
// byte-for-byte: ToJSON -> LoadFromJSON -> Provide -> Quench -> ToJSON yields
// identical bytes (structure preserved losslessly).
func TestRoundTrip_ByteIdentity(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	b1, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	ir, err := state.LoadFromJSON[DocState, DocEvent, *Document](b1)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	m2 := ir.Provide(docRegistry()).Quench()
	b2, err := m2.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON (2): %v", err)
	}
	if string(b1) != string(b2) {
		t.Fatalf("round-trip not byte-identical:\n %s\n %s", b1, b2)
	}
}

// TestHistory_RecordsEveryFire asserts an instance accumulates a trace per Fire
// when unbounded history retention is enabled.
func TestHistory_RecordsEveryFire(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	inst := m.Cast(&Document{Status: Draft}, state.WithUnboundedHistory[DocState]())
	inst.Fire(context.Background(), Submit)
	inst.Fire(context.Background(), Archive) // Submitted->Archive is a valid transition
	if got := len(inst.History()); got != 2 {
		t.Fatalf("History len = %d, want 2", got)
	}
}

// TestSelfTransition asserts a self-targeting transition keeps the state and
// runs its effect.
func TestSelfTransition(t *testing.T) {
	type s = int
	type e = int
	const idle s = 0
	const ping e = 0

	fired := false
	m := state.Forge[s, e, any]("self").
		Action("noop", func(ctx state.ActionCtx[any]) (state.Effect, error) {
			fired = true
			return struct{}{}, nil
		}).
		State(idle).
		Initial(idle).
		CurrentStateFn(func(any) s { return idle }).
		Transition(idle).On(ping).GoTo(idle).Do("noop").
		Quench()

	res := m.Cast(nil).Fire(context.Background(), ping)
	if res.Err != nil {
		t.Fatalf("err = %v", res.Err)
	}
	if res.NewState != idle {
		t.Fatalf("state = %v, want idle", res.NewState)
	}
	if !fired {
		t.Fatal("effect did not fire")
	}
}

// TestActionError_AdvancesStateRecordsEffectError asserts the locked decision:
// state advances before actions run; a failing action records OutcomeEffectError
// and the typed *ErrActionFailed, with the state already advanced.
func TestActionError_AdvancesStateRecordsEffectError(t *testing.T) {
	type s = int
	type e = int
	const a s = 0
	const b s = 1
	const go0 e = 0

	boom := errors.New("boom")
	m := state.Forge[s, e, any]("act").
		Action("fail", func(ctx state.ActionCtx[any]) (state.Effect, error) {
			return nil, boom
		}).
		State(a).
		State(b).
		Initial(a).
		CurrentStateFn(func(any) s { return a }).
		Transition(a).On(go0).GoTo(b).Do("fail").
		Quench()

	res := m.Cast(nil).Fire(context.Background(), go0)
	var af *state.ErrActionFailed
	if !errors.As(res.Err, &af) {
		t.Fatalf("err = %v, want *ErrActionFailed", res.Err)
	}
	if !errors.Is(res.Err, boom) {
		t.Fatalf("err does not unwrap to boom: %v", res.Err)
	}
	if res.NewState != b {
		t.Fatalf("state = %v, want b (advanced before action)", res.NewState)
	}
	if res.Trace.Outcome != state.OutcomeEffectError {
		t.Fatalf("outcome = %v, want OutcomeEffectError", res.Trace.Outcome)
	}
}

// TestProvide_UnboundActionRef asserts an unbound action ref also panics with
// *ErrUnboundRef (kind "action").
func TestProvide_UnboundActionRef(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on unbound action ref")
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("recovered non-error: %v", r)
		}
		var ub *state.ErrUnboundRef
		if !errors.As(err, &ub) {
			t.Fatalf("err = %v, want *ErrUnboundRef", err)
		}
		if ub.Kind != "action" {
			t.Fatalf("Kind = %q, want action", ub.Kind)
		}
	}()
	type s = int
	type e = int
	_ = state.Forge[s, e, any]("x").
		State(0).Initial(0).CurrentStateFn(func(any) s { return 0 }).
		Transition(0).On(0).GoTo(0).Do("missing").
		Quench()
}

// TestGuardPanic_RecoveredAsTypedError asserts a panicking guard is recovered
// into *ErrGuardPanic rather than crashing the process.
func TestGuardPanic_RecoveredAsTypedError(t *testing.T) {
	type s = int
	type e = int
	m := state.Forge[s, e, any]("g").
		Guard("boom", func(ctx state.GuardCtx[any]) bool { panic("nope") }).
		State(0).State(1).Initial(0).CurrentStateFn(func(any) s { return 0 }).
		Transition(0).On(0).GoTo(1).When("boom").
		Quench()

	res := m.Cast(nil).Fire(context.Background(), 0)
	var gp *state.ErrGuardPanic
	if !errors.As(res.Err, &gp) {
		t.Fatalf("err = %v, want *ErrGuardPanic", res.Err)
	}
	if res.Trace.Outcome != state.OutcomeGuardPanic {
		t.Fatalf("outcome = %v, want OutcomeGuardPanic", res.Trace.Outcome)
	}
	if res.NewState != 0 {
		t.Fatalf("state = %v, want unchanged 0", res.NewState)
	}
}

// TestQuench_PanicsOnUndeclaredTarget asserts Quench panics for a transition
// pointing at an undeclared state.
func TestQuench_PanicsOnUndeclaredTarget(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on undeclared transition target")
		}
	}()
	type s = int
	type e = int
	_ = state.Forge[s, e, any]("u").
		State(0).Initial(0).CurrentStateFn(func(any) s { return 0 }).
		Transition(0).On(0).GoTo(99). // 99 never declared
		Quench()
}

// TestTemper_NonFailing asserts Temper returns diagnostics without panicking
// for a malformed (missing initial) definition.
func TestTemper_NonFailing(t *testing.T) {
	type s = int
	type e = int
	b := state.Forge[s, e, any]("t").State(0) // no Initial, no CurrentStateFn
	diags := b.Temper()
	if len(diags) == 0 {
		t.Fatal("expected diagnostics for a missing-initial definition")
	}
}
