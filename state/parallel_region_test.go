package state_test

import (
	"context"
	"sort"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// prCtx is a trivial context for the parallel-region regression tests.
type prCtx struct{ N int }

// sortedConfig returns the instance configuration sorted for order-independent
// comparison of the orthogonal leaf set.
func sortedConfig(inst interface{ Configuration() []string }) []string {
	cfg := append([]string(nil), inst.Configuration()...)
	sort.Strings(cfg)
	return cfg
}

// TestRegionTransition_RaiseIsDelivered proves that a Raise declared on a
// region-internal transition enqueues its internal event so the run-to-completion
// loop delivers it to a sibling region (probe T2).
//
// Machine: parallel "par" with regions a (a1->a2 on "tick", raising "boost") and
// b (b1->b2 on "boost"). After Fire("tick") the config must be {a2, b2}: the
// region transition's Raise must reach region b. The pre-fix kernel dropped the
// Raise (applyRegionTransition never called enqueueRaised), leaving {a2, b1}.
func TestRegionTransition_RaiseIsDelivered(t *testing.T) {
	m := state.Forge[string, string, prCtx]("pr-raise").
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("a").Initial("a1").SubState("a1").SubState("a2").EndRegion().
		Region("b").Initial("b1").SubState("b1").SubState("b2").EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("a1").On("tick").GoTo("a2").Raise("boost").
		Transition("b1").On("boost").GoTo("b2").
		Quench()

	inst := m.Cast(prCtx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel: %v", res.Err)
	}

	res := inst.Fire(ctx, "tick")
	if res.Err != nil {
		t.Fatalf("tick errored: %v", res.Err)
	}

	got := sortedConfig(inst)
	want := []string{"a2", "b2"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("config after tick = %v, want %v (region Raise must reach sibling region b)", inst.Configuration(), want)
	}
}
