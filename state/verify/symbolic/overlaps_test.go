package symbolic_test

import (
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/verify/symbolic"
)

// overlapMachine builds a one-source machine whose two transitions on "go" are
// guarded by the supplied Core expressions, with the ctx schema attached.
func overlapMachine(t *testing.T, ga, gb state.GuardNode[string]) *state.Machine[string, string, ctx] {
	t.Helper()
	return state.Forge[string, string, ctx]("m").
		WithContextSchema(state.SchemaOf[ctx]()).
		State("s").
		Transition("s").On("go").GoTo("a").WhenExpr(ga).
		Transition("s").On("go").GoTo("b").WhenExpr(gb).
		State("a").
		State("b").
		Initial("s").
		Quench()
}

func TestOverlaps_DisjointGuardsAreClean(t *testing.T) {
	m := overlapMachine(t,
		f("status").Eq(state.Str[string]("paid")),
		f("status").Eq(state.Str[string]("open")))
	got, err := symbolic.Overlaps(m)
	if err != nil {
		t.Fatalf("Overlaps: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Overlaps = %+v, want none (guards are disjoint)", got)
	}
}

func TestOverlaps_OverlappingGuardsAreReported(t *testing.T) {
	m := overlapMachine(t,
		f("total").Gt(state.Float[string](5)),
		f("total").Gt(state.Float[string](10)))
	got, err := symbolic.Overlaps(m)
	if err != nil {
		t.Fatalf("Overlaps: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Overlaps = %+v, want one overlap (total>5 and total>10 both hold at 20)", got)
	}
	o := got[0]
	if o.From != "s" || o.On != "go" {
		t.Fatalf("overlap = %+v, want From=s On=go", o)
	}
	if o.ToA != "a" || o.ToB != "b" {
		t.Fatalf("overlap targets = %v/%v, want a/b", o.ToA, o.ToB)
	}
}

// TestOverlaps_DeterministicOrder builds a state with several distinct overlap
// groups (different events) and asserts repeated scans return byte-identical
// ordered results. Without a deterministic group iteration order the overlaps
// would shuffle between runs and break golden assertions.
func TestOverlaps_DeterministicOrder(t *testing.T) {
	build := func() *state.Machine[string, string, ctx] {
		return state.Forge[string, string, ctx]("multi").
			WithContextSchema(state.SchemaOf[ctx]()).
			Guard("g", func(state.GuardCtx[ctx]) bool { return true }).
			State("s").
			// Three distinct events, each with two opaque-guarded competing edges.
			Transition("s").On("e1").GoTo("a1").WhenExpr(state.Guard[string]("g")).
			Transition("s").On("e1").GoTo("b1").WhenExpr(state.Guard[string]("g")).
			Transition("s").On("e2").GoTo("a2").WhenExpr(state.Guard[string]("g")).
			Transition("s").On("e2").GoTo("b2").WhenExpr(state.Guard[string]("g")).
			Transition("s").On("e3").GoTo("a3").WhenExpr(state.Guard[string]("g")).
			Transition("s").On("e3").GoTo("b3").WhenExpr(state.Guard[string]("g")).
			State("a1").State("b1").State("a2").State("b2").State("a3").State("b3").
			Initial("s").
			Quench()
	}

	first, err := symbolic.Overlaps(build())
	if err != nil {
		t.Fatalf("Overlaps: %v", err)
	}
	if len(first) != 3 {
		t.Fatalf("Overlaps = %+v, want three groups", first)
	}
	// Repeated scans of freshly built (structurally identical) machines must agree
	// on order, run after run.
	for i := 0; i < 50; i++ {
		got, err := symbolic.Overlaps(build())
		if err != nil {
			t.Fatalf("Overlaps[%d]: %v", i, err)
		}
		if len(got) != len(first) {
			t.Fatalf("Overlaps[%d] length = %d, want %d", i, len(got), len(first))
		}
		for j := range got {
			if got[j] != first[j] {
				t.Fatalf("Overlaps[%d][%d] = %+v, want %+v (nondeterministic order)", i, j, got[j], first[j])
			}
		}
	}
}

func TestOverlaps_OpaqueGuardsAreConservativelyReported(t *testing.T) {
	// Named-ref guards are opaque to the analyzer, so it cannot prove them disjoint
	// and reports the pair — a false positive is the safe direction for
	// nondeterminism detection.
	def := state.Forge[string, string, ctx]("m").
		WithContextSchema(state.SchemaOf[ctx]()).
		Guard("ga", func(state.GuardCtx[ctx]) bool { return true }).
		Guard("gb", func(state.GuardCtx[ctx]) bool { return true }).
		State("s").
		Transition("s").On("go").GoTo("a").WhenExpr(state.Guard[string]("ga")).
		Transition("s").On("go").GoTo("b").WhenExpr(state.Guard[string]("gb")).
		State("a").
		State("b").
		Initial("s").
		Quench()

	got, err := symbolic.Overlaps(def)
	if err != nil {
		t.Fatalf("Overlaps: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Overlaps = %+v, want one (opaque guards not provably disjoint)", got)
	}
}
