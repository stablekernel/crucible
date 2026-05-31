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
