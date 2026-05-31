package symbolic_test

import (
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/verify/symbolic"
)

type ctx struct {
	Total    float64 `json:"total"`
	Quantity int     `json:"quantity"`
	Status   string  `json:"status"`
	Rush     bool    `json:"rush"`
}

func schema() state.ContextSchema { return state.SchemaOf[ctx]() }

// f is a shorthand for a context field ref of the test's state type.
func f(path string) state.FieldRef[string] { return state.Field[string](path) }

func TestSatisfiable(t *testing.T) {
	rich := state.Guard[string]("rich")
	rich.Kind = state.GuardKindRich

	cases := []struct {
		name string
		g    state.GuardNode[string]
		sat  bool // expected Satisfiable
	}{
		// numeric intervals
		{"numeric contradiction", state.And(f("total").Gt(state.Float[string](50)), f("total").Lt(state.Float[string](10))), false},
		{"numeric satisfiable", state.And(f("total").Gt(state.Float[string](10)), f("total").Lt(state.Float[string](50))), true},
		{"two distinct eq", state.And(f("quantity").Eq(state.Int[string](3)), f("quantity").Eq(state.Int[string](5))), false},
		{"closed point ok", state.And(f("total").Ge(state.Float[string](5)), f("total").Le(state.Float[string](5))), true},
		{"open point empty", state.And(f("total").Gt(state.Float[string](5)), f("total").Lt(state.Float[string](5))), false},
		{"half-open point empty", state.And(f("total").Ge(state.Float[string](5)), f("total").Lt(state.Float[string](5))), false},
		{"point excluded by ne", state.And(f("quantity").Ge(state.Int[string](5)), f("quantity").Le(state.Int[string](5)), f("quantity").Ne(state.Int[string](5))), false},

		// discrete value sets
		{"eq vs ne same value", state.And(f("status").Eq(state.Str[string]("paid")), f("status").Ne(state.Str[string]("paid"))), false},
		{"eq plus unrelated", state.And(f("status").Eq(state.Str[string]("paid")), f("total").Gt(state.Float[string](10))), true},
		{"eq outside in-set", state.And(f("status").In(state.Str[string]("paid"), state.Str[string]("open")), f("status").Eq(state.Str[string]("closed"))), false},
		{"in-set fully excluded", state.And(f("status").In(state.Str[string]("paid"), state.Str[string]("open")), f("status").Ne(state.Str[string]("paid")), f("status").Ne(state.Str[string]("open"))), false},
		{"bool eq satisfiable", f("rush").Eq(state.Bool[string](true)), true},

		// boolean structure
		{"or of unsat and sat", state.Or(state.And(f("total").Gt(state.Float[string](50)), f("total").Lt(state.Float[string](10))), f("status").Eq(state.Str[string]("paid"))), true},
		{"or of two unsat", state.Or(state.And(f("total").Gt(state.Float[string](50)), f("total").Lt(state.Float[string](10))), state.And(f("quantity").Eq(state.Int[string](1)), f("quantity").Eq(state.Int[string](2)))), false},
		{"not gt is le", state.Not(f("total").Gt(state.Float[string](5))), true},
		{"field and its negation", state.And(f("total").Gt(state.Float[string](5)), state.Not(f("total").Gt(state.Float[string](5)))), false},

		// opaque / stateIn (conservative)
		{"opaque leaf unknown", state.Guard[string]("ext"), true},
		{"opaque self-contradiction", state.And(state.Guard[string]("ext"), state.Not(state.Guard[string]("ext"))), false},
		{"rich guard unknown", rich, true},
		{"stateIn satisfiable", state.StateIn[string]("active"), true},
		{"stateIn self-contradiction", state.And(state.StateIn[string]("active"), state.Not(state.StateIn[string]("active"))), false},
		{"two states parallel", state.And(state.StateIn[string]("a"), state.StateIn[string]("b")), true},
	}

	sc := schema()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := symbolic.Satisfiable(tc.g, sc)
			if got != tc.sat {
				t.Fatalf("Satisfiable = %v, want %v", got, tc.sat)
			}
			if c := symbolic.Contradiction(tc.g, sc); c != !tc.sat {
				t.Fatalf("Contradiction = %v, want %v", c, !tc.sat)
			}
		})
	}
}

// litNode and fieldNode build raw operand nodes for IR forms the Go DSL never emits
// (a literal on the left, or two literals) but a JSON-authored machine can.
func litNode(v float64) state.GuardNode[string] {
	return state.GuardNode[string]{Op: state.GuardLit, Lit: &state.Literal{Type: state.FloatParam, Value: v}}
}

func fieldNode(path string) state.GuardNode[string] {
	return state.GuardNode[string]{Op: state.GuardField, Path: path}
}

func cmp(op state.GuardOp, a, b state.GuardNode[string]) state.GuardNode[string] {
	return state.GuardNode[string]{Op: op, Children: []state.GuardNode[string]{a, b}}
}

func TestSatisfiable_RawIRForms(t *testing.T) {
	sc := schema()
	cases := []struct {
		name string
		g    state.GuardNode[string]
		sat  bool
	}{
		// literal on the left: 5 < total ≡ total > 5; conjoined with total < 3 → empty.
		{"literal-left contradiction", state.And(cmp(state.GuardLt, litNode(5), fieldNode("total")), f("total").Lt(state.Float[string](3))), false},
		{"literal-left satisfiable", cmp(state.GuardLt, litNode(5), fieldNode("total")), true},
		// both-literal constant comparisons.
		{"const false", cmp(state.GuardLt, litNode(5), litNode(3)), false},
		{"const true", cmp(state.GuardLt, litNode(3), litNode(5)), true},
		// negation across the comparison ops.
		{"not lt is ge", state.And(f("total").Lt(state.Float[string](10)), state.Not(f("total").Lt(state.Float[string](10)))), false},
		{"not le is gt", state.And(f("total").Le(state.Float[string](10)), state.Not(f("total").Le(state.Float[string](10)))), false},
		{"not ge is lt", state.And(f("total").Ge(state.Float[string](10)), state.Not(f("total").Ge(state.Float[string](10)))), false},
		{"not eq is ne", state.And(f("status").Eq(state.Str[string]("paid")), state.Not(f("status").Eq(state.Str[string]("paid")))), false},
		{"not ne is eq", state.And(f("status").Ne(state.Str[string]("paid")), state.Not(f("status").Ne(state.Str[string]("paid")))), false},
		// two `in` sets intersect; the required value falls outside the intersection.
		{"in intersection excludes eq", state.And(
			f("status").In(state.Str[string]("paid"), state.Str[string]("open"), state.Str[string]("closed")),
			f("status").In(state.Str[string]("open"), state.Str[string]("closed")),
			f("status").Eq(state.Str[string]("paid")),
		), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := symbolic.Satisfiable(tc.g, sc); got != tc.sat {
				t.Fatalf("Satisfiable = %v, want %v", got, tc.sat)
			}
		})
	}
}

func TestDisjoint(t *testing.T) {
	sc := schema()
	cases := []struct {
		name     string
		a, b     state.GuardNode[string]
		disjoint bool
	}{
		{"distinct status values", f("status").Eq(state.Str[string]("paid")), f("status").Eq(state.Str[string]("open")), true},
		{"disjoint numeric ranges", f("total").Lt(state.Float[string](10)), f("total").Gt(state.Float[string](20)), true},
		{"overlapping numeric ranges", f("total").Gt(state.Float[string](5)), f("total").Gt(state.Float[string](10)), false},
		{"different fields not disjoint", f("status").Eq(state.Str[string]("paid")), f("total").Gt(state.Float[string](10)), false},
		{"opaque not disjoint", state.Guard[string]("a"), state.Guard[string]("b"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := symbolic.Disjoint(tc.a, tc.b, sc); got != tc.disjoint {
				t.Fatalf("Disjoint = %v, want %v", got, tc.disjoint)
			}
		})
	}
}
