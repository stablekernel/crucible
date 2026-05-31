package expr_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/expr"
)

// TestLower_RendersExpectedSource asserts Lower emits the CEL source a Core tree maps
// to, including the injected double() cast on an int operand compared against a float.
func TestLower_RendersExpectedSource(t *testing.T) {
	cases := []struct {
		name string
		node state.GuardNode[string]
		want string // substring the lowered source must contain
	}{
		{"string eq", state.Field[string]("s").Eq(state.Str[string]("a")), `s == "a"`},
		{"int eq", state.Field[string]("i").Eq(state.Int[string](3)), `i == 3`},
		{"int-vs-float cast", state.Field[string]("i").Lt(state.Float[string](2.5)), `double(i) < 2.5`},
		{"float-vs-int cast", state.Field[string]("f").Gt(state.Int[string](1)), `f > double(1)`},
		{"duration literal", state.Field[string]("d").Ge(state.Dur[string](time.Second)), `duration("1s")`},
		{"bool spine", state.And(
			state.Field[string]("b").Eq(state.Bool[string](true)),
			state.Not(state.Field[string]("s").Eq(state.Str[string]("x"))),
		), `&&`},
		{"membership", state.Field[string]("s").In(state.Str[string]("a"), state.Str[string]("b")), `s in ["a", "b"]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, source, err := expr.Lower(tc.node, fuzzSchema())
			if err != nil {
				t.Fatalf("Lower: %v", err)
			}
			if !strings.Contains(source, tc.want) {
				t.Fatalf("lowered source %q does not contain %q", source, tc.want)
			}
		})
	}
}

// TestLower_MembershipNumericCast asserts an int field tested against a set holding a
// float is double-cast on both the operand and the int set elements, so membership
// matches Core's numeric coercion.
func TestLower_MembershipNumericCast(t *testing.T) {
	node := state.Field[string]("i").In(state.Int[string](1), state.Float[string](2.5))
	_, source, err := expr.Lower(node, fuzzSchema())
	if err != nil {
		t.Fatalf("Lower: %v", err)
	}
	if !strings.Contains(source, "double(i) in") || !strings.Contains(source, "double(1)") {
		t.Fatalf("expected double-cast membership, got %q", source)
	}
}

// TestLower_RejectsNonCoreNode asserts Lower refuses a node outside the Core
// vocabulary — a named-ref leaf and the stateIn built-in have no Core data to lower.
func TestLower_RejectsNonCoreNode(t *testing.T) {
	for _, node := range []state.GuardNode[string]{
		state.Guard[string]("named"),
		state.StateIn[string]("someState"),
	} {
		if _, _, err := expr.Lower(node, fuzzSchema()); err == nil {
			t.Fatalf("Lower should reject op %q", node.Op)
		}
	}
}

// TestLower_RejectsUnknownField asserts a field path absent from the schema fails
// lowering loudly rather than producing an untypeable expression.
func TestLower_RejectsUnknownField(t *testing.T) {
	node := state.Field[string]("nope").Eq(state.Int[string](1))
	if _, _, err := expr.Lower(node, fuzzSchema()); err == nil {
		t.Fatal("Lower should reject an unknown field")
	}
}

// TestEvalLowered_MatchesIntent spot-checks EvalLowered against hand-computed truths,
// independent of the generative equivalence check, so the helper itself is verified.
func TestEvalLowered_MatchesIntent(t *testing.T) {
	ctx := fuzzCtx{I: 3, F: 2.5, S: "paid", B: true, D: 2 * time.Second}
	cases := []struct {
		node state.GuardNode[string]
		want bool
	}{
		{state.Field[string]("i").Gt(state.Float[string](2.5)), true},
		{state.Field[string]("f").Eq(state.Float[string](2.5)), true},
		{state.Field[string]("s").In(state.Str[string]("paid")), true},
		{state.Field[string]("b").Ne(state.Bool[string](false)), true},
		{state.Field[string]("d").Le(state.Dur[string](2 * time.Second)), true},
		{state.Field[string]("i").Lt(state.Int[string](0)), false},
	}
	for _, tc := range cases {
		got, source, err := expr.EvalLowered(tc.node, fuzzSchema(), ctx)
		if err != nil {
			t.Fatalf("EvalLowered: %v", err)
		}
		if got != tc.want {
			t.Fatalf("source %q: got %v, want %v", source, got, tc.want)
		}
	}
}
