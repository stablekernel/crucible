package symbolic

import (
	"testing"

	"github.com/stablekernel/crucible/state"
)

// comparisonOps is the closed set of comparison GuardOps the op tables
// (negateOp, reverseOp, evalConst, applyNumeric, applyDiscrete) must each handle.
// These are the only ops that can reach the op tables as a comparison; a wrong
// table entry for any of them can flip a disjointness verdict and silently mask
// nondeterminism (an unsound result), so every one is exercised exhaustively
// below. allGuardOps additionally enumerates the structural ops so the
// "exhaustiveness guard" tests fail loudly if a new comparison op is added to the
// enum without a matching table entry.
var comparisonOps = []state.GuardOp{
	state.GuardEq,
	state.GuardNe,
	state.GuardLt,
	state.GuardLe,
	state.GuardGt,
	state.GuardGe,
}

// allGuardOps is every GuardOp constant declared in the kernel. It is hand-mirrored
// from state/guard.go; the TestGuardOpEnum_Exhaustive test below pins the count so a
// new op added to the kernel forces a deliberate update here (and a review of every
// op table).
var allGuardOps = []state.GuardOp{
	state.GuardLeaf,
	state.GuardStateIn,
	state.GuardAnd,
	state.GuardOr,
	state.GuardNot,
	state.GuardEq,
	state.GuardNe,
	state.GuardLt,
	state.GuardLe,
	state.GuardGt,
	state.GuardGe,
	state.GuardIn,
	state.GuardField,
	state.GuardLit,
}

// TestGuardOpEnum_Exhaustive pins the size of the GuardOp enum and the comparison
// subset. If the kernel adds or removes a GuardOp, this test fails first, forcing a
// review of allGuardOps, comparisonOps, and every op table (negateOp / reverseOp /
// evalConst / applyNumeric / applyDiscrete) for a missing entry.
func TestGuardOpEnum_Exhaustive(t *testing.T) {
	const wantAll = 14
	if len(allGuardOps) != wantAll {
		t.Fatalf("allGuardOps has %d ops, want %d — a GuardOp was added/removed; "+
			"review every op table for a missing entry", len(allGuardOps), wantAll)
	}
	const wantComparisons = 6
	if len(comparisonOps) != wantComparisons {
		t.Fatalf("comparisonOps has %d ops, want %d", len(comparisonOps), wantComparisons)
	}
	seen := map[state.GuardOp]bool{}
	for _, op := range allGuardOps {
		if seen[op] {
			t.Fatalf("allGuardOps lists %q twice", op)
		}
		seen[op] = true
	}
	for _, op := range comparisonOps {
		if !seen[op] {
			t.Fatalf("comparisonOps op %q is not in allGuardOps", op)
		}
	}
}

// TestNegateOp_EveryOp asserts negateOp returns the logical negation of every
// comparison op (¬(x<y) is x≥y, etc.) and that it is an involution: negating twice
// returns the original. A wrong entry would invert a guard incorrectly under a
// GuardNot and corrupt the DNF.
func TestNegateOp_EveryOp(t *testing.T) {
	want := map[state.GuardOp]state.GuardOp{
		state.GuardEq: state.GuardNe,
		state.GuardNe: state.GuardEq,
		state.GuardLt: state.GuardGe,
		state.GuardLe: state.GuardGt,
		state.GuardGt: state.GuardLe,
		state.GuardGe: state.GuardLt,
	}
	for _, op := range comparisonOps {
		op := op
		t.Run(string(op), func(t *testing.T) {
			got := negateOp(op)
			if got != want[op] {
				t.Fatalf("negateOp(%q) = %q, want %q", op, got, want[op])
			}
			// Involution: ¬¬p ≡ p.
			if back := negateOp(got); back != op {
				t.Fatalf("negateOp(negateOp(%q)) = %q, want %q (not an involution)", op, back, op)
			}
			// Cross-check against truth: for representative operands the negated
			// op must disagree with the original on exactly the boolean.
			for _, pair := range [][2]float64{{1, 2}, {2, 2}, {3, 2}} {
				x, y := pair[0], pair[1]
				orig, ok1 := evalConst(num(x), op, num(y))
				neg, ok2 := evalConst(num(x), got, num(y))
				if !ok1 || !ok2 {
					t.Fatalf("evalConst returned not-ok for op %q on (%v,%v)", op, x, y)
				}
				if orig == neg {
					t.Fatalf("negateOp(%q)=%q but both give %v on (%v,%v) — not a negation",
						op, got, orig, x, y)
				}
			}
		})
	}
	// Structural (non-comparison) ops pass through unchanged.
	for _, op := range []state.GuardOp{state.GuardAnd, state.GuardOr, state.GuardIn, state.GuardField} {
		if got := negateOp(op); got != op {
			t.Fatalf("negateOp(%q) = %q, want passthrough %q", op, got, op)
		}
	}
}

// TestReverseOp_EveryOp asserts reverseOp swaps the operand orientation correctly
// for every comparison op (lit<field becomes field>lit, etc.) and round-trips:
// reversing twice returns the original. A wrong entry here directly mis-orients a
// literal-on-left comparison and can flip a disjointness verdict.
func TestReverseOp_EveryOp(t *testing.T) {
	want := map[state.GuardOp]state.GuardOp{
		state.GuardEq: state.GuardEq, // symmetric
		state.GuardNe: state.GuardNe, // symmetric
		state.GuardLt: state.GuardGt,
		state.GuardLe: state.GuardGe,
		state.GuardGt: state.GuardLt,
		state.GuardGe: state.GuardLe,
	}
	for _, op := range comparisonOps {
		op := op
		t.Run(string(op), func(t *testing.T) {
			got := reverseOp(op)
			if got != want[op] {
				t.Fatalf("reverseOp(%q) = %q, want %q", op, got, want[op])
			}
			// Round-trip: reversing the operands twice is the identity.
			if back := reverseOp(got); back != op {
				t.Fatalf("reverseOp(reverseOp(%q)) = %q, want %q (no round-trip)", op, back, op)
			}
			// Cross-check against truth: a <op> b must equal b <reverseOp(op)> a.
			for _, pair := range [][2]float64{{1, 2}, {2, 2}, {3, 2}} {
				a, b := pair[0], pair[1]
				lhs, ok1 := evalConst(num(a), op, num(b))
				rhs, ok2 := evalConst(num(b), got, num(a))
				if !ok1 || !ok2 {
					t.Fatalf("evalConst not-ok for op %q on (%v,%v)", op, a, b)
				}
				if lhs != rhs {
					t.Fatalf("a %q b = %v but b %q a = %v on (%v,%v) — reverseOp wrong",
						op, lhs, got, rhs, a, b)
				}
			}
		})
	}
	// Structural / symmetric-by-passthrough ops are returned unchanged.
	for _, op := range []state.GuardOp{state.GuardAnd, state.GuardOr, state.GuardIn, state.GuardField} {
		if got := reverseOp(op); got != op {
			t.Fatalf("reverseOp(%q) = %q, want passthrough %q", op, got, op)
		}
	}
}

// TestEvalConst_EveryOp evaluates every comparison op against representative
// constant operands — strictly less, equal, and strictly greater — and asserts the
// boolean and ok flag. A wrong entry folds a both-literal comparison to the wrong
// constant truth value, which can silently delete or keep a DNF clause.
func TestEvalConst_EveryOp(t *testing.T) {
	// For each op, the expected truth on (a<b), (a==b), (a>b).
	type want struct{ lt, eq, gt bool }
	table := map[state.GuardOp]want{
		state.GuardEq: {lt: false, eq: true, gt: false},
		state.GuardNe: {lt: true, eq: false, gt: true},
		state.GuardLt: {lt: true, eq: false, gt: false},
		state.GuardLe: {lt: true, eq: true, gt: false},
		state.GuardGt: {lt: false, eq: false, gt: true},
		state.GuardGe: {lt: false, eq: true, gt: true},
	}
	for _, op := range comparisonOps {
		op := op
		w, ok := table[op]
		if !ok {
			t.Fatalf("no expected-truth row for comparison op %q", op)
		}
		t.Run(string(op), func(t *testing.T) {
			cases := []struct {
				name   string
				a, b   float64
				expect bool
			}{
				{"less", 1, 2, w.lt},
				{"equal", 2, 2, w.eq},
				{"greater", 3, 2, w.gt},
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					got, ok := evalConst(num(c.a), op, num(c.b))
					if !ok {
						t.Fatalf("evalConst(%v,%q,%v) ok=false, want true", c.a, op, c.b)
					}
					if got != c.expect {
						t.Fatalf("evalConst(%v,%q,%v) = %v, want %v", c.a, op, c.b, got, c.expect)
					}
				})
			}
		})
	}
}

// TestEvalConst_TypedAndBoundaryOperands covers the operand-typing branches and
// numeric boundaries: mixed int/float forms compare by value, and a non-numeric
// operand on either side returns ok=false (abstain) rather than a bogus verdict.
func TestEvalConst_TypedAndBoundaryOperands(t *testing.T) {
	cases := []struct {
		name   string
		a      state.Literal
		op     state.GuardOp
		b      state.Literal
		want   bool
		wantOK bool
	}{
		{"int64 eq float64 same value", lit(state.IntParam, int64(2)), state.GuardEq, lit(state.FloatParam, float64(2)), true, true},
		{"int eq int32 same value", lit(state.IntParam, int(5)), state.GuardEq, lit(state.IntParam, int32(5)), true, true},
		{"float lt int", lit(state.FloatParam, float64(1.5)), state.GuardLt, lit(state.IntParam, int64(2)), true, true},
		{"zero ge zero", lit(state.IntParam, int64(0)), state.GuardGe, lit(state.IntParam, int64(0)), true, true},
		{"negative lt zero", lit(state.IntParam, int64(-1)), state.GuardLt, lit(state.IntParam, int64(0)), true, true},
		{"string operand abstains", lit(state.StringParam, "x"), state.GuardEq, lit(state.IntParam, int64(1)), false, false},
		{"bool operand abstains", lit(state.BoolParam, true), state.GuardEq, lit(state.BoolParam, true), false, false},
		{"both string abstains", lit(state.StringParam, "a"), state.GuardEq, lit(state.StringParam, "a"), false, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, ok := evalConst(c.a, c.op, c.b)
			if ok != c.wantOK {
				t.Fatalf("evalConst ok = %v, want %v", ok, c.wantOK)
			}
			if ok && got != c.want {
				t.Fatalf("evalConst = %v, want %v", got, c.want)
			}
		})
	}
}

// TestEvalConst_UnknownOpAbstains feeds evalConst a structural (non-comparison) op
// to assert the default branch abstains (ok=false) instead of inventing a verdict.
func TestEvalConst_UnknownOpAbstains(t *testing.T) {
	for _, op := range []state.GuardOp{state.GuardAnd, state.GuardOr, state.GuardNot, state.GuardIn, state.GuardField} {
		if v, ok := evalConst(num(1), op, num(2)); ok {
			t.Fatalf("evalConst with non-comparison op %q = (%v, ok) — want ok=false", op, v)
		}
	}
}

// TestCompareAtom_EveryBranch covers every orientation branch of compareAtom:
// field-op-lit (kept as-is and negated), lit-op-field (reversed, and reversed then
// negated), lit-op-lit (folded to a const, with and without negation), and the
// unorientable cases (wrong arity, two fields) that fall through to opaque.
func TestCompareAtom_EveryBranch(t *testing.T) {
	fieldNode := state.GuardNode[string]{Op: state.GuardField, Path: "total"}
	litNode := func(v float64) state.GuardNode[string] {
		l := lit(state.FloatParam, v)
		return state.GuardNode[string]{Op: state.GuardLit, Lit: &l}
	}

	t.Run("field-op-lit kept", func(t *testing.T) {
		g := state.GuardNode[string]{Op: state.GuardLt, Children: []state.GuardNode[string]{fieldNode, litNode(5)}}
		a := compareAtom(g, false)
		if a.kind != atomCompare || a.path != "total" || a.op != state.GuardLt {
			t.Fatalf("compareAtom = %+v, want atomCompare total lt", a)
		}
	})
	t.Run("field-op-lit negated folds the op", func(t *testing.T) {
		g := state.GuardNode[string]{Op: state.GuardLt, Children: []state.GuardNode[string]{fieldNode, litNode(5)}}
		a := compareAtom(g, true)
		if a.kind != atomCompare || a.op != state.GuardGe {
			t.Fatalf("compareAtom(neg) op = %q, want %q (negateOp of lt)", a.op, state.GuardGe)
		}
	})
	t.Run("lit-op-field reversed", func(t *testing.T) {
		// 5 < total  ⇒  total > 5
		g := state.GuardNode[string]{Op: state.GuardLt, Children: []state.GuardNode[string]{litNode(5), fieldNode}}
		a := compareAtom(g, false)
		if a.kind != atomCompare || a.path != "total" || a.op != state.GuardGt {
			t.Fatalf("compareAtom = %+v, want atomCompare total gt (reversed)", a)
		}
	})
	t.Run("lit-op-field reversed then negated", func(t *testing.T) {
		// ¬(5 < total) = ¬(total > 5) = total ≤ 5
		g := state.GuardNode[string]{Op: state.GuardLt, Children: []state.GuardNode[string]{litNode(5), fieldNode}}
		a := compareAtom(g, true)
		if a.op != state.GuardLe {
			t.Fatalf("compareAtom(neg) op = %q, want %q", a.op, state.GuardLe)
		}
	})
	t.Run("lit-op-lit folds to const true", func(t *testing.T) {
		g := state.GuardNode[string]{Op: state.GuardLt, Children: []state.GuardNode[string]{litNode(1), litNode(2)}}
		a := compareAtom(g, false)
		if a.kind != atomConst || a.value != true {
			t.Fatalf("compareAtom = %+v, want atomConst true (1<2)", a)
		}
	})
	t.Run("lit-op-lit negated flips the const", func(t *testing.T) {
		g := state.GuardNode[string]{Op: state.GuardLt, Children: []state.GuardNode[string]{litNode(1), litNode(2)}}
		a := compareAtom(g, true)
		if a.kind != atomConst || a.value != false {
			t.Fatalf("compareAtom(neg) = %+v, want atomConst false (¬(1<2))", a)
		}
	})
	t.Run("lit-op-lit non-numeric is opaque", func(t *testing.T) {
		sl := lit(state.StringParam, "a")
		sn := state.GuardNode[string]{Op: state.GuardLit, Lit: &sl}
		g := state.GuardNode[string]{Op: state.GuardEq, Children: []state.GuardNode[string]{sn, sn}}
		a := compareAtom(g, false)
		if a.kind != atomOpaque {
			t.Fatalf("compareAtom = %+v, want atomOpaque (non-numeric const)", a)
		}
	})
	t.Run("wrong arity is opaque", func(t *testing.T) {
		g := state.GuardNode[string]{Op: state.GuardLt, Children: []state.GuardNode[string]{fieldNode}}
		if a := compareAtom(g, false); a.kind != atomOpaque {
			t.Fatalf("compareAtom = %+v, want atomOpaque (arity 1)", a)
		}
	})
	t.Run("two fields is opaque", func(t *testing.T) {
		other := state.GuardNode[string]{Op: state.GuardField, Path: "quantity"}
		g := state.GuardNode[string]{Op: state.GuardLt, Children: []state.GuardNode[string]{fieldNode, other}}
		if a := compareAtom(g, false); a.kind != atomOpaque {
			t.Fatalf("compareAtom = %+v, want atomOpaque (field vs field)", a)
		}
	})
}

// TestApplyNumeric_EveryOp folds each comparison op into a fresh interval and
// asserts the resulting bound, mirroring the op-table semantics applyNumeric
// encodes. A wrong entry mis-constrains a numeric field and can flip emptiness.
func TestApplyNumeric_EveryOp(t *testing.T) {
	t.Run("eq pins a closed point", func(t *testing.T) {
		iv := newInterval()
		applyNumeric(iv, state.GuardEq, 5)
		if !iv.loSet || !iv.hiSet || iv.lo != 5 || iv.hi != 5 || !iv.loInc || !iv.hiInc {
			t.Fatalf("eq did not pin closed [5,5]: %+v", iv)
		}
	})
	t.Run("ne records an exclusion", func(t *testing.T) {
		iv := newInterval()
		applyNumeric(iv, state.GuardNe, 5)
		if _, ok := iv.neqs[5]; !ok {
			t.Fatalf("ne did not record exclusion of 5: %+v", iv)
		}
	})
	t.Run("lt sets exclusive upper", func(t *testing.T) {
		iv := newInterval()
		applyNumeric(iv, state.GuardLt, 5)
		if !iv.hiSet || iv.hi != 5 || iv.hiInc {
			t.Fatalf("lt did not set exclusive hi=5: %+v", iv)
		}
	})
	t.Run("le sets inclusive upper", func(t *testing.T) {
		iv := newInterval()
		applyNumeric(iv, state.GuardLe, 5)
		if !iv.hiSet || iv.hi != 5 || !iv.hiInc {
			t.Fatalf("le did not set inclusive hi=5: %+v", iv)
		}
	})
	t.Run("gt sets exclusive lower", func(t *testing.T) {
		iv := newInterval()
		applyNumeric(iv, state.GuardGt, 5)
		if !iv.loSet || iv.lo != 5 || iv.loInc {
			t.Fatalf("gt did not set exclusive lo=5: %+v", iv)
		}
	})
	t.Run("ge sets inclusive lower", func(t *testing.T) {
		iv := newInterval()
		applyNumeric(iv, state.GuardGe, 5)
		if !iv.loSet || iv.lo != 5 || !iv.loInc {
			t.Fatalf("ge did not set inclusive lo=5: %+v", iv)
		}
	})
}

// TestApplyDiscrete_EveryOp folds each op into a discrete set: eq requires, ne
// excludes, and the ordering ops are intentionally left unconstrained (a discrete
// field has no order). A wrong entry would either drop an equality constraint or
// invent an ordering the field has not.
func TestApplyDiscrete_EveryOp(t *testing.T) {
	t.Run("eq requires the value", func(t *testing.T) {
		d := newDiscrete()
		applyDiscrete(d, state.GuardEq, "paid")
		if _, ok := d.required["paid"]; !ok {
			t.Fatalf("eq did not require paid: %+v", d)
		}
	})
	t.Run("ne excludes the value", func(t *testing.T) {
		d := newDiscrete()
		applyDiscrete(d, state.GuardNe, "paid")
		if _, ok := d.excluded["paid"]; !ok {
			t.Fatalf("ne did not exclude paid: %+v", d)
		}
	})
	for _, op := range []state.GuardOp{state.GuardLt, state.GuardLe, state.GuardGt, state.GuardGe} {
		op := op
		t.Run("ordering "+string(op)+" leaves unconstrained", func(t *testing.T) {
			d := newDiscrete()
			applyDiscrete(d, op, "paid")
			if len(d.required) != 0 || len(d.excluded) != 0 {
				t.Fatalf("ordering op %q constrained a discrete field: %+v", op, d)
			}
		})
	}
}

// num builds a numeric (float) literal for op-table tests.
func num(v float64) state.Literal { return state.Literal{Type: state.FloatParam, Value: v} }

// lit builds a typed literal for op-table tests.
func lit(t state.ParamType, v any) state.Literal { return state.Literal{Type: t, Value: v} }
