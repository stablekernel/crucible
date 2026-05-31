// Package symbolic reasons about a machine's guards structurally — without
// executing them — over the kernel's Core GuardNode tree. It answers whether a
// guard is satisfiable (can ever hold) and whether two guards are disjoint (can
// never both hold), and scans a machine for competing transitions whose guards
// overlap (candidate nondeterminism).
//
// It is a bounded, pure-Go analyzer, not an SMT solver: it normalizes a guard to
// disjunctive normal form and checks each conjunctive clause for a per-field
// contradiction — numeric fields as intervals, discrete (string/bool/enum) fields
// as value sets. It is deliberately conservative: a Rich or named-leaf guard, a
// cross-field comparison, or a duration/time field it cannot model is treated as an
// unconstrained unknown, so it never reports a contradiction or a disjointness it
// cannot prove. This makes Contradiction and Disjoint sound (no false positives):
// a true verdict is always correct, while an unprovable case is reported as
// satisfiable / not-disjoint.
//
// It consumes only the kernel's own GuardNode and ContextSchema, so it adds no
// dependency and stays in the stdlib-only state module.
package symbolic

import (
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// maxClauses caps the disjunctive-normal-form expansion so a pathologically nested
// guard cannot blow up; beyond it the analyzer abstains (reports satisfiable / not a
// contradiction) rather than spend unbounded work.
const maxClauses = 4096

type atomKind int

const (
	atomCompare atomKind = iota // path <op> literal
	atomIn                      // path in {set} (negated: not in)
	atomState                   // stateIn(key) (negated: not in configuration)
	atomOpaque                  // a guard the analyzer cannot interpret
	atomConst                   // a statically-evaluated constant
)

// atom is one literal in a conjunctive clause.
type atom struct {
	kind  atomKind
	path  string
	op    state.GuardOp
	lit   state.Literal
	set   []state.Literal
	key   string
	neg   bool
	value bool // atomConst: the constant truth value
}

type clause []atom

// negateOp returns the comparison op of the negated predicate.
func negateOp(op state.GuardOp) state.GuardOp {
	switch op {
	case state.GuardEq:
		return state.GuardNe
	case state.GuardNe:
		return state.GuardEq
	case state.GuardLt:
		return state.GuardGe
	case state.GuardLe:
		return state.GuardGt
	case state.GuardGt:
		return state.GuardLe
	case state.GuardGe:
		return state.GuardLt
	default:
		return op
	}
}

// reverseOp returns the op for the operands swapped (literal-on-left → field-on-left).
func reverseOp(op state.GuardOp) state.GuardOp {
	switch op {
	case state.GuardLt:
		return state.GuardGt
	case state.GuardLe:
		return state.GuardGe
	case state.GuardGt:
		return state.GuardLt
	case state.GuardGe:
		return state.GuardLe
	default: // eq/ne are symmetric
		return op
	}
}

// dnf normalizes g (optionally negated) to disjunctive normal form: a slice of
// conjunctive clauses whose disjunction is equivalent to the guard. It returns
// ok=false when the expansion exceeds maxClauses.
func dnf[S comparable](g state.GuardNode[S], neg bool) ([]clause, bool) {
	switch g.Op {
	case state.GuardNot:
		if len(g.Children) == 1 {
			return dnf(g.Children[0], !neg)
		}
		return []clause{{opaque(g, neg)}}, true

	case state.GuardAnd:
		if neg { // ¬(a ∧ b …) = ¬a ∨ ¬b ∨ …
			return union(g.Children, true)
		}
		return product(g.Children, false)

	case state.GuardOr:
		if neg { // ¬(a ∨ b …) = ¬a ∧ ¬b ∧ …
			return product(g.Children, true)
		}
		return union(g.Children, false)

	case state.GuardEq, state.GuardNe, state.GuardLt, state.GuardLe, state.GuardGt, state.GuardGe:
		return []clause{{compareAtom(g, neg)}}, true

	case state.GuardIn:
		if len(g.Children) == 1 && g.Children[0].Op == state.GuardField {
			return []clause{{atom{kind: atomIn, path: g.Children[0].Path, set: g.Set, neg: neg}}}, true
		}
		return []clause{{opaque(g, neg)}}, true

	case state.GuardStateIn:
		key := ""
		if g.In != nil {
			key = "state:" + fmt.Sprintf("%v", *g.In)
		}
		return []clause{{atom{kind: atomState, key: key, neg: neg}}}, true

	default: // leaf, rich, field/lit at the top, empty/unrecognized → opaque
		return []clause{{opaque(g, neg)}}, true
	}
}

// union concatenates the children's clause sets (the disjunction of the children).
func union[S comparable](children []state.GuardNode[S], neg bool) ([]clause, bool) {
	var out []clause
	for i := range children {
		cs, ok := dnf(children[i], neg)
		if !ok {
			return nil, false
		}
		out = append(out, cs...)
		if len(out) > maxClauses {
			return nil, false
		}
	}
	return out, true
}

// product distributes AND over the children's clause sets (cartesian concatenation).
func product[S comparable](children []state.GuardNode[S], neg bool) ([]clause, bool) {
	acc := []clause{{}}
	for i := range children {
		cs, ok := dnf(children[i], neg)
		if !ok {
			return nil, false
		}
		next := make([]clause, 0, len(acc)*len(cs))
		for _, a := range acc {
			for _, b := range cs {
				combined := make(clause, 0, len(a)+len(b))
				combined = append(combined, a...)
				combined = append(combined, b...)
				next = append(next, combined)
				if len(next) > maxClauses {
					return nil, false
				}
			}
		}
		acc = next
	}
	return acc, true
}

// opaque builds an atom for a guard node the analyzer cannot interpret, keyed so
// that the same node and its negation cancel within a clause.
func opaque[S comparable](g state.GuardNode[S], neg bool) atom {
	key := "op:" + string(g.Op)
	if g.Ref != nil {
		key = "ref:" + g.Ref.Name
	} else if g.Path != "" {
		key = "path:" + g.Path
	}
	return atom{kind: atomOpaque, key: key, neg: neg}
}

// compareAtom builds the atom for a comparison node, orienting it as field-op-literal
// and folding negation into the op. A both-literal comparison is evaluated to a
// constant; a comparison the analyzer cannot orient becomes opaque.
func compareAtom[S comparable](g state.GuardNode[S], neg bool) atom {
	if len(g.Children) != 2 {
		return opaque(g, neg)
	}
	a, b := g.Children[0], g.Children[1]
	op := g.Op
	switch {
	case a.Op == state.GuardField && b.Op == state.GuardLit && b.Lit != nil:
		if neg {
			op = negateOp(op)
		}
		return atom{kind: atomCompare, path: a.Path, op: op, lit: *b.Lit}
	case a.Op == state.GuardLit && b.Op == state.GuardField && a.Lit != nil:
		op = reverseOp(op)
		if neg {
			op = negateOp(op)
		}
		return atom{kind: atomCompare, path: b.Path, op: op, lit: *a.Lit}
	case a.Op == state.GuardLit && b.Op == state.GuardLit && a.Lit != nil && b.Lit != nil:
		if v, ok := evalConst(*a.Lit, g.Op, *b.Lit); ok {
			return atom{kind: atomConst, value: v != neg}
		}
		return opaque(g, neg)
	default:
		return opaque(g, neg)
	}
}

// evalConst evaluates a comparison between two numeric literals, reporting false for
// a comparison it does not handle (non-numeric operands).
func evalConst(a state.Literal, op state.GuardOp, b state.Literal) (bool, bool) {
	x, okA := litNumber(a)
	y, okB := litNumber(b)
	if !okA || !okB {
		return false, false
	}
	switch op {
	case state.GuardEq:
		return x == y, true
	case state.GuardNe:
		return x != y, true
	case state.GuardLt:
		return x < y, true
	case state.GuardLe:
		return x <= y, true
	case state.GuardGt:
		return x > y, true
	case state.GuardGe:
		return x >= y, true
	default:
		return false, false
	}
}

// contradictory reports whether a clause admits no assignment: a false constant, an
// opaque key asserted both ways, an empty numeric interval, or an empty discrete set.
func contradictory(c clause, kinds map[string]fieldKind) bool {
	intervals := map[string]*interval{}
	discretes := map[string]*discrete{}
	opaques := map[string]bool{} // key -> seen-positive; paired with a negated sighting
	opaqueNeg := map[string]bool{}

	for _, a := range c {
		switch a.kind {
		case atomConst:
			if !a.value {
				return true
			}
		case atomCompare:
			switch kinds[a.path] {
			case kindNumeric:
				n, ok := litNumber(a.lit)
				if !ok {
					continue // numeric field vs non-numeric literal: cannot constrain
				}
				iv := intervals[a.path]
				if iv == nil {
					iv = newInterval()
					intervals[a.path] = iv
				}
				applyNumeric(iv, a.op, n)
			case kindDiscrete:
				d := discretes[a.path]
				if d == nil {
					d = newDiscrete()
					discretes[a.path] = d
				}
				applyDiscrete(d, a.op, litKey(a.lit))
			}
		case atomIn:
			if kinds[a.path] != kindDiscrete {
				continue // `in` over a numeric/unknown field is left unconstrained
			}
			d := discretes[a.path]
			if d == nil {
				d = newDiscrete()
				discretes[a.path] = d
			}
			vals := map[string]struct{}{}
			for _, l := range a.set {
				vals[litKey(l)] = struct{}{}
			}
			if a.neg {
				for v := range vals {
					d.exclude(v)
				}
			} else {
				d.restrict(vals)
			}
		case atomState, atomOpaque:
			if a.neg {
				opaqueNeg[a.key] = true
			} else {
				opaques[a.key] = true
			}
		}
	}

	for k := range opaques {
		if opaqueNeg[k] {
			return true // the same predicate asserted true and false
		}
	}
	for _, iv := range intervals {
		if iv.empty() {
			return true
		}
	}
	for _, d := range discretes {
		if d.contradiction() {
			return true
		}
	}
	return false
}

// applyNumeric folds a comparison into a numeric field's interval.
func applyNumeric(iv *interval, op state.GuardOp, v float64) {
	switch op {
	case state.GuardEq:
		iv.tightenLow(v, true)
		iv.tightenHigh(v, true)
	case state.GuardNe:
		iv.neqs[v] = struct{}{}
	case state.GuardLt:
		iv.tightenHigh(v, false)
	case state.GuardLe:
		iv.tightenHigh(v, true)
	case state.GuardGt:
		iv.tightenLow(v, false)
	case state.GuardGe:
		iv.tightenLow(v, true)
	}
}

// applyDiscrete folds an equality/inequality into a discrete field's value set.
// Ordering comparisons (lt/le/gt/ge) on a discrete field are left unconstrained.
func applyDiscrete(d *discrete, op state.GuardOp, key string) {
	switch op {
	case state.GuardEq:
		d.require(key)
	case state.GuardNe:
		d.exclude(key)
	}
}

// provablyUnsat reports whether g is provably unsatisfiable: every DNF clause is
// contradictory. An over-budget expansion abstains (false).
func provablyUnsat[S comparable](g state.GuardNode[S], schema state.ContextSchema) bool {
	clauses, ok := dnf(g, false)
	if !ok || len(clauses) == 0 {
		return false
	}
	kinds := schemaKinds(schema)
	for _, c := range clauses {
		if !contradictory(c, kinds) {
			return false
		}
	}
	return true
}

// Satisfiable reports whether the guard can ever hold. It is conservative: it
// returns false only when the guard is provably unsatisfiable (a contradiction over
// the analyzable Core constraints), and true otherwise — including guards it cannot
// analyze. So a false result is a proof of unsatisfiability; a true result may be
// "satisfiable" or merely "not provably unsatisfiable."
func Satisfiable[S comparable](g state.GuardNode[S], schema state.ContextSchema) bool {
	return !provablyUnsat(g, schema)
}

// Contradiction reports whether the guard is provably unsatisfiable — no context can
// make it true, so a transition it gates is dead. It is sound: it never reports a
// contradiction it cannot prove (an opaque, Rich, or cross-field guard yields false).
func Contradiction[S comparable](g state.GuardNode[S], schema state.ContextSchema) bool {
	return provablyUnsat(g, schema)
}

// Disjoint reports whether a and b are provably mutually exclusive — no context can
// satisfy both at once, so two transitions guarded by them can never both be
// enabled. It is the satisfiability of their conjunction, and is sound: it returns
// true only when the conjunction is provably unsatisfiable, so guards it cannot
// fully analyze are reported as not-disjoint (the safe answer for nondeterminism
// detection). It is the basis for finding competing transitions whose guards
// overlap.
func Disjoint[S comparable](a, b state.GuardNode[S], schema state.ContextSchema) bool {
	return provablyUnsat(state.And(a, b), schema)
}
