package expr_test

import (
	"math/rand"
	"time"

	"github.com/stablekernel/crucible/state"
)

// This file holds the generators the equivalence check draws from: random
// schema-conforming contexts and random, type-correct Core guard trees. Generation
// is constrained to type-compatible compares so every produced tree passes the
// kernel's Quench type-check (an incompatible compare would be rejected at Quench,
// which is a separate, already-covered behavior); the check's job is to compare
// VERDICTS on trees both tiers accept, not to re-test type rejection.

// genCtx generates a random fuzzCtx. Numeric ranges are kept small and overlapping
// across the int and float fields so int↔float compares land on both sides of
// equality and ordering boundaries, stressing the cast-injection path. Durations are
// drawn in whole seconds so the Go-duration round-trip is exact.
func genCtx(rng *rand.Rand) fuzzCtx {
	return fuzzCtx{
		I: rng.Intn(11) - 5,
		J: rng.Intn(11) - 5,
		F: float64(rng.Intn(21)-10) / 2.0, // -5.0 .. 5.0 in 0.5 steps
		G: float64(rng.Intn(21)-10) / 2.0,
		S: genString(rng),
		B: rng.Intn(2) == 0,
		D: time.Duration(rng.Intn(7)) * time.Second,
	}
}

// genString draws from a small alphabet so string equality and ordering both hit and
// miss across generated contexts and literals.
func genString(rng *rand.Rand) string {
	return []string{"a", "b", "c", "paid", "open"}[rng.Intn(5)]
}

// numericField names the int and float fields by category, so a compare can pick two
// numeric operands that inter-compare (the int↔float case the casts reconcile).
var (
	intFields = []string{"i", "j"}
	fltFields = []string{"f", "g"}
	numFields = []string{"i", "j", "f", "g"}
)

// genNode generates a random Core guard tree. depth bounds recursion so trees stay
// small; past the bound it always emits a leaf predicate. The boolean spine
// (and/or/not) composes child predicates; the leaves are numeric, string, bool, and
// duration compares plus numeric and string membership — the full Core vocabulary the
// lowering covers.
func genNode(rng *rand.Rand, depth int) state.GuardNode[string] {
	if depth >= 3 || rng.Intn(100) < 55 {
		return genLeaf(rng)
	}
	switch rng.Intn(3) {
	case 0:
		return state.And(genNode(rng, depth+1), genNode(rng, depth+1))
	case 1:
		return state.Or(genNode(rng, depth+1), genNode(rng, depth+1))
	default:
		return state.Not(genNode(rng, depth+1))
	}
}

// genLeaf generates a single type-correct Core predicate leaf.
func genLeaf(rng *rand.Rand) state.GuardNode[string] {
	switch rng.Intn(6) {
	case 0:
		return genNumericCompare(rng)
	case 1:
		return genStringCompare(rng)
	case 2:
		return genBoolCompare(rng)
	case 3:
		return genDurationCompare(rng)
	case 4:
		return genNumericMembership(rng)
	default:
		return genStringMembership(rng)
	}
}

// cmpOps is the set of compare builders keyed by a random index, returning the node
// for a given (field, operand) pair.
func applyCmp(rng *rand.Rand, f state.FieldRef[string], op state.Operand[string]) state.GuardNode[string] {
	switch rng.Intn(6) {
	case 0:
		return f.Eq(op)
	case 1:
		return f.Ne(op)
	case 2:
		return f.Lt(op)
	case 3:
		return f.Le(op)
	case 4:
		return f.Gt(op)
	default:
		return f.Ge(op)
	}
}

// genNumericCompare builds a compare whose left side is a numeric field and whose
// right side is either a numeric field or a numeric literal, mixing int and float
// freely so the int↔float coercion path is exercised on both operands.
func genNumericCompare(rng *rand.Rand) state.GuardNode[string] {
	left := state.Field[string](numFields[rng.Intn(len(numFields))])
	var op state.Operand[string]
	switch rng.Intn(4) {
	case 0:
		op = state.FieldOp(state.Field[string](intFields[rng.Intn(len(intFields))]))
	case 1:
		op = state.FieldOp(state.Field[string](fltFields[rng.Intn(len(fltFields))]))
	case 2:
		op = state.Int[string](int64(rng.Intn(11) - 5))
	default:
		op = state.Float[string](float64(rng.Intn(21)-10) / 2.0)
	}
	return applyCmp(rng, left, op)
}

// genStringCompare builds a compare between the string field and a string literal (or
// the field against itself).
func genStringCompare(rng *rand.Rand) state.GuardNode[string] {
	left := state.Field[string]("s")
	var op state.Operand[string]
	if rng.Intn(2) == 0 {
		op = state.Str[string](genString(rng))
	} else {
		op = state.FieldOp(state.Field[string]("s"))
	}
	return applyCmp(rng, left, op)
}

// genBoolCompare builds an equality/inequality between the bool field and a bool
// literal. Booleans have no ordering, so only eq/ne are generated.
func genBoolCompare(rng *rand.Rand) state.GuardNode[string] {
	left := state.Field[string]("b")
	op := state.Bool[string](rng.Intn(2) == 0)
	if rng.Intn(2) == 0 {
		return left.Eq(op)
	}
	return left.Ne(op)
}

// genDurationCompare builds a compare between the duration field and a duration
// literal in whole seconds.
func genDurationCompare(rng *rand.Rand) state.GuardNode[string] {
	left := state.Field[string]("d")
	op := state.Dur[string](time.Duration(rng.Intn(7)) * time.Second)
	return applyCmp(rng, left, op)
}

// genNumericMembership builds an `in` test of a numeric field against a small set of
// numeric literals, mixing int and float elements so membership exercises the same
// coercion the compares do.
func genNumericMembership(rng *rand.Rand) state.GuardNode[string] {
	left := state.Field[string](numFields[rng.Intn(len(numFields))])
	n := rng.Intn(3) + 1
	set := make([]state.Operand[string], 0, n)
	for i := 0; i < n; i++ {
		if rng.Intn(2) == 0 {
			set = append(set, state.Int[string](int64(rng.Intn(11)-5)))
		} else {
			set = append(set, state.Float[string](float64(rng.Intn(21)-10)/2.0))
		}
	}
	return left.In(set...)
}

// genStringMembership builds an `in` test of the string field against a small set of
// string literals.
func genStringMembership(rng *rand.Rand) state.GuardNode[string] {
	left := state.Field[string]("s")
	n := rng.Intn(3) + 1
	set := make([]state.Operand[string], 0, n)
	for i := 0; i < n; i++ {
		set = append(set, state.Str[string](genString(rng)))
	}
	return left.In(set...)
}
