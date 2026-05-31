package expr

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/google/cel-go/cel"

	"github.com/stablekernel/crucible/state"
)

// Lower compiles a Core guard expression tree to an equivalent CEL program against
// schema, returning the compiled program and the CEL source it lowered to. It is the
// bridge that lets a Core guard — authored with the kernel's dependency-free builder
// — evaluate through the same CEL engine a rich guard uses, and it is what the
// equivalence check exercises to prove the two tiers agree.
//
// Core is a strict subset of CEL modulo one deliberate softening: Core compares
// integers and floats by coercing both to float64, whereas CEL rejects a mixed
// int/double comparison at type-check. Lowering closes that gap by injecting an
// explicit double() cast around an integer operand compared against a float, so the
// lowered expression type-checks and evaluates identically to Core's numeric
// coercion. Raw rich CEL stays strict; only this lowering is coercion-lenient, and
// only by injecting casts the author could have written by hand.
//
// Lowering is defined for the boolean spine (and/or/not), the typed compares
// (eq/ne/lt/le/gt/ge), membership (in), field refs, and literals — the whole Core
// vocabulary. A node outside that vocabulary (a named-ref leaf, stateIn, or an
// unknown op) has no Core-data meaning to lower and is reported as an error.
func Lower[S comparable](node state.GuardNode[S], schema state.ContextSchema) (cel.Program, string, error) {
	source, err := lowerNode(&node, schema)
	if err != nil {
		return nil, "", err
	}
	env, err := newEnv(schema)
	if err != nil {
		return nil, source, fmt.Errorf("lower: %w", err)
	}
	ast, iss := env.Compile(source)
	if iss != nil && iss.Err() != nil {
		return nil, source, fmt.Errorf("lower: compile %q: %w", source, iss.Err())
	}
	program, err := env.Program(ast, cel.CostLimit(defaultCostLimit))
	if err != nil {
		return nil, source, fmt.Errorf("lower: build program: %w", err)
	}
	return program, source, nil
}

// EvalLowered lowers a Core guard tree to CEL and evaluates it against a context in
// one call, returning the boolean verdict and the CEL source it lowered to. It is the
// convenience the equivalence check uses to compare a lowered Core node against the
// kernel's own Core evaluation, and a useful tool for a host that wants to run a Core
// guard through the CEL engine (for example, to preview it the way the browser will).
func EvalLowered[S comparable](node state.GuardNode[S], schema state.ContextSchema, entity any) (bool, string, error) {
	program, source, err := Lower(node, schema)
	if err != nil {
		return false, source, err
	}
	activation, err := marshalActivation(entity, schema)
	if err != nil {
		return false, source, err
	}
	out, _, err := program.Eval(activation)
	if err != nil {
		return false, source, fmt.Errorf("eval %q: %w", source, err)
	}
	ok, err := boolVal(out)
	if err != nil {
		return false, source, fmt.Errorf("%q: %w", source, err)
	}
	return ok, source, nil
}

// lowerNode renders a Core guard node to CEL source text. It recurses the boolean
// spine and renders each Core predicate leaf with its operands, injecting numeric
// casts where Core's float64 coercion and CEL's strict numeric typing diverge.
func lowerNode[S comparable](g *state.GuardNode[S], schema state.ContextSchema) (string, error) {
	switch g.Op {
	case state.GuardAnd:
		return lowerBoolean(g, schema, "&&")
	case state.GuardOr:
		return lowerBoolean(g, schema, "||")
	case state.GuardNot:
		if len(g.Children) != 1 {
			return "", fmt.Errorf("not requires one operand, got %d", len(g.Children))
		}
		inner, err := lowerNode(&g.Children[0], schema)
		if err != nil {
			return "", err
		}
		return "!(" + inner + ")", nil
	case state.GuardEq, state.GuardNe, state.GuardLt, state.GuardLe, state.GuardGt, state.GuardGe:
		return lowerCompare(g, schema)
	case state.GuardIn:
		return lowerMembership(g, schema)
	default:
		return "", fmt.Errorf("cannot lower guard op %q (only Core nodes lower to CEL)", g.Op)
	}
}

// lowerBoolean renders an and/or node by joining its lowered children with the CEL
// boolean operator, parenthesizing each child so precedence matches the tree.
func lowerBoolean[S comparable](g *state.GuardNode[S], schema state.ContextSchema, op string) (string, error) {
	if len(g.Children) == 0 {
		return "", fmt.Errorf("%s requires at least one operand", g.Op)
	}
	parts := make([]string, 0, len(g.Children))
	for i := range g.Children {
		p, err := lowerNode(&g.Children[i], schema)
		if err != nil {
			return "", err
		}
		parts = append(parts, "("+p+")")
	}
	return strings.Join(parts, " "+op+" "), nil
}

// celCompareOp maps a Core compare op to its CEL operator.
var celCompareOp = map[state.GuardOp]string{
	state.GuardEq: "==",
	state.GuardNe: "!=",
	state.GuardLt: "<",
	state.GuardLe: "<=",
	state.GuardGt: ">",
	state.GuardGe: ">=",
}

// lowerCompare renders a two-operand compare. It resolves each operand's kind
// against the schema and, when one side is an integer field/literal and the other a
// float, wraps the integer operand in double() so the comparison type-checks and
// evaluates with Core's numeric (float64) semantics.
func lowerCompare[S comparable](g *state.GuardNode[S], schema state.ContextSchema) (string, error) {
	if len(g.Children) != 2 {
		return "", fmt.Errorf("%s requires two operands, got %d", g.Op, len(g.Children))
	}
	// lowerNode only dispatches the six compare ops here, so the lookup always hits.
	op := celCompareOp[g.Op]
	left, lk, err := lowerOperand(&g.Children[0], schema)
	if err != nil {
		return "", err
	}
	right, rk, err := lowerOperand(&g.Children[1], schema)
	if err != nil {
		return "", err
	}
	left, right = injectNumericCasts(left, lk, right, rk)
	return left + " " + op + " " + right, nil
}

// lowerMembership renders an `in` membership test as `operand in [lit, ...]`,
// injecting a double() cast on the operand or the literals when the operand is an
// integer and the set holds floats (or vice versa), matching Core's numeric
// coercion across the membership compare.
func lowerMembership[S comparable](g *state.GuardNode[S], schema state.ContextSchema) (string, error) {
	if len(g.Children) != 1 {
		return "", fmt.Errorf("in requires one operand, got %d", len(g.Children))
	}
	if len(g.Set) == 0 {
		return "", fmt.Errorf("in membership has an empty set")
	}
	operand, ok, err := lowerOperand(&g.Children[0], schema)
	if err != nil {
		return "", err
	}
	setHasFloat := false
	for i := range g.Set {
		if schemaKindOfLiteral(g.Set[i]) == state.SchemaFloat {
			setHasFloat = true
		}
	}
	// Core coerces numerics to float64 across membership, so if either the operand or
	// any set element is a float, render every numeric side as a double.
	asDouble := (ok == state.SchemaInt && setHasFloat) ||
		(ok == state.SchemaFloat && setHasNumericInt(g.Set))
	if asDouble && ok == state.SchemaInt {
		operand = "double(" + operand + ")"
	}
	elems := make([]string, 0, len(g.Set))
	for i := range g.Set {
		lit := lowerLiteral(g.Set[i])
		if asDouble && schemaKindOfLiteral(g.Set[i]) == state.SchemaInt {
			lit = "double(" + lit + ")"
		}
		elems = append(elems, lit)
	}
	return operand + " in [" + strings.Join(elems, ", ") + "]", nil
}

// setHasNumericInt reports whether any literal in the set is an integer, used to
// decide whether a float operand's membership set needs double-casting on its int
// elements.
func setHasNumericInt(set []state.Literal) bool {
	for i := range set {
		if schemaKindOfLiteral(set[i]) == state.SchemaInt {
			return true
		}
	}
	return false
}

// lowerOperand renders a field-ref or literal operand to CEL source and reports its
// resolved SchemaKind, used by the cast-injection logic.
func lowerOperand[S comparable](g *state.GuardNode[S], schema state.ContextSchema) (string, state.SchemaKind, error) {
	switch g.Op {
	case state.GuardField:
		f, ok := schema.FieldAt(g.Path)
		if !ok {
			return "", "", fmt.Errorf("unknown field %q", g.Path)
		}
		return g.Path, f.Kind, nil
	case state.GuardLit:
		if g.Lit == nil {
			return "", "", fmt.Errorf("literal operand has no value")
		}
		return lowerLiteral(*g.Lit), schemaKindOfLiteral(*g.Lit), nil
	default:
		return "", "", fmt.Errorf("invalid operand op %q", g.Op)
	}
}

// injectNumericCasts wraps an integer operand in double() when it is compared
// against a float operand, so the lowered comparison matches Core's float64
// coercion. Same-kind and non-numeric pairs are returned unchanged.
func injectNumericCasts(left string, lk state.SchemaKind, right string, rk state.SchemaKind) (string, string) {
	if lk == state.SchemaInt && rk == state.SchemaFloat {
		return "double(" + left + ")", right
	}
	if lk == state.SchemaFloat && rk == state.SchemaInt {
		return left, "double(" + right + ")"
	}
	return left, right
}

// lowerLiteral renders a Core literal to a typed CEL literal token: a string is
// quoted, a duration uses the duration() constructor, an enum renders as a string,
// and numbers and bools render as their CEL literal form. Integers render without a
// decimal point (CEL int) and floats with one (CEL double).
func lowerLiteral(l state.Literal) string {
	switch l.Type {
	case state.StringParam, state.EnumParam:
		s, _ := l.Value.(string)
		return strconv.Quote(s)
	case state.BoolParam:
		b, _ := l.Value.(bool)
		return strconv.FormatBool(b)
	case state.DurationParam:
		s, _ := l.Value.(string)
		return "duration(" + strconv.Quote(s) + ")"
	case state.IntParam:
		return strconv.FormatInt(litInt64(l.Value), 10)
	case state.FloatParam:
		return formatFloat(litFloat64(l.Value))
	default:
		return fmt.Sprintf("%v", l.Value)
	}
}

// schemaKindOfLiteral maps a literal's ParamType to the SchemaKind it lowers as,
// mirroring the kernel's literal-to-kind mapping so cast injection sees the same
// numeric categories Core's type-check does.
func schemaKindOfLiteral(l state.Literal) state.SchemaKind {
	switch l.Type {
	case state.StringParam:
		return state.SchemaString
	case state.IntParam:
		return state.SchemaInt
	case state.FloatParam:
		return state.SchemaFloat
	case state.BoolParam:
		return state.SchemaBool
	case state.DurationParam:
		return state.SchemaDuration
	case state.EnumParam:
		return state.SchemaEnum
	default:
		return state.SchemaString
	}
}

// litInt64 normalizes a Core int literal value to int64. The builder carries an int
// literal as int64; a JSON round-trip rehydrates it as float64. Any other shape is a
// malformed literal and normalizes to zero.
func litInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		return 0
	}
}

// litFloat64 normalizes a Core float literal value to float64. The builder carries a
// float literal as float64; a JSON round-trip preserves that. An int64-valued float
// literal (an out-of-band construction) widens; any other shape normalizes to zero.
func litFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	default:
		return 0
	}
}

// formatFloat renders a float64 as a CEL double literal, always including a decimal
// point so CEL types it as a double rather than an int.
func formatFloat(f float64) string {
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}
	return s
}
