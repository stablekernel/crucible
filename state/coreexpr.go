package state

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// This file ships the Core guard-expression layer: a small, dependency-free
// vocabulary that extends the existing GuardNode boolean spine with leaves the
// kernel evaluates directly against the context — a typed comparison, a
// field-ref, a typed literal, and a membership test. It is the structured-tree
// tier of a guard expression (the open kind: core), authored with a fluent
// builder, evaluated synchronously inside the pure Fire step, and type-checked at
// Quench against an attached ContextSchema.
//
// Core is a strict subset: only boolean composition, typed compare, membership,
// stateIn, field-ref, and literal. Arithmetic and map/object construction are
// deliberately excluded — they belong to the Rich tier (reserved by the
// GuardKindRich discriminant), where they can be checked against the schema with
// a full expression engine. Everything here is stdlib-only so the kernel stays
// dependency-free.
//
// NOTE: the Core↔Rich equivalence fuzz — generating Core trees and asserting the
// in-kernel evaluation matches the Rich engine's evaluation of the lowered form —
// lands with the Rich tier; this PR ships only the Core evaluator and its direct
// unit coverage.

// Literal is a typed constant operand in a Core expression: a value tagged with
// the ParamType vocabulary the palette already uses for ref params, so a single
// type language spans param schemas, the context schema, and Core literals. It
// serializes cleanly for the IR round-trip.
type Literal struct {
	// Type tags the literal's value type, drawn from the ParamType vocabulary
	// (string/int/float/bool/duration/enum). It drives type-checking against the
	// ContextSchema and the comparison's coercion rules.
	Type ParamType `json:"type"`
	// Value is the literal's value. It is held as the natural Go value for the
	// type (string, int64, float64, bool, or a duration string) and round-trips
	// through JSON; a duration is carried as its Go duration string.
	Value any `json:"value"`
}

// render renders a literal as a compact human-readable token for diagnostics and
// visualization.
func (l Literal) render() string {
	switch v := l.Value.(type) {
	case string:
		return strconv.Quote(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// renderLiteralSet renders a membership set as a compact token for diagnostics.
func renderLiteralSet(set []Literal) string {
	parts := make([]string, 0, len(set))
	for _, l := range set {
		parts = append(parts, l.render())
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// ---------------------------------------------------------------------------
// Core builder — fluent, functional-options-friendly authoring surface
// ---------------------------------------------------------------------------

// FieldRef is a Core field-ref operand under construction: a dotted context path
// that becomes either side of a comparison or the subject of a membership test.
// Obtain one with Field, then close it with a comparison (Eq/Ne/Lt/Le/Gt/Ge) or
// In to produce a GuardNode. FieldRef is parameterized by the state type so the
// produced node composes with And/Or/Not and StateIn over the same machine.
type FieldRef[S comparable] struct {
	path string
}

// Field opens a Core field-ref operand at the given dotted context path (e.g.
// "Status" or "order.total"). Close it with a comparison or In:
//
//	state.Field[string]("Status").In(state.Str("paid"), state.Str("settled"))
//	state.Field[string]("Balance").Gte(state.Param("amount"))
func Field[S comparable](path string) FieldRef[S] {
	return FieldRef[S]{path: path}
}

// node returns the field-ref as a GuardField operand node.
func (f FieldRef[S]) node() GuardNode[S] {
	return GuardNode[S]{Op: GuardField, Kind: GuardKindCore, Path: f.path}
}

// Eq builds a Core equality comparison between the field and the given operand.
func (f FieldRef[S]) Eq(operand Operand[S]) GuardNode[S] { return f.compare(GuardEq, operand) }

// Ne builds a Core inequality comparison between the field and the given operand.
func (f FieldRef[S]) Ne(operand Operand[S]) GuardNode[S] { return f.compare(GuardNe, operand) }

// Lt builds a Core less-than comparison: field < operand.
func (f FieldRef[S]) Lt(operand Operand[S]) GuardNode[S] { return f.compare(GuardLt, operand) }

// Le builds a Core less-than-or-equal comparison: field <= operand.
func (f FieldRef[S]) Le(operand Operand[S]) GuardNode[S] { return f.compare(GuardLe, operand) }

// Gt builds a Core greater-than comparison: field > operand.
func (f FieldRef[S]) Gt(operand Operand[S]) GuardNode[S] { return f.compare(GuardGt, operand) }

// Ge builds a Core greater-than-or-equal comparison: field >= operand.
func (f FieldRef[S]) Ge(operand Operand[S]) GuardNode[S] { return f.compare(GuardGe, operand) }

// In builds a Core membership test true when the field's value equals one of the
// given literal operands. Every operand must be a literal (Str/Int/Float/Bool/
// Dur/Param); a field operand in a membership set is rejected at Quench.
func (f FieldRef[S]) In(values ...Operand[S]) GuardNode[S] {
	set := make([]Literal, 0, len(values))
	for _, v := range values {
		if v.lit != nil {
			set = append(set, *v.lit)
		}
	}
	return GuardNode[S]{
		Op:       GuardIn,
		Kind:     GuardKindCore,
		Set:      set,
		Children: []GuardNode[S]{f.node()},
	}
}

// compare assembles a two-operand compare node from the field's left operand and
// the supplied right operand.
func (f FieldRef[S]) compare(op GuardOp, right Operand[S]) GuardNode[S] {
	return GuardNode[S]{
		Op:       op,
		Kind:     GuardKindCore,
		Children: []GuardNode[S]{f.node(), right.node()},
	}
}

// Operand is a Core comparison operand: either a field-ref or a typed literal.
// It is produced by Field (via FieldOp), Str/Int/Float/Bool/Dur, or Param, and
// consumed by the FieldRef comparison methods. The zero Operand is invalid.
type Operand[S comparable] struct {
	field *FieldRef[S]
	lit   *Literal
}

// node returns the operand as its GuardField or GuardLit node.
func (o Operand[S]) node() GuardNode[S] {
	if o.field != nil {
		return o.field.node()
	}
	if o.lit != nil {
		return GuardNode[S]{Op: GuardLit, Kind: GuardKindCore, Lit: o.lit}
	}
	// An uninitialized operand is a programmer error; surface it as an invalid
	// literal node so validate() rejects it at Quench rather than at Fire.
	return GuardNode[S]{Op: GuardLit, Kind: GuardKindCore}
}

// FieldOp wraps a field-ref as a comparison operand, so a comparison can put a
// field on either side (e.g. Field("a").Lt(FieldOp(Field("b")))).
func FieldOp[S comparable](f FieldRef[S]) Operand[S] { return Operand[S]{field: &f} }

// Str builds a string literal operand.
func Str[S comparable](v string) Operand[S] {
	return Operand[S]{lit: &Literal{Type: StringParam, Value: v}}
}

// Int builds an integer literal operand.
func Int[S comparable](v int64) Operand[S] {
	return Operand[S]{lit: &Literal{Type: IntParam, Value: v}}
}

// Float builds a floating-point literal operand.
func Float[S comparable](v float64) Operand[S] {
	return Operand[S]{lit: &Literal{Type: FloatParam, Value: v}}
}

// Bool builds a boolean literal operand.
func Bool[S comparable](v bool) Operand[S] {
	return Operand[S]{lit: &Literal{Type: BoolParam, Value: v}}
}

// Dur builds a duration literal operand, carried as its Go duration string.
func Dur[S comparable](v time.Duration) Operand[S] {
	return Operand[S]{lit: &Literal{Type: DurationParam, Value: v.String()}}
}

// Param builds an enum-typed string literal operand — a named, schema-validated
// constant such as an order status. It is tagged EnumParam so a comparison
// against an enum-kinded context field type-checks, while still comparing as a
// string at evaluation.
func Param[S comparable](v string) Operand[S] {
	return Operand[S]{lit: &Literal{Type: EnumParam, Value: v}}
}

// ---------------------------------------------------------------------------
// Build-time type-check against the ContextSchema
// ---------------------------------------------------------------------------

// typeCheckCoreExpr walks a guard expression tree and, for every Core leaf
// (compare or membership), type-checks its field-ref operands and literals
// against the schema: an unknown field path and a literal whose type cannot
// compare against the field's kind are reported as errors. Boolean and named-ref
// nodes carry no schema obligations and are recursed through. It returns one
// error per problem so Quench surfaces them all at once.
func typeCheckCoreExpr[S comparable](g *GuardNode[S], schema *ContextSchema) []error {
	if g == nil || schema == nil {
		return nil
	}
	var errs []error
	switch g.Op {
	case GuardEq, GuardNe, GuardLt, GuardLe, GuardGt, GuardGe:
		// Resolve both operand kinds; a field that does not resolve is reported,
		// and a literal whose type cannot compare against a resolved field kind is
		// reported.
		lk, lerr := operandKind(&g.Children[0], schema)
		errs = appendErr(errs, lerr)
		rk, rerr := operandKind(&g.Children[1], schema)
		errs = appendErr(errs, rerr)
		if lerr == nil && rerr == nil && !kindsComparable(lk, rk) {
			errs = append(errs, fmt.Errorf("cannot compare %s and %s with %s", lk, rk, g.Op))
		}
	case GuardIn:
		lk, lerr := operandKind(&g.Children[0], schema)
		errs = appendErr(errs, lerr)
		if lerr == nil {
			for i := range g.Set {
				if sk := schemaKindOfParam(g.Set[i].Type); !kindsComparable(lk, sk) {
					errs = append(errs, fmt.Errorf("membership value %s is not comparable to field of kind %s", g.Set[i].render(), lk))
				}
			}
		}
	default:
		for k := range g.Children {
			errs = append(errs, typeCheckCoreExpr(&g.Children[k], schema)...)
		}
	}
	return errs
}

// operandKind resolves the SchemaKind an operand contributes to a comparison: a
// field-ref resolves its path against the schema (an unresolved path is an
// error); a literal maps its ParamType to a SchemaKind.
func operandKind[S comparable](g *GuardNode[S], schema *ContextSchema) (SchemaKind, error) {
	switch g.Op {
	case GuardField:
		f, ok := schema.FieldAt(g.Path)
		if !ok {
			return "", fmt.Errorf("unknown field %q", g.Path)
		}
		return f.Kind, nil
	case GuardLit:
		if g.Lit == nil {
			return "", fmt.Errorf("literal operand has no value")
		}
		return schemaKindOfParam(g.Lit.Type), nil
	default:
		return "", fmt.Errorf("invalid operand op %q", g.Op)
	}
}

// schemaKindOfParam maps a literal's ParamType to the SchemaKind it compares
// against. The scalar param types share their wire string with the matching
// schema kind, and an enum literal compares against a string or enum field.
func schemaKindOfParam(p ParamType) SchemaKind {
	switch p {
	case StringParam:
		return SchemaString
	case IntParam:
		return SchemaInt
	case FloatParam:
		return SchemaFloat
	case BoolParam:
		return SchemaBool
	case DurationParam:
		return SchemaDuration
	case EnumParam:
		return SchemaEnum
	default:
		return SchemaString
	}
}

// kindsComparable reports whether two schema kinds may be compared in a Core
// expression. Numeric kinds (int/float) are mutually comparable; an enum literal
// compares against a string or enum field; otherwise the kinds must match.
func kindsComparable(a, b SchemaKind) bool {
	if a == b {
		return true
	}
	if isNumericKind(a) && isNumericKind(b) {
		return true
	}
	if isStringyKind(a) && isStringyKind(b) {
		return true
	}
	return false
}

// isNumericKind reports whether a schema kind is one of the numeric kinds that
// inter-compare.
func isNumericKind(k SchemaKind) bool { return k == SchemaInt || k == SchemaFloat }

// isStringyKind reports whether a schema kind is a string or an enum, which
// compare interchangeably (an enum is a constrained string).
func isStringyKind(k SchemaKind) bool { return k == SchemaString || k == SchemaEnum }

// appendErr appends err to errs only when err is non-nil.
func appendErr(errs []error, err error) []error {
	if err != nil {
		return append(errs, err)
	}
	return errs
}

// ---------------------------------------------------------------------------
// Core evaluator — in-kernel, stdlib-only, synchronous
// ---------------------------------------------------------------------------

// evalCorePredicate evaluates a Core predicate leaf (a compare or membership
// node) against the live context value. It reads each operand — a field-ref
// resolves the dotted path against the entity by reflection; a literal yields its
// constant — then compares them with the op's typed comparison. An unresolvable
// field path or an incomparable pair surfaces a typed ErrGuardPanic, matching how
// a named guard's failure surfaces, so a malformed Core guard fails the firing
// deterministically rather than silently passing.
func evalCorePredicate[S comparable, C any](g *GuardNode[S], entity C) (bool, error) {
	guardErr := func(reason string) error {
		return &ErrGuardPanic{GuardName: renderGuardExpr(g), Recovered: reason}
	}

	switch g.Op {
	case GuardIn:
		if len(g.Children) != 1 {
			return false, guardErr("membership requires one operand")
		}
		left, err := operandValue(&g.Children[0], entity)
		if err != nil {
			return false, guardErr(err.Error())
		}
		for i := range g.Set {
			eq, cerr := valuesEqual(left, literalValue(g.Set[i]))
			if cerr != nil {
				return false, guardErr(cerr.Error())
			}
			if eq {
				return true, nil
			}
		}
		return false, nil

	case GuardEq, GuardNe, GuardLt, GuardLe, GuardGt, GuardGe:
		if len(g.Children) != 2 {
			return false, guardErr("compare requires two operands")
		}
		left, err := operandValue(&g.Children[0], entity)
		if err != nil {
			return false, guardErr(err.Error())
		}
		right, err := operandValue(&g.Children[1], entity)
		if err != nil {
			return false, guardErr(err.Error())
		}
		ok, cerr := compareValues(g.Op, left, right)
		if cerr != nil {
			return false, guardErr(cerr.Error())
		}
		return ok, nil

	default:
		return false, guardErr("not a core predicate")
	}
}

// operandValue resolves an operand node to its Go value: a literal yields its
// constant; a field-ref resolves its dotted path against the entity.
func operandValue[S comparable, C any](g *GuardNode[S], entity C) (any, error) {
	switch g.Op {
	case GuardLit:
		if g.Lit == nil {
			return nil, fmt.Errorf("literal operand has no value")
		}
		return literalValue(*g.Lit), nil
	case GuardField:
		return resolveContextPath(entity, g.Path)
	default:
		return nil, fmt.Errorf("invalid operand op %q", g.Op)
	}
}

// literalValue normalizes a literal to the canonical Go value used in
// comparisons: integers to int64, floats to float64, everything else verbatim.
// JSON decoding yields float64 for every number, so a rehydrated int literal is
// re-narrowed here.
func literalValue(l Literal) any {
	switch l.Type {
	case IntParam:
		return toInt64(l.Value)
	case FloatParam:
		return toFloat64(l.Value)
	default:
		return l.Value
	}
}

// resolveContextPath reads the value at a dotted path off the live context by
// reflection, descending struct fields by their JSON wire name (matching the
// ContextSchema's field naming), dereferencing pointers, and indexing maps with
// string keys. A path that does not resolve is an error so a misauthored field
// reference fails loudly.
func resolveContextPath(entity any, path string) (any, error) {
	if path == "" {
		return entity, nil
	}
	cur := reflect.ValueOf(entity)
	for _, seg := range strings.Split(path, ".") {
		next, err := childValue(cur, seg)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", path, err)
		}
		cur = next
	}
	if !cur.IsValid() {
		return nil, nil
	}
	return cur.Interface(), nil
}

// childValue resolves one path segment against a reflect.Value, unwrapping
// pointers and interfaces, looking up a struct field by JSON wire name, and
// indexing a string-keyed map.
func childValue(v reflect.Value, seg string) (reflect.Value, error) {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return reflect.Value{}, fmt.Errorf("segment %q: nil along path", seg)
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			if sf.PkgPath != "" {
				continue
			}
			name, skip := jsonFieldName(sf)
			if skip {
				continue
			}
			if name == seg {
				return v.Field(i), nil
			}
		}
		return reflect.Value{}, fmt.Errorf("segment %q: no such field", seg)
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return reflect.Value{}, fmt.Errorf("segment %q: non-string map key", seg)
		}
		mv := v.MapIndex(reflect.ValueOf(seg).Convert(v.Type().Key()))
		if !mv.IsValid() {
			return reflect.Value{}, fmt.Errorf("segment %q: no such map key", seg)
		}
		return mv, nil
	default:
		return reflect.Value{}, fmt.Errorf("segment %q: cannot descend into %s", seg, v.Kind())
	}
}

// ---------------------------------------------------------------------------
// Typed comparison — the Core coercion rules
// ---------------------------------------------------------------------------

// compareValues applies a compare op to two resolved values, coercing numerics to
// a common form (int<->float compare numerically) and ordering strings
// lexicographically. eq/ne work across every comparable pair; the ordering ops
// require an ordered pair (numbers, strings, or durations). An unordered or
// mismatched pair is an error so a nonsensical comparison fails loudly rather
// than silently returning false.
func compareValues(op GuardOp, left, right any) (bool, error) {
	if op == GuardEq || op == GuardNe {
		eq, err := valuesEqual(left, right)
		if err != nil {
			return false, err
		}
		if op == GuardNe {
			return !eq, nil
		}
		return eq, nil
	}

	c, err := orderedCompare(left, right)
	if err != nil {
		return false, err
	}
	switch op {
	case GuardLt:
		return c < 0, nil
	case GuardLe:
		return c <= 0, nil
	case GuardGt:
		return c > 0, nil
	case GuardGe:
		return c >= 0, nil
	default:
		return false, fmt.Errorf("not an ordering op: %s", op)
	}
}

// valuesEqual reports whether two resolved values are equal under the Core rules:
// numbers compare numerically across int/float, strings and bools compare
// directly, and a duration compares against a duration. Cross-category pairs
// (e.g. string vs int) are unequal rather than an error, so an equality test
// against a differently typed field is simply false.
func valuesEqual(left, right any) (bool, error) {
	ln, lIsNum := asFloat(left)
	rn, rIsNum := asFloat(right)
	if lIsNum && rIsNum {
		return ln == rn, nil
	}
	ld, lIsDur := asDuration(left)
	rd, rIsDur := asDuration(right)
	if lIsDur && rIsDur {
		return ld == rd, nil
	}
	ls, lIsStr := asString(left)
	rs, rIsStr := asString(right)
	if lIsStr && rIsStr {
		return ls == rs, nil
	}
	lb, lIsBool := left.(bool)
	rb, rIsBool := right.(bool)
	if lIsBool && rIsBool {
		return lb == rb, nil
	}
	// Differently categorized operands are simply not equal.
	return false, nil
}

// orderedCompare returns -1, 0, or +1 comparing two ordered values, coercing
// numerics to float64, comparing durations as int64 nanoseconds, and strings
// lexicographically. A pair with no ordering (e.g. bool, or mismatched
// categories) is an error.
func orderedCompare(left, right any) (int, error) {
	if ld, ok := asDuration(left); ok {
		if rd, ok := asDuration(right); ok {
			return cmpInt64(int64(ld), int64(rd)), nil
		}
	}
	if ln, ok := asFloat(left); ok {
		if rn, ok := asFloat(right); ok {
			switch {
			case ln < rn:
				return -1, nil
			case ln > rn:
				return 1, nil
			default:
				return 0, nil
			}
		}
	}
	if ls, ok := asString(left); ok {
		if rs, ok := asString(right); ok {
			return strings.Compare(ls, rs), nil
		}
	}
	return 0, fmt.Errorf("operands are not ordered-comparable: %T vs %T", left, right)
}

// cmpInt64 returns the sign of a-b.
func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// asFloat coerces an integer or floating Go value (including the reflect-narrowed
// kinds a field read can yield) to float64. A duration is intentionally NOT a
// number here so a duration compares only against another duration.
func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

// asString coerces a string or a named string type to string.
func asString(v any) (string, bool) {
	if s, ok := v.(string); ok {
		return s, true
	}
	rv := reflect.ValueOf(v)
	if rv.IsValid() && rv.Kind() == reflect.String {
		return rv.String(), true
	}
	return "", false
}

// asDuration coerces a time.Duration value, or a duration-string literal, to a
// time.Duration. A literal carries its duration as a Go duration string; a field
// read yields a time.Duration directly.
func asDuration(v any) (time.Duration, bool) {
	switch d := v.(type) {
	case time.Duration:
		return d, true
	case string:
		// Only a parseable duration string counts; an arbitrary string does not.
		if parsed, err := time.ParseDuration(d); err == nil {
			return parsed, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// toInt64 narrows a JSON-decoded number (float64) or a Go integer to int64 for a
// canonical int literal value.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case float32:
		return int64(n)
	default:
		return 0
	}
}

// toFloat64 widens a JSON-decoded number or a Go numeric to float64 for a
// canonical float literal value.
func toFloat64(v any) float64 {
	if f, ok := asFloat(v); ok {
		return f
	}
	return 0
}
