package state

import (
	"strings"
	"testing"
	"time"
)

// These white-box tests exercise the Core-expression helpers directly, covering
// the typed-comparison coercions, the reflective path reader's edge branches, and
// the rendering/diagnostic paths that a guard-driven Fire does not always reach.

func TestAsFloat_AllNumericKinds(t *testing.T) {
	vals := []any{
		int(1), int8(1), int16(1), int32(1), int64(1),
		uint(1), uint8(1), uint16(1), uint32(1), uint64(1),
		float32(1), float64(1),
	}
	for _, v := range vals {
		if f, ok := asFloat(v); !ok || f != 1 {
			t.Fatalf("asFloat(%T) = %v, %v; want 1, true", v, f, ok)
		}
	}
	if _, ok := asFloat("x"); ok {
		t.Fatal("asFloat(string) should not be numeric")
	}
}

func TestToInt64AndToFloat64(t *testing.T) {
	cases := []struct {
		in   any
		want int64
	}{
		{int64(5), 5}, {int(5), 5}, {float64(5.9), 5}, {float32(5.9), 5}, {"x", 0},
	}
	for _, c := range cases {
		if got := toInt64(c.in); got != c.want {
			t.Fatalf("toInt64(%v) = %d, want %d", c.in, got, c.want)
		}
	}
	if got := toFloat64(int32(7)); got != 7 {
		t.Fatalf("toFloat64(int32 7) = %v, want 7", got)
	}
	if got := toFloat64("nope"); got != 0 {
		t.Fatalf("toFloat64(string) = %v, want 0", got)
	}
}

func TestLiteralValue_Narrowing(t *testing.T) {
	if v := literalValue(Literal{Type: IntParam, Value: float64(3)}); v != int64(3) {
		t.Fatalf("int literal narrowing = %v (%T), want int64(3)", v, v)
	}
	if v := literalValue(Literal{Type: FloatParam, Value: int(3)}); v != float64(3) {
		t.Fatalf("float literal widening = %v (%T), want float64(3)", v, v)
	}
	if v := literalValue(Literal{Type: StringParam, Value: "x"}); v != "x" {
		t.Fatalf("string literal = %v, want x", v)
	}
}

func TestResolveContextPath_Errors(t *testing.T) {
	type inner struct {
		Tier string `json:"tier"`
	}
	type ctx struct {
		Name  string         `json:"name"`
		Ptr   *inner         `json:"ptr"`
		Bag   map[string]int `json:"bag"`
		IBag  map[int]int    `json:"ibag"`
		Plain int            `json:"plain"`
	}
	c := ctx{Name: "a", Ptr: &inner{Tier: "gold"}, Bag: map[string]int{"k": 9}}

	// Empty path returns the whole entity.
	if v, err := resolveContextPath(c, ""); err != nil || v == nil {
		t.Fatalf("empty path = %v, %v", v, err)
	}
	// Pointer descent.
	if v, err := resolveContextPath(c, "ptr.tier"); err != nil || v != "gold" {
		t.Fatalf("ptr.tier = %v, %v", v, err)
	}
	// Map index.
	if v, err := resolveContextPath(c, "bag.k"); err != nil || v != 9 {
		t.Fatalf("bag.k = %v, %v", v, err)
	}
	// Unknown field.
	if _, err := resolveContextPath(c, "nope"); err == nil {
		t.Fatal("expected unknown-field error")
	}
	// Nil pointer along path.
	if _, err := resolveContextPath(ctx{}, "ptr.tier"); err == nil {
		t.Fatal("expected nil-along-path error")
	}
	// Descend into a scalar.
	if _, err := resolveContextPath(c, "plain.x"); err == nil {
		t.Fatal("expected cannot-descend error")
	}
	// Non-string map key.
	if _, err := resolveContextPath(c, "ibag.k"); err == nil {
		t.Fatal("expected non-string-map-key error")
	}
	// Missing map key.
	if _, err := resolveContextPath(c, "bag.absent"); err == nil {
		t.Fatal("expected missing-map-key error")
	}
}

func TestCompareValues_DurationAndErrors(t *testing.T) {
	// Duration ordering across both operands.
	ok, err := compareValues(GuardLt, time.Minute, time.Hour)
	if err != nil || !ok {
		t.Fatalf("min < hour = %v, %v", ok, err)
	}
	ok, err = compareValues(GuardGe, time.Hour, time.Hour)
	if err != nil || !ok {
		t.Fatalf("hour >= hour = %v, %v", ok, err)
	}
	// Ordering two bools is an error (no ordering category).
	if _, berr := compareValues(GuardLt, true, false); berr == nil {
		t.Fatal("expected unordered-pair error for bools")
	}
	// ne over equal numbers.
	ok, err = compareValues(GuardNe, 1, 1)
	if err != nil || ok {
		t.Fatalf("1 != 1 = %v, %v; want false", ok, err)
	}
}

func TestValuesEqual_Categories(t *testing.T) {
	if eq, _ := valuesEqual(int64(2), float64(2)); !eq {
		t.Fatal("2 (int) == 2.0 (float) should be equal")
	}
	if eq, _ := valuesEqual(true, true); !eq {
		t.Fatal("bool equality")
	}
	if eq, _ := valuesEqual(time.Hour, time.Hour); !eq {
		t.Fatal("duration equality")
	}
	if eq, _ := valuesEqual("a", 1); eq {
		t.Fatal("cross-category should be unequal")
	}
}

func TestEvalCorePredicate_NonPredicateOp(t *testing.T) {
	// A field op is not a predicate; evalCorePredicate must surface a guard error.
	g := &GuardNode[string]{Op: GuardField, Path: "x"}
	if _, err := evalCorePredicate(g, struct{}{}); err == nil {
		t.Fatal("expected error for non-predicate op")
	}
}

func TestRenderGuardExpr_CoreOps(t *testing.T) {
	expr := Field[string]("total").Gt(Float[string](40))
	if got := renderGuardExpr(&expr); !strings.Contains(got, "total") || !strings.Contains(got, "gt") {
		t.Fatalf("compare render = %q", got)
	}
	in := Field[string]("status").In(Str[string]("paid"))
	if got := renderGuardExpr(&in); !strings.Contains(got, "in") || !strings.Contains(got, "paid") {
		t.Fatalf("membership render = %q", got)
	}
	lit := Operand[string]{lit: &Literal{Type: IntParam, Value: int64(7)}}.node()
	if got := renderGuardExpr(&lit); got != "7" {
		t.Fatalf("literal render = %q, want 7", got)
	}
}

func TestSchemaKindOfParam_AllTypes(t *testing.T) {
	cases := map[ParamType]SchemaKind{
		StringParam:    SchemaString,
		IntParam:       SchemaInt,
		FloatParam:     SchemaFloat,
		BoolParam:      SchemaBool,
		DurationParam:  SchemaDuration,
		EnumParam:      SchemaEnum,
		ParamType("?"): SchemaString,
	}
	for p, want := range cases {
		if got := schemaKindOfParam(p); got != want {
			t.Fatalf("schemaKindOfParam(%s) = %s, want %s", p, got, want)
		}
	}
}

func TestOperand_UninitializedNodeIsInvalid(t *testing.T) {
	// A zero Operand yields a literal node with no value, which validate rejects.
	n := Operand[string]{}.node()
	if n.Op != GuardLit || n.Lit != nil {
		t.Fatalf("zero operand node = %+v", n)
	}
	if err := n.validate(); err == nil {
		t.Fatal("expected validate to reject a value-less literal")
	}
}

func TestValidate_CoreOps(t *testing.T) {
	// Compare with wrong arity.
	bad := GuardNode[string]{Op: GuardEq, Children: []GuardNode[string]{Field[string]("a").node()}}
	if err := bad.validate(); err == nil {
		t.Fatal("expected arity error for one-operand compare")
	}
	// Compare with a non-operand child (a boolean node).
	badChild := GuardNode[string]{Op: GuardEq, Children: []GuardNode[string]{
		Field[string]("a").node(), And(Field[string]("b").Eq(Int[string](1))),
	}}
	if err := badChild.validate(); err == nil {
		t.Fatal("expected non-operand error")
	}
	// Membership with empty set.
	emptySet := GuardNode[string]{Op: GuardIn, Children: []GuardNode[string]{Field[string]("a").node()}}
	if err := emptySet.validate(); err == nil {
		t.Fatal("expected empty-set error")
	}
	// Field with no path.
	noPath := GuardNode[string]{Op: GuardField}
	if err := noPath.validate(); err == nil {
		t.Fatal("expected no-path error")
	}
}
