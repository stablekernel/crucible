package durable

import (
	"reflect"
	"strconv"
	"testing"

	alpha "github.com/stablekernel/crucible/durable/internal/typefixture/alpha/model"
	beta "github.com/stablekernel/crucible/durable/internal/typefixture/beta/model"
)

// TestTypeName_UnnamedTypesKeepTheImportPath answers whether a parked result's type
// name tells two packages' types apart inside an unnamed type: a struct literal, func
// or chan spelled from alpha's model.Result must differ from one spelled from beta's,
// as the named types do, or a decoder for the one settles a result parked as the other.
// A struct's tags and its unexported fields' packages are part of its type, so they
// are part of the name too.
func TestTypeName_UnnamedTypesKeepTheImportPath(t *testing.T) {
	type tagged struct {
		R alpha.Result `json:"r"`
	}
	cases := []struct {
		name string
		a, b any
	}{
		{"named", alpha.Result{}, beta.Result{}},
		{"struct field", struct{ R alpha.Result }{}, struct{ R beta.Result }{}},
		{"embedded field", struct{ alpha.Result }{}, struct{ beta.Result }{}},
		{"nested struct", struct{ S struct{ R *alpha.Result } }{}, struct{ S struct{ R *beta.Result } }{}},
		{"struct in a slice", []struct{ R alpha.Result }{}, []struct{ R beta.Result }{}},
		{"tag", struct {
			R alpha.Result `json:"r"`
		}{}, struct {
			R alpha.Result `json:"x"`
		}{}},
		{"named struct vs literal", tagged{}, struct {
			R alpha.Result `json:"r"`
		}{}},
		{"func", func(alpha.Result) error { return nil }, func(beta.Result) error { return nil }},
		{"func result", func() alpha.Result { return alpha.Result{} }, func() beta.Result { return beta.Result{} }},
		{"chan", make(chan alpha.Result), make(chan beta.Result)},
		{"chan direction", make(<-chan alpha.Result), make(chan<- alpha.Result)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if a, b := typeName(tc.a), typeName(tc.b); a == b {
				t.Fatalf("typeName gives %q for both %T and %T", a, tc.a, tc.b)
			}
		})
	}
}

// TestTypeName_SpellsUnnamedTypesAsGoDoes pins the form: reflect's own spelling, with
// every named type's import path in place of its package name, so a type with no
// named parts reads as reflect gives it.
func TestTypeName_SpellsUnnamedTypesAsGoDoes(t *testing.T) {
	const a = "github.com/stablekernel/crucible/durable/internal/typefixture/alpha/model.Result"
	cases := []struct {
		v    any
		want string
	}{
		{struct{}{}, "struct {}"},
		{struct {
			A int
			B string `json:"b"`
		}{}, `struct { A int; B string "json:\"b\"" }`},
		{struct{ R alpha.Result }{}, "struct { R " + a + " }"},
		{struct{ alpha.Result }{}, "struct { " + a + " }"},
		{struct{ r alpha.Result }{}, "struct { github.com/stablekernel/crucible/durable.r " + a + " }"},
		{func(int, ...alpha.Result) (bool, error) { return false, nil }, "func(int, ..." + a + ") (bool, error)"},
		{func() alpha.Result { return alpha.Result{} }, "func() " + a},
		{func() {}, "func()"},
		{make(chan alpha.Result), "chan " + a},
		{make(<-chan alpha.Result), "<-chan " + a},
		{make(chan<- alpha.Result), "chan<- " + a},
		{make(chan (<-chan int)), "chan (<-chan int)"},
		{map[string][]struct{ N int }{}, "map[string][]struct { N int }"},
	}
	for _, tc := range cases {
		if got := typeName(tc.v); got != tc.want {
			t.Errorf("typeName(%T) = %q, want %q", tc.v, got, tc.want)
		}
	}
}

// TestTypeName_InterfaceFieldKeepsTheImportPath covers an interface literal inside a
// struct literal, the one place an interface type can appear in a value's type.
func TestTypeName_InterfaceFieldKeepsTheImportPath(t *testing.T) {
	a := typeName(struct{ X interface{ M() alpha.Result } }{})
	b := typeName(struct{ X interface{ M() beta.Result } }{})
	if a == b {
		t.Fatalf("typeName gives %q for both", a)
	}
	if got, want := typeName(struct{ X any }{}), "struct { X interface {} }"; got != want {
		t.Fatalf("typeName = %q, want %q", got, want)
	}
}

// TestFullTypeName_CacheIsBounded answers whether runtime-minted types can grow
// typeNameCache without end: reflect.StructOf mints a new reflect.Type for every
// shape a caller asks, so spelling more distinct structs than the cap must leave
// the cache at the cap, with every spelling still the one its own type spells.
func TestFullTypeName_CacheIsBounded(t *testing.T) {
	const cap = 4
	oldLimit := typeNameCacheLimit
	typeNameCacheLimit = cap
	defer func() { typeNameCacheLimit = oldLimit }()
	typeNameCache.Range(func(k, _ any) bool { typeNameCache.Delete(k); return true })
	typeNameCacheLen.Store(0)

	fields := []reflect.StructField{{Name: "A", Type: reflect.TypeOf(0)}}
	for i := 0; i < 3*cap; i++ {
		fields = append(fields, reflect.StructField{
			Name: "F" + strconv.Itoa(i),
			Type: reflect.TypeOf(0),
		})
		minted := reflect.StructOf(fields)
		if got, want := fullTypeName(minted), spellTypeName(minted); got != want {
			t.Fatalf("fullTypeName(%s) = %q, want the uncached %q", minted, got, want)
		}
	}

	entries := 0
	typeNameCache.Range(func(_, _ any) bool { entries++; return true })
	if entries > cap {
		t.Fatalf("typeNameCache holds %d entries, want at most the cap of %d", entries, cap)
	}
}

// BenchmarkFullTypeName_DeepUnnamedType measures spelling a deeply unnamed type: a
// struct literal of a map of slices of funcs and chans built over two packages'
// named types, the shape a settle's result type is spelled from for every result of
// that type.
func BenchmarkFullTypeName_DeepUnnamedType(b *testing.B) {
	t := reflect.TypeOf(struct {
		Roster map[string][]func(alpha.Result) (*beta.Result, error)
		Feeds  []chan struct{ Head *alpha.Result }
	}{})
	b.ReportAllocs()
	var sink string
	for i := 0; i < b.N; i++ {
		sink = fullTypeName(t)
	}
	if sink == "" {
		b.Fatal("fullTypeName spelled nothing")
	}
}
