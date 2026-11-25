package durable

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
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

	// The miss that races the cap: more distinct minted structs per goroutine than the
	// cap admits, so fills run while the cap is being crossed and past it.
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

// TestFullTypeName_ConcurrentSpellingsShareTheCache answers the interleavings the
// shared cache exists for: settles on different Handles spell the same result types
// at once - simultaneous first misses on one type, and hits under load - and a fill
// races the cap. Whatever the interleaving, every spelling must be its type's own,
// the counter must count each stored entry exactly once, and the fill can pass the
// cap only by as many spellings as raced it: a goroutine's store follows its own
// check of the counter, so at most one store per goroutine can be in flight past a
// check that saw the cap not yet crossed.
func TestFullTypeName_ConcurrentSpellingsShareTheCache(t *testing.T) {
	const goroutines = 8
	const cap = 4
	oldLimit := typeNameCacheLimit
	typeNameCacheLimit = cap
	defer func() { typeNameCacheLimit = oldLimit }()
	typeNameCache.Range(func(k, _ any) bool { typeNameCache.Delete(k); return true })
	typeNameCacheLen.Store(0)

	// The handful of result types every Handle settles: all goroutines spell these,
	// so their first misses overlap and their later spellings are hits under load.
	shared := []reflect.Type{
		reflect.TypeOf(alpha.Result{}),
		reflect.TypeOf(beta.Result{}),
		reflect.TypeOf(map[string][]*alpha.Result{}),
	}

	var wg sync.WaitGroup
	errs := make(chan string, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// More distinct minted structs than the cap admits, each spelled from the
			// shared types, so this goroutine's fills race the others' and the cap.
			fields := []reflect.StructField{{Name: "A", Type: shared[0]}}
			for i := 0; i < 3*cap; i++ {
				fields = append(fields, reflect.StructField{
					Name: "G" + strconv.Itoa(g) + "F" + strconv.Itoa(i),
					Type: shared[i%len(shared)],
				})
				minted := reflect.StructOf(fields)
				if got, want := fullTypeName(minted), spellTypeName(minted); got != want {
					errs <- fmt.Sprintf("goroutine %d: fullTypeName(%s) = %q, want the uncached %q", g, minted, got, want)
					return
				}
			}
			for _, s := range shared {
				if got, want := fullTypeName(s), spellTypeName(s); got != want {
					errs <- fmt.Sprintf("goroutine %d: fullTypeName(%s) = %q, want the uncached %q", g, s, got, want)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// The counter counts each stored entry once, however the LoadOrStores interleaved.
	entries := 0
	typeNameCache.Range(func(_, _ any) bool { entries++; return true })
	if got := typeNameCacheLen.Load(); got != int64(entries) {
		t.Fatalf("typeNameCacheLen counts %d, want the %d entries the cache holds", got, entries)
	}
	// The fill can pass the cap only by the spellings that raced it: at most one
	// in-flight store per goroutine past a check that saw the cap not yet crossed.
	if entries > cap-1+goroutines {
		t.Fatalf("typeNameCache holds %d entries, want at most the cap of %d plus the %d spellings that can race it", entries, cap, goroutines)
	}
}

// BenchmarkFullTypeName_DeepUnnamedType measures a deeply unnamed type's spelling
// on the cache's hit path: a struct literal of a map of slices of funcs and chans
// built over two packages' named types, the shape a settle's result type is spelled
// from for every result of that type. The first iteration spells it and stores it;
// every one after - the steady state for a settling run - is a hit, so what this
// reports is the hit, not the first spelling.
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
