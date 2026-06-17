package query_test

import (
	"errors"
	"os"
	"sort"
	"testing"

	"github.com/stablekernel/crucible/cmd/crucible/internal/query"
	"github.com/stablekernel/crucible/state"
)

// loadFixture reads and parses a testdata fixture, failing on error.
func loadFixture(t *testing.T, name string) *state.IR[string, string, any] {
	t.Helper()
	b, err := os.ReadFile("../../testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	ir, err := state.LoadFromJSON[string, string, any](b)
	if err != nil {
		t.Fatalf("load fixture %s: %v", name, err)
	}
	return ir
}

// branchyIR builds a small fixture with multiple distinct simple paths from
// "a" to "d" so AllSimplePaths can be exercised and capped:
//
//	a -e-> b -e-> d
//	a -e-> c -e-> d
//	a -e-> d            (direct)
//
// Three simple acyclic paths a->d. Also a self-ish cycle d->a to prove the
// DFS does not loop forever.
func branchyIR() *state.IR[string, string, any] {
	tr := func(from, to, on string) state.Transition[string, string, any] {
		return state.Transition[string, string, any]{From: from, To: to, On: on}
	}
	return &state.IR[string, string, any]{
		Name:    "branchy",
		Initial: "a",
		States: []state.State[string, string, any]{
			{Name: "a", Transitions: []state.Transition[string, string, any]{
				tr("a", "b", "x"), tr("a", "c", "y"), tr("a", "d", "z"),
			}},
			{Name: "b", Transitions: []state.Transition[string, string, any]{tr("b", "d", "x")}},
			{Name: "c", Transitions: []state.Transition[string, string, any]{tr("c", "d", "x")}},
			{Name: "d", IsFinal: true, Transitions: []state.Transition[string, string, any]{tr("d", "a", "loop")}},
		},
	}
}

func TestReachableFrom_Linear(t *testing.T) {
	ir := loadFixture(t, "clean.json")
	got, err := query.ReachableFrom(ir, "paying")
	if err != nil {
		t.Fatalf("ReachableFrom: %v", err)
	}
	// From paying we reach done (and paying itself). cart is NOT reachable.
	if !got["paying"] || !got["done"] {
		t.Fatalf("expected paying and done reachable, got %v", sortedKeys(got))
	}
	if got["cart"] {
		t.Fatalf("cart must not be reachable from paying, got %v", sortedKeys(got))
	}
}

func TestReachableFrom_RootNotFound(t *testing.T) {
	ir := loadFixture(t, "clean.json")
	if _, err := query.ReachableFrom(ir, "nope"); err == nil {
		t.Fatal("expected error for unknown root")
	}
}

func TestReachableFrom_CompoundDescends(t *testing.T) {
	ir := loadFixture(t, "composite.json")
	// "active" is a composite whose InitialChild is "working".
	// Reaching active must descend to working, then working -submit-> review.
	got, err := query.ReachableFrom(ir, "active")
	if err != nil {
		t.Fatalf("ReachableFrom: %v", err)
	}
	for _, want := range []string{"active", "working", "review"} {
		if !got[want] {
			t.Fatalf("expected %s reachable from active, got %v", want, sortedKeys(got))
		}
	}
}

func TestReachableFrom_ParallelDescendsAllRegions(t *testing.T) {
	ir := loadFixture(t, "composite.json")
	// "parallel" has region regionA whose initial is ra1; ra1 -tick-> ra2.
	got, err := query.ReachableFrom(ir, "parallel")
	if err != nil {
		t.Fatalf("ReachableFrom: %v", err)
	}
	for _, want := range []string{"parallel", "ra1", "ra2"} {
		if !got[want] {
			t.Fatalf("expected %s reachable from parallel, got %v", want, sortedKeys(got))
		}
	}
}

func TestShortestPath_Linear(t *testing.T) {
	ir := loadFixture(t, "clean.json")
	p, found, err := query.ShortestPath(ir, "cart", "done")
	if err != nil {
		t.Fatalf("ShortestPath: %v", err)
	}
	if !found {
		t.Fatal("expected a path cart->done")
	}
	want := []query.Step{
		{From: "cart", To: "paying", Event: "checkout"},
		{From: "paying", To: "done", Event: "paid"},
	}
	if len(p) != len(want) {
		t.Fatalf("path length = %d, want %d (%v)", len(p), len(want), p)
	}
	for i := range want {
		if p[i] != want[i] {
			t.Fatalf("step %d = %+v, want %+v", i, p[i], want[i])
		}
	}
}

func TestShortestPath_NoPath_FoundFalse(t *testing.T) {
	ir := loadFixture(t, "clean.json")
	// done -> cart: no transition leaves done.
	p, found, err := query.ShortestPath(ir, "done", "cart")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatalf("expected no path done->cart, got %v", p)
	}
}

func TestShortestPath_UnknownEndpoint_Error(t *testing.T) {
	ir := loadFixture(t, "clean.json")
	if _, _, err := query.ShortestPath(ir, "cart", "ghost"); err == nil {
		t.Fatal("expected error for unknown 'to' endpoint")
	}
	if _, _, err := query.ShortestPath(ir, "ghost", "done"); err == nil {
		t.Fatal("expected error for unknown 'from' endpoint")
	}
}

func TestShortestPath_PicksShortest(t *testing.T) {
	ir := branchyIR()
	p, found, err := query.ShortestPath(ir, "a", "d")
	if err != nil {
		t.Fatalf("ShortestPath: %v", err)
	}
	if !found {
		t.Fatal("expected path a->d")
	}
	// The direct edge a->d (length 1) is shortest.
	if len(p) != 1 || p[0].From != "a" || p[0].To != "d" {
		t.Fatalf("expected single-step a->d, got %v", p)
	}
}

func TestAllSimplePaths_EnumeratesAll(t *testing.T) {
	ir := branchyIR()
	paths, truncated, err := query.AllSimplePaths(ir, "a", "d", 100)
	if err != nil {
		t.Fatalf("AllSimplePaths: %v", err)
	}
	if truncated {
		t.Fatal("did not expect truncation at cap 100")
	}
	if len(paths) != 3 {
		t.Fatalf("expected 3 simple paths a->d, got %d: %v", len(paths), paths)
	}
}

func TestAllSimplePaths_CapTruncates(t *testing.T) {
	ir := branchyIR()
	paths, truncated, err := query.AllSimplePaths(ir, "a", "d", 2)
	if err != nil {
		t.Fatalf("AllSimplePaths: %v", err)
	}
	if !truncated {
		t.Fatal("expected truncated=true at cap 2")
	}
	if len(paths) != 2 {
		t.Fatalf("expected exactly 2 paths at cap, got %d", len(paths))
	}
}

func TestAllSimplePaths_UnknownEndpoint_Error(t *testing.T) {
	ir := branchyIR()
	if _, _, err := query.AllSimplePaths(ir, "a", "zzz", 10); err == nil {
		t.Fatal("expected error for unknown endpoint")
	}
}

func TestAllSimplePaths_NonPositiveCap_Error(t *testing.T) {
	ir := branchyIR()
	if _, _, err := query.AllSimplePaths(ir, "a", "d", 0); err == nil {
		t.Fatal("expected error for non-positive cap")
	}
}

func TestResolveEndpoint_LeafAndCompound(t *testing.T) {
	ir := loadFixture(t, "composite.json")
	// Leaf name resolves to itself.
	id, err := query.ResolveEndpoint(ir, "working")
	if err != nil {
		t.Fatalf("resolve working: %v", err)
	}
	if id != "working" {
		t.Fatalf("resolve working = %q, want working", id)
	}
	// Compound name resolves to itself (the container node ID).
	id, err = query.ResolveEndpoint(ir, "active")
	if err != nil {
		t.Fatalf("resolve active: %v", err)
	}
	if id != "active" {
		t.Fatalf("resolve active = %q, want active", id)
	}
}

func TestResolveEndpoint_Unknown_Error(t *testing.T) {
	ir := loadFixture(t, "composite.json")
	_, err := query.ResolveEndpoint(ir, "missing")
	if err == nil {
		t.Fatal("expected error for unknown endpoint")
	}
	if !errors.Is(err, query.ErrUnknownState) {
		t.Fatalf("expected ErrUnknownState, got %v", err)
	}
}

func TestResolveEndpoint_Ambiguous_Error(t *testing.T) {
	// Two distinct states share the leaf name "dup" under different parents.
	ir := &state.IR[string, string, any]{
		Name:    "amb",
		Initial: "p1",
		States: []state.State[string, string, any]{
			{Name: "p1", Children: []state.State[string, string, any]{{Name: "dup"}}},
			{Name: "p2", Children: []state.State[string, string, any]{{Name: "dup"}}},
		},
	}
	_, err := query.ResolveEndpoint(ir, "dup")
	if err == nil {
		t.Fatal("expected error for ambiguous endpoint")
	}
	if !errors.Is(err, query.ErrAmbiguousState) {
		t.Fatalf("expected ErrAmbiguousState, got %v", err)
	}
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
