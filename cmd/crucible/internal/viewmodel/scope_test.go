package viewmodel_test

import (
	"os"
	"testing"

	"github.com/stablekernel/crucible/cmd/crucible/internal/viewmodel"
	"github.com/stablekernel/crucible/state"
)

func loadClean(t *testing.T) *state.IR[string, string, any] {
	t.Helper()
	b, err := os.ReadFile("../../testdata/clean.json")
	if err != nil {
		t.Fatalf("read clean fixture: %v", err)
	}
	ir, err := state.LoadFromJSON[string, string, any](b)
	if err != nil {
		t.Fatalf("load clean fixture: %v", err)
	}
	return ir
}

func hasNode(vm viewmodel.ViewModel, id string) bool {
	for i := range vm.Nodes {
		if vm.Nodes[i].ID == id {
			return true
		}
	}
	return false
}

func hasEdge(vm viewmodel.ViewModel, from, to string) bool {
	for i := range vm.Edges {
		if vm.Edges[i].From == from && vm.Edges[i].To == to {
			return true
		}
	}
	return false
}

// TestScope_Whole keeps every node and marks nothing on-path.
func TestScope_Whole(t *testing.T) {
	ir := loadClean(t)
	opts := fullOpts()
	opts.Scope = viewmodel.ScopeWhole
	vm := viewmodel.Build(ir, nil, opts)

	for _, id := range []string{"cart", "paying", "done"} {
		if !hasNode(vm, id) {
			t.Fatalf("whole scope should keep %s; have %v", id, nodeIDs(vm))
		}
	}
	if len(vm.Highlight) != 0 {
		t.Fatalf("whole scope should not highlight, got %v", vm.Highlight)
	}
	for i := range vm.Nodes {
		if vm.Nodes[i].OnPath {
			t.Fatalf("whole scope should not mark OnPath: %s", vm.Nodes[i].ID)
		}
	}
}

// TestScope_ReachableFrom keeps only the induced reachable subgraph.
func TestScope_ReachableFrom(t *testing.T) {
	ir := loadClean(t)
	opts := fullOpts()
	opts.Scope = viewmodel.ScopeReachableFrom
	opts.From = "paying"
	vm := viewmodel.Build(ir, nil, opts)

	if !hasNode(vm, "paying") || !hasNode(vm, "done") {
		t.Fatalf("reachable-from paying should keep paying+done; have %v", nodeIDs(vm))
	}
	if hasNode(vm, "cart") {
		t.Fatalf("reachable-from paying must drop cart; have %v", nodeIDs(vm))
	}
	// cart->paying edge must be dropped (cart pruned); paying->done kept.
	if hasEdge(vm, "cart", "paying") {
		t.Fatal("edge from pruned node cart must be dropped")
	}
	if !hasEdge(vm, "paying", "done") {
		t.Fatal("paying->done edge should be kept")
	}
}

// TestScope_PathShortest keeps the whole induced subgraph and marks on-path
// nodes/edges OnPath=true, leaving off-path elements present but OnPath=false.
func TestScope_PathShortest(t *testing.T) {
	ir := loadClean(t)
	opts := fullOpts()
	opts.Scope = viewmodel.ScopePath
	opts.Mode = viewmodel.ModeShortest
	opts.From = "cart"
	opts.To = "done"
	vm := viewmodel.Build(ir, nil, opts)

	// Induced subgraph from cart = all three states.
	for _, id := range []string{"cart", "paying", "done"} {
		if !hasNode(vm, id) {
			t.Fatalf("shortest mode keeps induced subgraph; missing %s", id)
		}
	}
	// On-path nodes marked.
	for _, id := range []string{"cart", "paying", "done"} {
		n := nodeByID(t, vm, id)
		if !n.OnPath {
			t.Fatalf("node %s should be OnPath in shortest path", id)
		}
	}
	// On-path edges marked.
	for _, fromTo := range [][2]string{{"cart", "paying"}, {"paying", "done"}} {
		e := edgeByFromTo(t, vm, fromTo[0], fromTo[1])
		if !e.OnPath {
			t.Fatalf("edge %s->%s should be OnPath", fromTo[0], fromTo[1])
		}
	}
	// Highlight lists the on-path node IDs.
	if len(vm.Highlight) != 3 {
		t.Fatalf("expected 3 highlighted nodes, got %v", vm.Highlight)
	}
}

// TestScope_PathTrace keeps ONLY the path nodes and edges.
func TestScope_PathTrace(t *testing.T) {
	ir := loadClean(t)
	opts := fullOpts()
	opts.Scope = viewmodel.ScopePath
	opts.Mode = viewmodel.ModeTrace
	opts.From = "cart"
	opts.To = "paying"
	vm := viewmodel.Build(ir, nil, opts)

	if !hasNode(vm, "cart") || !hasNode(vm, "paying") {
		t.Fatalf("trace should keep cart+paying; have %v", nodeIDs(vm))
	}
	if hasNode(vm, "done") {
		t.Fatalf("trace mode should drop off-path node done; have %v", nodeIDs(vm))
	}
	if !hasEdge(vm, "cart", "paying") {
		t.Fatal("trace should keep cart->paying")
	}
	if hasEdge(vm, "paying", "done") {
		t.Fatal("trace should drop off-path edge paying->done")
	}
	for i := range vm.Nodes {
		if !vm.Nodes[i].OnPath {
			t.Fatalf("trace nodes must all be OnPath: %s", vm.Nodes[i].ID)
		}
	}
}

// TestScope_PathAll keeps the union of all simple paths and marks them OnPath.
func TestScope_PathAll(t *testing.T) {
	ir := loadClean(t)
	opts := fullOpts()
	opts.Scope = viewmodel.ScopePath
	opts.Mode = viewmodel.ModeAll
	opts.From = "cart"
	opts.To = "done"
	opts.PathCap = 100
	vm := viewmodel.Build(ir, nil, opts)

	// clean.json has a single simple path cart->paying->done.
	for _, id := range []string{"cart", "paying", "done"} {
		if !hasNode(vm, id) {
			t.Fatalf("all mode union missing %s; have %v", id, nodeIDs(vm))
		}
		n := nodeByID(t, vm, id)
		if !n.OnPath {
			t.Fatalf("all-mode node %s should be OnPath", id)
		}
	}
}

// TestScope_CompositeEndpoint resolves a composite name and scopes correctly.
func TestScope_CompositeEndpoint(t *testing.T) {
	ir := loadComposite(t)
	opts := fullOpts()
	opts.Scope = viewmodel.ScopeReachableFrom
	opts.From = "active" // composite; descends to working->review.
	vm := viewmodel.Build(ir, nil, opts)

	for _, id := range []string{"active", "working", "review"} {
		if !hasNode(vm, id) {
			t.Fatalf("reachable-from composite active should include %s; have %v", id, nodeIDs(vm))
		}
	}
	// "parallel" and "done" are unreachable from active.
	if hasNode(vm, "ra1") {
		t.Fatalf("ra1 should be unreachable from active; have %v", nodeIDs(vm))
	}
}

// TestScope_UnknownEndpoint_GracefulEmpty: an unresolvable endpoint must not
// panic. Build degrades to an empty (or whole) projection rather than crashing.
func TestScope_UnknownEndpoint(t *testing.T) {
	ir := loadClean(t)
	opts := fullOpts()
	opts.Scope = viewmodel.ScopeReachableFrom
	opts.From = "ghost"
	// Should not panic. Build either returns the error via BuildScoped or
	// degrades; here we assert it does not include phantom nodes.
	vm := viewmodel.Build(ir, nil, opts)
	if len(vm.Nodes) != 0 {
		t.Fatalf("unknown endpoint should yield empty scope, got %v", nodeIDs(vm))
	}
}

// TestBuildScoped_ReturnsError surfaces endpoint errors to callers that want
// them, instead of silently degrading.
func TestBuildScoped_ReturnsError(t *testing.T) {
	ir := loadClean(t)
	opts := fullOpts()
	opts.Scope = viewmodel.ScopeReachableFrom
	opts.From = "ghost"
	_, err := viewmodel.BuildScoped(ir, nil, opts)
	if err == nil {
		t.Fatal("BuildScoped should return an error for unknown endpoint")
	}
}
