package analysis

import (
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// buildForbidFixture builds a machine where "stuck" only forbids an event, via
// the public Forge+Quench path.
func buildForbidFixture(t *testing.T) *state.Machine[string, string, any] {
	t.Helper()
	return state.Forge[string, string, any]("forbid").
		State("open").
		Transition("open").On("go").GoTo("stuck").
		Transition("open").On("done").GoTo("closed").
		State("stuck").
		Transition("stuck").Forbid("cancel").
		State("closed").Final().
		Initial("open").
		Quench()
}

// newGraph builds a minimal graph fixture for white-box checks. Forbidden edges
// are intentionally never added (flatten drops them), so to model "a forbidden
// transition" a test simply omits the edge.
func newGraph(initial string, names []string, edges []edge) *graph {
	g := &graph{
		nodes:      map[string]*node{},
		outgoing:   map[string][]edge{},
		initial:    initial,
		hasInitial: initial != "",
	}
	for _, n := range names {
		g.nodes[n] = &node{name: n}
		g.order = append(g.order, n)
	}
	for _, e := range edges {
		g.edges = append(g.edges, e)
		g.outgoing[e.from] = append(g.outgoing[e.from], e)
	}
	g.hasFinal = g.anyFinal()
	return g
}

// TestCheckUndefinedTargets_FlagsDanglingEdge proves checkUndefinedTargets emits a
// KindUndefinedTarget error for an edge whose target is not a declared node, and
// nothing for an edge to a declared node (F7). This is exercised at the graph
// layer because a dangling target is a hard Quench error, so such a machine never
// reaches Analyze through the public API; the check is defense-in-depth.
func TestCheckUndefinedTargets_FlagsDanglingEdge(t *testing.T) {
	g := newGraph("open", []string{"open", "closed"}, []edge{
		{from: "open", to: "closed", on: "done"},
		{from: "open", to: "nowhere", on: "go"},
	})

	var r Report
	checkUndefinedTargets(g, &r)

	got := r.OfKind(KindUndefinedTarget)
	if len(got) != 1 {
		t.Fatalf("expected exactly one undefined_target finding, got %d: %s", len(got), r)
	}
	if got[0].State != "open" {
		t.Fatalf("undefined_target state = %q, want open", got[0].State)
	}
	if got[0].Severity != SeverityError {
		t.Fatalf("undefined_target should be an error, got %s", got[0].Severity)
	}
	if !strings.Contains(got[0].Message, "nowhere") {
		t.Fatalf("message should name the undeclared target; got %q", got[0].Message)
	}
}

// TestCheckUndefinedTargets_NoFalsePositive proves a graph whose every edge lands
// on a declared node yields no finding.
func TestCheckUndefinedTargets_NoFalsePositive(t *testing.T) {
	g := newGraph("open", []string{"open", "closed"}, []edge{
		{from: "open", to: "closed", on: "done"},
	})
	var r Report
	checkUndefinedTargets(g, &r)
	if got := r.OfKind(KindUndefinedTarget); len(got) != 0 {
		t.Fatalf("no edge dangles, expected no findings; got: %s", r)
	}
}

// TestFlatten_DropsForbiddenEdges proves the flattened graph never contains an
// edge for a forbidden transition: its meaningless To must not invent an exit or
// reachability (F2). Built through the public Forge+Quench path.
func TestFlatten_DropsForbiddenEdges(t *testing.T) {
	m := buildForbidFixture(t)
	g, err := buildGraph(m)
	if err != nil {
		t.Fatalf("buildGraph: %v", err)
	}
	for _, e := range g.outgoing["stuck"] {
		t.Fatalf("forbidden transition must not appear as an edge; got %s", e.label())
	}
}
