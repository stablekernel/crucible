package analysis

import (
	"fmt"
	"sort"

	"github.com/stablekernel/crucible/state"
)

// Kind names a category of structural defect a static pass can find.
type Kind string

// The kinds of finding Analyze reports. Each is documented as exact (the IR
// proves it) or heuristic (the IR strongly suggests it, but a guard's runtime
// truth could change the verdict).
const (
	// KindUnreachableState marks a declared state with no inbound path from the
	// initial state over the transition graph. Exact: the graph proves it, since
	// reachability ignores guards (a guard can only ever remove an edge at run
	// time, never add one, so a statically unreachable state is unreachable in
	// every run).
	KindUnreachableState Kind = "unreachable_state"

	// KindDeadTransition marks a transition whose source state is itself
	// unreachable, so the edge can never fire. Exact, for the same reason as
	// KindUnreachableState. A transition that is live but whose guard is always
	// false is out of scope: guards are opaque host funcs and cannot be evaluated
	// statically.
	KindDeadTransition Kind = "dead_transition"

	// KindNondeterministic marks a state with two or more enabled transitions
	// that a static reader cannot disambiguate: multiple guardless transitions on
	// the same event, or multiple guardless eventless ("always") transitions.
	// Exact for the guardless case — nothing at run time can break the tie. A
	// guarded overlap (same event, but each transition carries a guard) is not
	// reported: whether the guards are mutually exclusive is a runtime property
	// the IR cannot decide.
	KindNondeterministic Kind = "nondeterministic"

	// KindDeadEnd marks a non-final state with no outgoing transitions: a state
	// you can enter but never leave, yet which is not declared terminal.
	// Heuristic: a state legitimately may be terminal-by-convention without the
	// IsFinal flag, or be left only by a parent/region transition in an HSM, so
	// this is a warning, not an error.
	KindDeadEnd Kind = "dead_end"

	// KindCannotReachFinal marks a state from which no final state is reachable
	// over the transition graph, in a machine that declares at least one final
	// state: an instance there can never complete. Heuristic, because the edges
	// out of the state may be guarded and the static graph treats every guarded
	// edge as traversable; if the only paths to a final state run through guards
	// that are always false at run time, the state is stuck despite looking live
	// here. Skipped entirely when the machine declares no final states.
	KindCannotReachFinal Kind = "cannot_reach_final"

	// KindDuplicateState marks two distinct declared states that flatten to the
	// same name. The analysis graph keys nodes by the state's rendered name, so a
	// collision (two states in different regions or branches whose names render
	// identically) would silently merge into one node and quietly mask the other
	// state's reachability, dead-end, and liveness defects. Surfacing it as a
	// finding makes the ambiguity explicit instead of analyzing a graph that does
	// not match the declared machine.
	KindDuplicateState Kind = "duplicate_state"

	// KindUndefinedTarget marks a transition whose target state is not a declared
	// state of the machine. Such an edge can never complete: the kernel cannot
	// enter a state that does not exist. Exact: the IR proves the target is absent
	// from the declared state set. A forbidden transition is exempt — its To field
	// carries no meaning — and so is never flagged here.
	KindUndefinedTarget Kind = "undefined_target"

	// KindInternalError marks a failure inside the analysis pass itself — most
	// commonly a machine whose serialized IR could not be read back. It is not a
	// defect in the analyzed machine but a signal that the pass could not run, so
	// the rest of the report (if any) is incomplete. Surfacing it as its own kind
	// keeps an analyzer/loader failure from masquerading as a machine defect such
	// as an unreachable state.
	KindInternalError Kind = "internal_error"
)

// Severity ranks a finding's seriousness.
type Severity string

// Finding severities. Error marks a defect the IR proves; Warning marks a
// heuristic finding a human should confirm.
const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

// Finding is a single classified result of a static analysis. State names the
// state the finding concerns; Transition is set (as "from -on-> to") only for
// transition-scoped findings.
type Finding struct {
	Kind       Kind
	Severity   Severity
	State      string
	Transition string
	Message    string
}

// Report is the full set of findings from a single Analyze pass. The zero
// Report (no findings) means the machine has no defects the enabled checks
// could find.
type Report struct {
	Findings []Finding
}

// Empty reports whether the analysis found nothing.
func (r Report) Empty() bool { return len(r.Findings) == 0 }

// HasErrors reports whether any finding is an error-severity (IR-proven) defect.
func (r Report) HasErrors() bool {
	for _, f := range r.Findings {
		if f.Severity == SeverityError {
			return true
		}
	}
	return false
}

// OfKind returns the findings of a single kind, in report order.
func (r Report) OfKind(k Kind) []Finding {
	var out []Finding
	for _, f := range r.Findings {
		if f.Kind == k {
			out = append(out, f)
		}
	}
	return out
}

// String renders the report as one line per finding, in report order.
func (r Report) String() string {
	if r.Empty() {
		return "no findings"
	}
	var b []byte
	for i, f := range r.Findings {
		if i > 0 {
			b = append(b, '\n')
		}
		loc := f.State
		if f.Transition != "" {
			loc = f.Transition
		}
		b = append(b, fmt.Sprintf("%-7s %-19s %s: %s", f.Severity, f.Kind, loc, f.Message)...)
	}
	return string(b)
}

// Analyze runs the enabled static checks over a Quenched machine and returns a
// classified Report. The machine's IR is read via its serialized form, so a
// machine built by the Forge DSL and one loaded from JSON analyze identically.
// The entity type parameter is unused at run time — no instance is cast and no
// guard or action is evaluated — but is required to name the machine's type.
//
// By default every check runs. Pass [Only] to restrict the pass to named kinds,
// or [Without] to exclude kinds. Findings are returned in a deterministic order:
// grouped by check, then by state name (and event), so the report is stable
// across runs.
func Analyze[S comparable, E comparable, C any](m *state.Machine[S, E, C], opts ...Option) Report {
	cfg := config{}
	for _, o := range opts {
		o(&cfg)
	}

	g, err := buildGraph(m)
	if err != nil {
		// A machine that cannot serialize/round-trip its own IR is an analyzer or
		// kernel failure, not a user defect; surface it as a single internal-error
		// finding rather than panicking (honoring the no-panic contract) and never
		// as a machine defect such as an unreachable state.
		return Report{Findings: []Finding{{
			Kind:     KindInternalError,
			Severity: SeverityWarning,
			Message:  fmt.Sprintf("analysis skipped: machine IR could not be read: %v", err),
		}}}
	}

	var r Report
	if cfg.enabled(KindDuplicateState) {
		for _, name := range g.duplicates {
			r.Findings = append(r.Findings, Finding{
				Kind:     KindDuplicateState,
				Severity: SeverityError,
				State:    name,
				Message:  fmt.Sprintf("state %q is declared more than once; its analysis node collides and other declarations are masked", name),
			})
		}
	}
	if cfg.enabled(KindUndefinedTarget) {
		checkUndefinedTargets(g, &r)
	}
	if cfg.enabled(KindUnreachableState) {
		checkUnreachable(g, &r)
	}
	if cfg.enabled(KindDeadTransition) {
		checkDeadTransitions(g, &r)
	}
	if cfg.enabled(KindNondeterministic) {
		checkNondeterminism(g, &r)
	}
	if cfg.enabled(KindDeadEnd) {
		checkDeadEnds(g, &r)
	}
	if cfg.enabled(KindCannotReachFinal) {
		checkLiveness(g, &r)
	}
	return r
}

// --- options ---------------------------------------------------------------

// Option configures an Analyze pass. Options compose: Only then Without narrows
// further. With no options every check runs.
type Option func(*config)

type config struct {
	only    map[Kind]bool // nil => all kinds
	without map[Kind]bool
}

// enabled reports whether a check should run under the current configuration.
func (c config) enabled(k Kind) bool {
	if c.without[k] {
		return false
	}
	if c.only != nil {
		return c.only[k]
	}
	return true
}

// Only restricts the pass to the named kinds. Repeated Only calls union their
// kinds.
func Only(kinds ...Kind) Option {
	return func(c *config) {
		if c.only == nil {
			c.only = map[Kind]bool{}
		}
		for _, k := range kinds {
			c.only[k] = true
		}
	}
}

// Without excludes the named kinds from the pass. Applied after Only.
func Without(kinds ...Kind) Option {
	return func(c *config) {
		if c.without == nil {
			c.without = map[Kind]bool{}
		}
		for _, k := range kinds {
			c.without[k] = true
		}
	}
}

// --- graph -----------------------------------------------------------------

// edge is one transition flattened out of the IR for graph analysis. Forbidden
// transitions (Forbid/ForbidAny) are NOT modeled as edges at all — they consume
// and drop an event and carry no meaningful target — so an edge is always a real,
// traversable transition. A wildcard (OnAny) edge is a real catch-all transition
// to its target, distinguished only so its label and event-overlap accounting do
// not treat the (meaningless) On field as a specific event.
type edge struct {
	from      string
	to        string
	on        string // event label; "" when eventless or wildcard
	eventLess bool
	wildcard  bool
	internal  bool
	guarded   bool
}

// label renders the edge for a finding's Transition field.
func (e edge) label() string {
	on := e.on
	switch {
	case e.wildcard:
		on = "*"
	case e.eventLess:
		on = "always"
	}
	return fmt.Sprintf("%s -%s-> %s", e.from, on, e.to)
}

// node is one state flattened out of the (possibly nested) IR.
type node struct {
	name string
	// final is true for a declared final state.
	final bool
	// compound is true for a state declaring child substates.
	compound bool
	// parallel is true for a state declaring orthogonal regions.
	parallel bool
	// initialChildren are the states entered when this state is entered: the
	// compound state's initial child, or each region's initial child. Used to
	// propagate reachability into a composite state's interior.
	initialChildren []string
	// children are all direct substate names (compound children or region
	// states), used to propagate reachability up: a reachable child implies a
	// reachable parent context, and a reachable parent implies its initial
	// descent.
	children []string
	// parent names the lexically enclosing composite state, "" for top level.
	parent string
}

// graph is the flattened, analyzable view of a machine's IR.
type graph struct {
	nodes      map[string]*node
	order      []string // node names in declaration order, for stable output
	edges      []edge
	outgoing   map[string][]edge
	initial    string
	hasInitial bool
	hasFinal   bool
	// duplicates names every state that collided with an already-recorded node
	// during flatten, in declaration order, so Analyze can surface the ambiguity.
	duplicates []string
}

// buildGraph serializes the machine to its IR and flattens the (possibly
// hierarchical) state graph into a single analyzable structure. Round-tripping
// through JSON is the same technique the evolution package uses to obtain a
// position-independent public IR, and it means a code-built and a JSON-loaded
// machine flatten identically.
func buildGraph[S comparable, E comparable, C any](m *state.Machine[S, E, C]) (*graph, error) {
	b, err := m.ToJSON()
	if err != nil {
		return nil, err
	}
	ir, err := state.LoadFromJSON[S, E, C](b)
	if err != nil {
		return nil, err
	}

	g := &graph{
		nodes:      map[string]*node{},
		outgoing:   map[string][]edge{},
		hasInitial: ir.HasInitial,
	}
	if ir.HasInitial {
		g.initial = fmt.Sprint(ir.Initial)
	}
	for i := range ir.States {
		flatten(g, &ir.States[i], "")
	}
	g.hasFinal = g.anyFinal()
	return g, nil
}

// flatten records one state and recurses through its children and regions,
// collecting nodes and edges. It treats unknown pseudo-state kinds
// conservatively: a state is modeled by its declared structure (children,
// regions, transitions, IsFinal) and any reserved kind it carries (history,
// invoke) is ignored for graph purposes rather than crashing the pass.
func flatten[S comparable, E comparable, C any](g *graph, s *state.State[S, E, C], parent string) {
	name := fmt.Sprint(s.Name)
	n := &node{
		name:     name,
		final:    s.IsFinal,
		compound: len(s.Children) > 0,
		parallel: len(s.Regions) > 0,
		parent:   parent,
	}

	for ti := range s.Transitions {
		t := &s.Transitions[ti]
		// A forbidden transition (Forbid/ForbidAny) consumes and drops an event; it
		// is not a real, traversable transition and its To field is meaningless, so
		// it is not modeled as an edge. Recording it as an edge would both invent a
		// false exit (masking a dead end) and invent false reachability into its junk
		// target.
		if t.Forbidden {
			continue
		}
		e := edge{
			from:      name,
			to:        fmt.Sprint(t.To),
			on:        fmt.Sprint(t.On),
			eventLess: t.EventLess,
			wildcard:  t.Wildcard,
			internal:  t.Internal,
			guarded:   len(t.Guards) > 0 || t.GuardExpr != nil,
		}
		g.edges = append(g.edges, e)
		g.outgoing[name] = append(g.outgoing[name], e)
	}

	for i := range s.Children {
		c := &s.Children[i]
		n.children = append(n.children, fmt.Sprint(c.Name))
	}
	if s.InitialChild != nil {
		n.initialChildren = append(n.initialChildren, fmt.Sprint(*s.InitialChild))
	}
	for ri := range s.Regions {
		r := &s.Regions[ri]
		if r.InitialChild != nil {
			n.initialChildren = append(n.initialChildren, fmt.Sprint(*r.InitialChild))
		}
		for i := range r.States {
			n.children = append(n.children, fmt.Sprint(r.States[i].Name))
		}
	}

	if _, exists := g.nodes[name]; exists {
		// A second state flattened to a name already recorded: the graph would
		// otherwise silently merge the two. Record the collision so Analyze reports
		// it; keep the first node so the rest of the pass stays deterministic.
		g.duplicates = append(g.duplicates, name)
	} else {
		g.nodes[name] = n
		g.order = append(g.order, name)
	}

	for i := range s.Children {
		flatten(g, &s.Children[i], name)
	}
	for ri := range s.Regions {
		for i := range s.Regions[ri].States {
			flatten(g, &s.Regions[ri].States[i], name)
		}
	}
}

// anyFinal reports whether any node is a declared final state.
func (g *graph) anyFinal() bool {
	for _, n := range g.nodes {
		if n.final {
			return true
		}
	}
	return false
}

// reachable computes the set of states reachable from the initial state over
// the transition graph, by the same breadth-first walk the kernel's PlanPath
// uses — but guard-agnostic, because a static pass cannot evaluate guards and a
// guard can only ever prune an edge, never add one. Internal (self) transitions
// add no reachability. Entering a composite state implies entering its initial
// descent; a reachable child implies its enclosing parent context is reachable.
func (g *graph) reachable() map[string]bool {
	seen := map[string]bool{}
	if !g.hasInitial {
		return seen
	}
	var queue []string
	visit := func(name string) {
		if _, ok := g.nodes[name]; !ok {
			return
		}
		if seen[name] {
			return
		}
		seen[name] = true
		queue = append(queue, name)
	}

	visit(g.initial)
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		n := g.nodes[cur]
		if n == nil {
			continue
		}
		// Entering a composite state cascades into its initial children.
		for _, ic := range n.initialChildren {
			visit(ic)
		}
		// Reaching a substate makes its enclosing context active; entering the
		// parent in turn cascades into the parent's initial descent.
		if n.parent != "" {
			visit(n.parent)
		}
		// Follow outgoing event/eventless transitions (internal edges re-enter the
		// same state and add nothing).
		for _, e := range g.outgoing[cur] {
			if e.internal {
				continue
			}
			visit(e.to)
		}
	}
	return seen
}

// canReachFinal computes, by reverse reachability from every final state, the
// set of states from which some final state is reachable over the transition
// graph. A state not in the returned set can never complete.
func (g *graph) canReachFinal() map[string]bool {
	// Build reverse adjacency over non-internal edges plus the structural
	// implications used in forward reachability (parent <-> initial descent), so
	// "reaches final" matches "reachable" semantics in reverse.
	rev := map[string][]string{}
	addRev := func(from, to string) {
		if _, ok := g.nodes[to]; !ok {
			return
		}
		rev[to] = append(rev[to], from)
	}
	for _, e := range g.edges {
		if e.internal {
			continue
		}
		addRev(e.from, e.to)
	}
	for name, n := range g.nodes {
		for _, ic := range n.initialChildren {
			addRev(name, ic)
		}
		if n.parent != "" {
			addRev(name, n.parent)
		}
	}

	can := map[string]bool{}
	var queue []string
	for _, name := range g.order {
		if g.nodes[name].final {
			can[name] = true
			queue = append(queue, name)
		}
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, pred := range rev[cur] {
			if !can[pred] {
				can[pred] = true
				queue = append(queue, pred)
			}
		}
	}
	return can
}

// sortFindings orders findings deterministically: by kind, then by the location
// (state or transition), then by message — so a report is reproducible.
func sortFindings(fs []Finding) {
	sort.SliceStable(fs, func(i, j int) bool {
		a, b := fs[i], fs[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		la, lb := a.State, b.State
		if a.Transition != "" {
			la = a.Transition
		}
		if b.Transition != "" {
			lb = b.Transition
		}
		if la != lb {
			return la < lb
		}
		return a.Message < b.Message
	})
}
