// Package viewmodel transforms a crucible state-machine IR into a flat,
// render-agnostic view model suitable for diagramming.
//
// It performs no rendering, emits no D2/Mermaid/DOT, and depends only on the
// crucible state package. A caller chooses how much detail to project via a
// cumulative DetailLevel ladder plus per-dimension Show/Hide overrides, and the
// resulting ViewModel is a deterministic, ordered description of nodes, edges,
// and containers.
package viewmodel

import (
	"fmt"
	"strings"

	"github.com/stablekernel/crucible/cmd/crucible/internal/query"
	"github.com/stablekernel/crucible/state"
)

// NodeKind classifies a single state node for rendering.
type NodeKind string

// Node kinds. The classification is mutually exclusive; see Build for the
// precedence rules used to pick exactly one kind per state.
const (
	NodeInitial   NodeKind = "initial"
	NodeAtomic    NodeKind = "atomic"
	NodeComposite NodeKind = "composite"
	NodeParallel  NodeKind = "parallel"
	NodeFinal     NodeKind = "final"
	NodeHistory   NodeKind = "history"
	NodeInvoke    NodeKind = "invoke"
)

// EdgeKind classifies a transition for rendering.
type EdgeKind string

// Edge kinds. The classification is mutually exclusive; see Build for the
// precedence rules used to pick exactly one kind per transition.
const (
	EdgeEvent     EdgeKind = "event"
	EdgeEventless EdgeKind = "eventless"
	EdgeDelayed   EdgeKind = "delayed"
	EdgeForbidden EdgeKind = "forbidden"
	EdgeWildcard  EdgeKind = "wildcard"
	EdgeInternal  EdgeKind = "internal"
)

// DetailItem is a resolved reference (guard, effect, assign, lifecycle action,
// or invocation source). Name is always set. Category and Description are
// populated from the resolver subject to the active projection.
type DetailItem struct {
	Name        string
	Category    string
	Description string
}

// ViewNode is one state rendered as a node. Lifecycle slices (Entry/Exit/Done)
// and Invoke are populated only when the projection includes the corresponding
// dimension. OnPath marks a node that lies on a highlighted path; it is set by
// path-scoped projections and is false otherwise.
type ViewNode struct {
	ID     string
	Name   string
	Kind   NodeKind
	Entry  []DetailItem
	Exit   []DetailItem
	Done   []DetailItem
	Invoke []DetailItem
	OnPath bool
}

// ViewEdge is one transition. Guards/Effects/Assigns are populated subject to
// the projection. After is the formatted delay string, present only when the
// delays dimension is included. OnPath marks edges on a highlighted path and is
// always false in this increment.
type ViewEdge struct {
	From    string
	To      string
	Event   string
	Guards  []DetailItem
	Effects []DetailItem
	Assigns []DetailItem
	Kind    EdgeKind
	After   string
	OnPath  bool
}

// ViewContainer groups child nodes. Kind is one of "composite", "parallel", or
// "region". Children lists child node IDs in document order.
type ViewContainer struct {
	ID       string
	Name     string
	Kind     string
	Children []string
}

// ViewModel is the complete, render-agnostic projection of a machine. Highlight
// lists node IDs to emphasize and is always empty in this increment.
type ViewModel struct {
	Nodes      []ViewNode
	Edges      []ViewEdge
	Containers []ViewContainer
	Highlight  []string
}

// DetailLevel is a cumulative ladder: each level implies every dimension of the
// levels below it. The zero value is Outline.
type DetailLevel int

// Detail levels, in increasing order. Default for callers that do not set a
// level is Actions (see DefaultLevel).
const (
	Outline DetailLevel = iota
	Guards
	Actions
	Lifecycle
	Full
)

// DefaultLevel is the level a caller should use when none is specified.
const DefaultLevel = Actions

// Dimension names an independently toggleable facet of detail.
type Dimension string

// The full set of dimensions.
const (
	DimGuards        Dimension = "guards"
	DimEffects       Dimension = "effects"
	DimAssigns       Dimension = "assigns"
	DimEntryExit     Dimension = "entryExit"
	DimInvoke        Dimension = "invoke"
	DimDelays        Dimension = "delays"
	DimDescriptions  Dimension = "descriptions"
	DimDataFlow      Dimension = "dataFlow"
	DimContextSchema Dimension = "contextSchema"
	DimSource        Dimension = "source"
)

// dimMinLevel maps each dimension to the minimum DetailLevel that implies it.
var dimMinLevel = map[Dimension]DetailLevel{
	DimGuards:        Guards,
	DimEffects:       Actions,
	DimAssigns:       Actions,
	DimEntryExit:     Lifecycle,
	DimInvoke:        Lifecycle,
	DimDelays:        Full,
	DimDescriptions:  Full,
	DimDataFlow:      Full,
	DimContextSchema: Full,
	DimSource:        Full,
}

// Scope selects which portion of the machine a projection retains.
type Scope string

// Scope values. The zero value is ScopeWhole.
const (
	// ScopeWhole keeps every node and marks nothing on-path.
	ScopeWhole Scope = ""
	// ScopeReachableFrom keeps only the subgraph reachable from From.
	ScopeReachableFrom Scope = "reachableFromA"
	// ScopePath keeps a path (or its surrounding subgraph) between From and To,
	// shaped by Mode.
	ScopePath Scope = "path"
)

// Mode selects how a ScopePath projection treats the path between From and To.
type Mode string

// Mode values. The zero value is ModeShortest.
const (
	// ModeShortest keeps the whole subgraph induced by From's reachability,
	// marks the single shortest path OnPath=true, and leaves off-path elements
	// present but OnPath=false (dimmed).
	ModeShortest Mode = ""
	// ModeAll keeps the union of all simple paths from From to To, all OnPath.
	ModeAll Mode = "all"
	// ModeTrace keeps ONLY the shortest path's nodes and edges, all OnPath.
	ModeTrace Mode = "trace"
)

// DefaultPathCap bounds AllSimplePaths enumeration when a caller leaves PathCap
// unset (zero) in ModeAll.
const DefaultPathCap = 1000

// ProjectionOptions controls how much detail Build projects and how the result
// is scoped. Level sets the cumulative detail baseline; Show forces dimensions
// on and Hide forces them off. Scope/Mode/From/To/PathCap drive path-and-scope
// filtering applied after the full projection is built.
type ProjectionOptions struct {
	Level DetailLevel
	Show  []Dimension
	Hide  []Dimension

	// Scope and Mode select the graph-query filter; From/To are endpoint node
	// IDs (bare state names, matching ViewNode.ID). PathCap bounds ModeAll
	// enumeration (defaults to DefaultPathCap when zero).
	Scope   Scope
	Mode    Mode
	From    string
	To      string
	PathCap int
}

// included reports whether a dimension is part of the projection.
//
// Precedence: Show wins over Hide. If a dimension appears in Show it is always
// included, even when also listed in Hide. Otherwise, if it appears in Hide it
// is excluded. Otherwise it is included iff the active Level implies it (the
// Level meets or exceeds the dimension's minimum level).
func included(opts ProjectionOptions, dim Dimension) bool {
	for _, d := range opts.Show {
		if d == dim {
			return true
		}
	}
	for _, d := range opts.Hide {
		if d == dim {
			return false
		}
	}
	minLvl, ok := dimMinLevel[dim]
	if !ok {
		return false
	}
	return opts.Level >= minLvl
}

// Included is the exported form of the included projection helper, exposed for
// testing and for callers that build their own projections.
func Included(opts ProjectionOptions, dim Dimension) bool {
	return included(opts, dim)
}

// RefResolver indexes palette descriptors by name so refs can be enriched with
// category and description metadata. Machine (user) descriptors take precedence
// over builtins on a name collision; builtins fill only the names the machine
// palette does not define.
type RefResolver struct {
	byName map[string]state.Descriptor
}

// NewRefResolver builds a resolver from the builtin palette and the machine
// palette. Builtins are indexed first, then machine descriptors overwrite any
// colliding names, so machine descriptors win. Either slice may be nil.
func NewRefResolver(builtin []state.Descriptor, machine []state.Descriptor) *RefResolver {
	idx := make(map[string]state.Descriptor, len(builtin)+len(machine))
	for i := range builtin {
		idx[builtin[i].Name] = builtin[i]
	}
	for i := range machine {
		idx[machine[i].Name] = machine[i]
	}
	return &RefResolver{byName: idx}
}

// Resolve returns the descriptor registered under name and whether it was found.
func (r *RefResolver) Resolve(name string) (state.Descriptor, bool) {
	if r == nil || r.byName == nil {
		return state.Descriptor{}, false
	}
	d, ok := r.byName[name]
	return d, ok
}

// Build walks the IR recursively (children and regions) and produces a flat,
// deterministic ViewModel under the given projection.
//
// Container model: every state becomes a ViewNode. A composite or parallel
// state additionally emits a ViewContainer listing its child node IDs, and each
// Region emits a separate "region" ViewContainer listing the IDs of the states
// in that region. This keeps nodes and grouping orthogonal so a renderer can
// lay out nodes independently of nesting.
//
// NodeKind precedence (first match wins):
//
//	final > history > parallel > composite > invoke > initial > atomic
//
// Note "active" is both composite (it has children) and the initial state;
// composite precedes initial, so it classifies as composite.
//
// EdgeKind precedence (first match wins):
//
//	forbidden > wildcard > eventless > delayed > internal > event
//
// Determinism: slices are ranged by index and never produced by ranging a map,
// so node, edge, and container order follows document order. A nil palette is
// handled gracefully — refs fall back to their raw name with empty metadata.
func Build(ir *state.IR[string, string, any], palette *RefResolver, opts ProjectionOptions) ViewModel {
	vm, err := BuildScoped(ir, palette, opts)
	if err != nil {
		// Build degrades to an empty projection on a scoping error rather than
		// panicking; callers that need the error should use BuildScoped.
		return ViewModel{Highlight: []string{}}
	}
	return vm
}

// BuildScoped is Build plus error reporting for the scope/mode query. It builds
// the full projection, then applies Scope/Mode filtering and OnPath marking.
// It returns an error when From/To do not resolve, or when ScopePath has no
// path between them, so callers can surface a friendly message instead of an
// empty diagram.
func BuildScoped(ir *state.IR[string, string, any], palette *RefResolver, opts ProjectionOptions) (ViewModel, error) {
	vm := ViewModel{Highlight: []string{}}
	if ir == nil {
		return vm, nil
	}
	for i := range ir.States {
		walkState(&vm, &ir.States[i], ir.Initial, palette, opts)
	}
	return applyScope(vm, ir, opts)
}

// walkState appends the node, edges, container, and lifecycle/invoke detail for
// one state, then recurses into its children and regions.
func walkState(
	vm *ViewModel,
	s *state.State[string, string, any],
	initial string,
	palette *RefResolver,
	opts ProjectionOptions,
) {
	node := ViewNode{
		ID:   s.Name,
		Name: s.Name,
		Kind: classifyNode(s, initial),
	}
	if included(opts, DimEntryExit) {
		node.Entry = resolveRefs(append(cloneRefs(s.OnEntry), s.OnEntryAssign...), palette, opts)
		node.Exit = resolveRefs(append(cloneRefs(s.OnExit), s.OnExitAssign...), palette, opts)
		node.Done = resolveRefs(s.OnDone, palette, opts)
	}
	if included(opts, DimInvoke) {
		node.Invoke = resolveInvokes(s.Invoke, palette, opts)
	}
	vm.Nodes = append(vm.Nodes, node)

	for i := range s.Transitions {
		vm.Edges = append(vm.Edges, buildEdge(&s.Transitions[i], palette, opts))
	}

	// Composite / parallel containers.
	switch {
	case len(s.Regions) > 0:
		// Emit the parallel container first, then one region container per
		// region, so a renderer sees the parent before the groups it owns.
		container := ViewContainer{ID: s.Name, Name: s.Name, Kind: "parallel"}
		regionContainers := make([]ViewContainer, 0, len(s.Regions))
		for i := range s.Regions {
			r := &s.Regions[i]
			rc := ViewContainer{ID: r.Name, Name: r.Name, Kind: "region"}
			for j := range r.States {
				rc.Children = append(rc.Children, r.States[j].Name)
			}
			container.Children = append(container.Children, r.Name)
			regionContainers = append(regionContainers, rc)
		}
		vm.Containers = append(vm.Containers, container)
		vm.Containers = append(vm.Containers, regionContainers...)
	case len(s.Children) > 0:
		container := ViewContainer{ID: s.Name, Name: s.Name, Kind: "composite"}
		for i := range s.Children {
			container.Children = append(container.Children, s.Children[i].Name)
		}
		vm.Containers = append(vm.Containers, container)
	}

	// Recurse into children, then region states.
	for i := range s.Children {
		walkState(vm, &s.Children[i], initial, palette, opts)
	}
	for i := range s.Regions {
		r := &s.Regions[i]
		for j := range r.States {
			walkState(vm, &r.States[j], initial, palette, opts)
		}
	}
}

// classifyNode applies the documented NodeKind precedence.
func classifyNode(s *state.State[string, string, any], initial string) NodeKind {
	switch {
	case s.IsFinal:
		return NodeFinal
	case s.HistoryType != state.HistoryNone:
		return NodeHistory
	case len(s.Regions) > 0:
		return NodeParallel
	case len(s.Children) > 0:
		return NodeComposite
	case len(s.Invoke) > 0:
		return NodeInvoke
	case s.Name == initial:
		return NodeInitial
	default:
		return NodeAtomic
	}
}

// buildEdge converts one transition into a ViewEdge under the projection.
func buildEdge(
	t *state.Transition[string, string, any],
	palette *RefResolver,
	opts ProjectionOptions,
) ViewEdge {
	e := ViewEdge{
		From:  t.From,
		To:    t.To,
		Event: t.On,
		Kind:  classifyEdge(t),
	}
	if t.After != nil && included(opts, DimDelays) {
		e.After = t.After.String()
	}
	if included(opts, DimGuards) {
		e.Guards = resolveRefs(t.Guards, palette, opts)
	}
	if included(opts, DimEffects) {
		e.Effects = resolveRefs(t.Effects, palette, opts)
	}
	if included(opts, DimAssigns) {
		e.Assigns = resolveRefs(t.Assigns, palette, opts)
	}
	return e
}

// classifyEdge applies the documented EdgeKind precedence.
func classifyEdge(t *state.Transition[string, string, any]) EdgeKind {
	switch {
	case t.Forbidden:
		return EdgeForbidden
	case t.Wildcard:
		return EdgeWildcard
	case t.EventLess:
		return EdgeEventless
	case t.After != nil:
		return EdgeDelayed
	case t.Internal:
		return EdgeInternal
	default:
		return EdgeEvent
	}
}

// cloneRefs returns a fresh copy of a Ref slice so appends do not mutate the IR.
func cloneRefs(refs []state.Ref) []state.Ref {
	return append([]state.Ref(nil), refs...)
}

// resolveRefs maps each Ref to a DetailItem, enriching via the palette subject
// to the projection. A nil palette or unknown name falls back to the raw name
// with empty metadata.
func resolveRefs(refs []state.Ref, palette *RefResolver, opts ProjectionOptions) []DetailItem {
	if len(refs) == 0 {
		return nil
	}
	out := make([]DetailItem, 0, len(refs))
	for i := range refs {
		out = append(out, resolveOne(refs[i].Name, palette, opts))
	}
	return out
}

// resolveInvokes maps invocation sources to DetailItems.
func resolveInvokes(inv []state.Invocation[string, string, any], palette *RefResolver, opts ProjectionOptions) []DetailItem {
	if len(inv) == 0 {
		return nil
	}
	out := make([]DetailItem, 0, len(inv))
	for i := range inv {
		out = append(out, resolveOne(inv[i].Src.Name, palette, opts))
	}
	return out
}

// resolveOne builds a single DetailItem. Category is filled whenever the
// resolver knows the name (cheap). Description is filled only when the
// descriptions dimension is included; data-flow Reads/Writes are folded into
// the description only when the dataFlow dimension is included.
func resolveOne(name string, palette *RefResolver, opts ProjectionOptions) DetailItem {
	item := DetailItem{Name: name}
	if palette == nil {
		return item
	}
	d, ok := palette.Resolve(name)
	if !ok {
		return item
	}
	item.Category = d.Category
	if included(opts, DimDescriptions) {
		item.Description = d.Description
		if included(opts, DimDataFlow) {
			item.Description = appendDataFlow(item.Description, d.Reads, d.Writes)
		}
	}
	return item
}

// applyScope filters and marks a fully-built ViewModel per the scope/mode
// options. ScopeWhole returns vm unchanged. ScopeReachableFrom prunes to the
// induced reachable subgraph. ScopePath marks the path OnPath and, depending on
// Mode, either keeps the surrounding induced subgraph (shortest), only the path
// (trace), or the union of all simple paths (all).
func applyScope(vm ViewModel, ir *state.IR[string, string, any], opts ProjectionOptions) (ViewModel, error) {
	switch opts.Scope {
	case ScopeWhole:
		return vm, nil

	case ScopeReachableFrom:
		reachable, err := query.ReachableFrom(ir, opts.From)
		if err != nil {
			return ViewModel{Highlight: []string{}}, fmt.Errorf("scope reachableFromA: %w", err)
		}
		return filterToSet(vm, reachable, nil), nil

	case ScopePath:
		return applyPathScope(vm, ir, opts)

	default:
		return ViewModel{Highlight: []string{}}, fmt.Errorf("unknown scope %q", opts.Scope)
	}
}

// applyPathScope handles ScopePath for each Mode.
func applyPathScope(vm ViewModel, ir *state.IR[string, string, any], opts ProjectionOptions) (ViewModel, error) {
	switch opts.Mode {
	case ModeShortest, ModeTrace:
		path, found, err := query.ShortestPath(ir, opts.From, opts.To)
		if err != nil {
			return ViewModel{Highlight: []string{}}, fmt.Errorf("scope path: %w", err)
		}
		if !found {
			return ViewModel{Highlight: []string{}}, fmt.Errorf("scope path: no path from %q to %q", opts.From, opts.To)
		}
		nodeSet, edgeSet := pathSets(path, opts.From)
		if opts.Mode == ModeTrace {
			// Keep only the path's nodes and edges.
			out := filterToSet(vm, nodeSet, edgeSet)
			markOnPath(&out, nodeSet, edgeSet)
			return out, nil
		}
		// ModeShortest: keep the whole induced reachable subgraph, mark on-path.
		reachable, err := query.ReachableFrom(ir, opts.From)
		if err != nil {
			return ViewModel{Highlight: []string{}}, fmt.Errorf("scope path: %w", err)
		}
		out := filterToSet(vm, reachable, nil)
		markOnPath(&out, nodeSet, edgeSet)
		return out, nil

	case ModeAll:
		pathCap := opts.PathCap
		if pathCap <= 0 {
			pathCap = DefaultPathCap
		}
		paths, _, err := query.AllSimplePaths(ir, opts.From, opts.To, pathCap)
		if err != nil {
			return ViewModel{Highlight: []string{}}, fmt.Errorf("scope path all: %w", err)
		}
		if len(paths) == 0 {
			return ViewModel{Highlight: []string{}}, fmt.Errorf("scope path all: no path from %q to %q", opts.From, opts.To)
		}
		nodeSet := map[string]bool{opts.From: true}
		edgeSet := map[[2]string]bool{}
		for _, p := range paths {
			for _, st := range p {
				nodeSet[st.From] = true
				nodeSet[st.To] = true
				edgeSet[[2]string{st.From, st.To}] = true
			}
		}
		out := filterToSet(vm, nodeSet, edgeSet)
		markOnPath(&out, nodeSet, edgeSet)
		return out, nil

	default:
		return ViewModel{Highlight: []string{}}, fmt.Errorf("unknown mode %q", opts.Mode)
	}
}

// pathSets returns the node set (including the path's start) and the directed
// edge set for a single path.
func pathSets(path query.Path, from string) (map[string]bool, map[[2]string]bool) {
	nodeSet := map[string]bool{from: true}
	edgeSet := map[[2]string]bool{}
	for _, st := range path {
		nodeSet[st.From] = true
		nodeSet[st.To] = true
		edgeSet[[2]string{st.From, st.To}] = true
	}
	return nodeSet, edgeSet
}

// filterToSet keeps only nodes whose ID is in nodeSet. Edges are kept when both
// endpoints survive AND, if edgeSet is non-nil, the (From,To) pair is in it.
// Containers are kept when they retain at least one surviving child; their
// child lists are pruned to surviving members. Highlight is preserved as-is
// (markOnPath repopulates it for path scopes).
func filterToSet(vm ViewModel, nodeSet map[string]bool, edgeSet map[[2]string]bool) ViewModel {
	out := ViewModel{Highlight: vm.Highlight}

	for i := range vm.Nodes {
		if nodeSet[vm.Nodes[i].ID] {
			out.Nodes = append(out.Nodes, vm.Nodes[i])
		}
	}
	for i := range vm.Edges {
		e := vm.Edges[i]
		if !nodeSet[e.From] || !nodeSet[e.To] {
			continue
		}
		if edgeSet != nil && !edgeSet[[2]string{e.From, e.To}] {
			continue
		}
		out.Edges = append(out.Edges, e)
	}
	for i := range vm.Containers {
		c := vm.Containers[i]
		kept := make([]string, 0, len(c.Children))
		for _, child := range c.Children {
			if nodeSet[child] {
				kept = append(kept, child)
			}
		}
		// A container survives if it is itself a kept node or retains children.
		if nodeSet[c.ID] || len(kept) > 0 {
			c.Children = kept
			out.Containers = append(out.Containers, c)
		}
	}
	if out.Highlight == nil {
		out.Highlight = []string{}
	}
	return out
}

// markOnPath sets OnPath=true on nodes/edges in the given sets and repopulates
// Highlight with the on-path node IDs in node order. Off-path nodes/edges keep
// OnPath=false (dimmed).
func markOnPath(vm *ViewModel, nodeSet map[string]bool, edgeSet map[[2]string]bool) {
	highlight := make([]string, 0, len(nodeSet))
	for i := range vm.Nodes {
		if nodeSet[vm.Nodes[i].ID] {
			vm.Nodes[i].OnPath = true
			highlight = append(highlight, vm.Nodes[i].ID)
		}
	}
	for i := range vm.Edges {
		e := &vm.Edges[i]
		if edgeSet[[2]string{e.From, e.To}] {
			e.OnPath = true
		}
	}
	vm.Highlight = highlight
}

// appendDataFlow folds read/write hints onto a description as a compact,
// human-readable suffix, e.g. "desc [reads: Order; writes: Total]".
func appendDataFlow(desc string, reads, writes []string) string {
	var parts []string
	if len(reads) > 0 {
		parts = append(parts, "reads: "+strings.Join(reads, ", "))
	}
	if len(writes) > 0 {
		parts = append(parts, "writes: "+strings.Join(writes, ", "))
	}
	if len(parts) == 0 {
		return desc
	}
	flow := "[" + strings.Join(parts, "; ") + "]"
	if desc == "" {
		return flow
	}
	return desc + " " + flow
}
