package render

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/stablekernel/crucible/cmd/crucible/internal/viewmodel"
)

// EmitD2 generates deterministic D2 source from a viewmodel under the given
// theme. The output is stable: it ranges slices in document order and never
// iterates a map for emitted content, so identical input yields identical D2.
//
// Structure mirrors the forge v5 reference: a vars/d2-config header pinning the
// ELK engine, a classes block parameterised by the theme, then nodes and edges
// emitted with container nesting. Node kinds map to classes (state/invoke/
// history/final/init), on-path nodes get a hot ember rim, off-path atomic nodes
// dim to dim_node, and edges pick hot_edge (on-path) or a cold class by kind.
func EmitD2(vm viewmodel.ViewModel, theme Theme) (string, error) {
	idx := buildIndex(vm)
	var b strings.Builder

	writeHeader(&b, theme)
	writeClasses(&b, theme)
	fmt.Fprintf(&b, "\nstyle.fill: %s\n\n", quote(theme.Bg))

	// Emit every top-level entity (a node or a container with no surviving
	// parent), recursing into container children. We walk containers first then
	// nodes, both in document order, tracking emitted IDs so a container that is
	// also backed by a node (parallel/composite) is emitted exactly once. This
	// also covers the case where path/scope filtering pruned a container's node
	// but kept the container record because its children survived.
	emitted := make(map[string]bool)
	for i := range vm.Containers {
		c := vm.Containers[i]
		if idx.parentOf[c.ID] != "" || emitted[c.ID] {
			continue
		}
		emitEntity(&b, vm, idx, theme, c.ID, 0, emitted)
	}
	for i := range vm.Nodes {
		n := vm.Nodes[i]
		if idx.parentOf[n.ID] != "" || emitted[n.ID] {
			continue
		}
		emitEntity(&b, vm, idx, theme, n.ID, 0, emitted)
	}

	// Edges last, at top level, using fully-qualified dotted D2 keys so an edge
	// between nested nodes resolves correctly.
	for i := range vm.Edges {
		emitEdge(&b, idx, vm.Edges[i])
	}

	return b.String(), nil
}

// emitEntity emits the node and/or container identified by id at the given
// depth, recursing into container children. It marks id (and emitted children)
// in emitted to prevent duplicates.
func emitEntity(b *strings.Builder, vm viewmodel.ViewModel, idx *d2Index, theme Theme, id string, depth int, emitted map[string]bool) {
	if emitted[id] {
		return
	}
	emitted[id] = true
	if idx.isContainer[id] {
		c := idx.containerByID[id]
		n, hasNode := idx.nodeByID[id]
		if c.Kind == "region" {
			emitRegion(b, vm, idx, theme, c, depth, emitted)
			return
		}
		emitContainerNode(b, vm, idx, theme, n, c, depth, hasNode, emitted)
		return
	}
	if n, ok := idx.nodeByID[id]; ok {
		emitNode(b, vm, idx, theme, n, depth)
	}
}

// d2Index holds the derived relationships and stable key mapping for a vm.
type d2Index struct {
	// keyOf maps a ViewNode/ViewContainer ID to its sanitized local D2 key.
	keyOf map[string]string
	// parentOf maps a node/container ID to its parent container ID ("" if top).
	parentOf map[string]string
	// containerByID indexes containers for kind/children lookups.
	containerByID map[string]viewmodel.ViewContainer
	// nodeByID indexes nodes.
	nodeByID map[string]viewmodel.ViewNode
	// childrenOf lists the ordered child IDs a container owns (nodes or regions).
	childrenOf map[string][]string
	// isContainer marks IDs that are containers (parallel/composite/region).
	isContainer map[string]bool
}

// buildIndex derives parent relationships and stable D2 keys from the viewmodel.
//
// Parent rule: container X is the parent of Y when Y's ID appears in X.Children.
// Containers may themselves be children (a region is a child of a parallel
// container, an inner composite is a child of an outer composite), so the same
// relation builds the full nesting tree. Keys are sanitized IDs disambiguated to
// stay unique and stable.
func buildIndex(vm viewmodel.ViewModel) *d2Index {
	idx := &d2Index{
		keyOf:         make(map[string]string),
		parentOf:      make(map[string]string),
		containerByID: make(map[string]viewmodel.ViewContainer),
		nodeByID:      make(map[string]viewmodel.ViewNode),
		childrenOf:    make(map[string][]string),
		isContainer:   make(map[string]bool),
	}
	for i := range vm.Nodes {
		idx.nodeByID[vm.Nodes[i].ID] = vm.Nodes[i]
	}
	for i := range vm.Containers {
		c := vm.Containers[i]
		idx.containerByID[c.ID] = c
		idx.isContainer[c.ID] = true
		idx.childrenOf[c.ID] = append(idx.childrenOf[c.ID], c.Children...)
		for _, child := range c.Children {
			idx.parentOf[child] = c.ID
		}
	}
	// Assign deterministic, unique keys. Sanitize, then disambiguate collisions
	// by appending an index in stable iteration order (nodes then containers, in
	// document order) so the mapping never depends on map iteration.
	used := make(map[string]bool)
	assign := func(id string) {
		if _, ok := idx.keyOf[id]; ok {
			return
		}
		base := sanitizeKey(id)
		k := base
		for n := 1; used[k]; n++ {
			k = fmt.Sprintf("%s_%d", base, n)
		}
		used[k] = true
		idx.keyOf[id] = k
	}
	for i := range vm.Nodes {
		assign(vm.Nodes[i].ID)
	}
	for i := range vm.Containers {
		assign(vm.Containers[i].ID)
	}
	return idx
}

// dottedPath returns the fully-qualified D2 path (parent.child...) for an ID.
func (idx *d2Index) dottedPath(id string) string {
	var parts []string
	cur := id
	for cur != "" {
		key, ok := idx.keyOf[cur]
		if !ok {
			key = sanitizeKey(cur)
		}
		parts = append([]string{key}, parts...)
		cur = idx.parentOf[cur]
	}
	return strings.Join(parts, ".")
}

// emitNode writes one leaf node (atomic/invoke/history/final/initial or a
// lifecycle compartment). Containers are handled by emitEntity. depth controls
// indentation for readable nested D2.
func emitNode(b *strings.Builder, _ viewmodel.ViewModel, idx *d2Index, theme Theme, n viewmodel.ViewNode, depth int) {
	ind := indent(depth)
	key := idx.keyOf[n.ID]

	// Lifecycle compartment: a node carrying Entry/Exit/Invoke detail renders as
	// a D2 `shape: class` table with field rows.
	if hasLifecycle(n) {
		emitLifecycle(b, theme, n, key, depth)
		return
	}

	switch n.Kind {
	case viewmodel.NodeInitial:
		fmt.Fprintf(b, "%s%s: %s {\n", ind, key, quote(n.Name))
		fmt.Fprintf(b, "%s  class: init\n", ind)
		fmt.Fprintf(b, "%s  label: \"\"\n", ind)
		fmt.Fprintf(b, "%s}\n", ind)
	case viewmodel.NodeFinal:
		fmt.Fprintf(b, "%s%s: %s {\n", ind, key, quote(n.Name))
		fmt.Fprintf(b, "%s  class: final\n", ind)
		fmt.Fprintf(b, "%s  label: \"\"\n", ind)
		fmt.Fprintf(b, "%s}\n", ind)
	case viewmodel.NodeHistory:
		fmt.Fprintf(b, "%s%s: %s {\n", ind, key, quote(historyLabel(n)))
		fmt.Fprintf(b, "%s  class: history\n", ind)
		fmt.Fprintf(b, "%s}\n", ind)
	case viewmodel.NodeInvoke:
		fmt.Fprintf(b, "%s%s: %s {\n", ind, key, quote(n.Name))
		fmt.Fprintf(b, "%s  class: invoke\n", ind)
		fmt.Fprintf(b, "%s}\n", ind)
	default: // NodeAtomic
		emitAtomic(b, theme, n, key, depth)
	}
}

// emitAtomic renders an atomic state. On-path nodes keep `class: state` but gain
// a hot ember rim (stroke + stroke-width). Off-path atomics dim to dim_node.
func emitAtomic(b *strings.Builder, theme Theme, n viewmodel.ViewNode, key string, depth int) {
	ind := indent(depth)
	fmt.Fprintf(b, "%s%s: %s {\n", ind, key, quote(n.Name))
	if n.OnPath {
		fmt.Fprintf(b, "%s  class: state\n", ind)
		fmt.Fprintf(b, "%s  style.stroke: %s\n", ind, quote(theme.Hot))
		fmt.Fprintf(b, "%s  style.stroke-width: 3\n", ind)
	} else {
		fmt.Fprintf(b, "%s  class: dim_node\n", ind)
	}
	fmt.Fprintf(b, "%s}\n", ind)
}

// emitContainerNode renders a composite or parallel container and recurses into
// its children (which may themselves be containers). Every such container gets
// an EXPLICIT forge style so the DarkMauve base theme's mauve fill / mauve
// border / lavender title never bleed through: a dark steel/charcoal panel fill
// (SteelDark) and warm-white title (TextWarm) always, with an ember border that
// goes HOT ember at a heavier weight when the container is on the highlighted
// path and plain ember otherwise. hasNode reports whether a backing ViewNode
// exists; on-path status comes from that node. Children are emitted via
// emitEntity so nested containers whose node was pruned still render.
func emitContainerNode(
	b *strings.Builder,
	vm viewmodel.ViewModel,
	idx *d2Index,
	theme Theme,
	n viewmodel.ViewNode,
	c viewmodel.ViewContainer,
	depth int,
	hasNode bool,
	emitted map[string]bool,
) {
	ind := indent(depth)
	key := idx.keyOf[c.ID]
	fmt.Fprintf(b, "%s%s: %s {\n", ind, key, quote(c.Name))
	// Forge container-panel styling. The base (off-path) stroke is ember; on-path
	// containers switch to hot ember at a heavier 3px weight to read as part of
	// the highlighted path. Fill and title are constant so no DarkMauve leaks.
	onPath := hasNode && n.OnPath
	stroke := theme.Ember
	strokeWidth := 2
	if onPath {
		stroke = theme.Hot
		strokeWidth = 3
	}
	fmt.Fprintf(b, "%s  style.fill: %s\n", ind, quote(theme.SteelDark))
	fmt.Fprintf(b, "%s  style.stroke: %s\n", ind, quote(stroke))
	fmt.Fprintf(b, "%s  style.font-color: %s\n", ind, quote(theme.TextWarm))
	fmt.Fprintf(b, "%s  style.stroke-width: %d\n", ind, strokeWidth)
	for _, childID := range c.Children {
		emitEntity(b, vm, idx, theme, childID, depth+1, emitted)
	}
	fmt.Fprintf(b, "%s}\n", ind)
}

// emitRegion renders a parallel region container (`class: region`) and its
// child states.
func emitRegion(b *strings.Builder, vm viewmodel.ViewModel, idx *d2Index, theme Theme, c viewmodel.ViewContainer, depth int, emitted map[string]bool) {
	ind := indent(depth)
	key := idx.keyOf[c.ID]
	fmt.Fprintf(b, "%s%s: %s {\n", ind, key, quote(c.Name))
	fmt.Fprintf(b, "%s  class: region\n", ind)
	for _, childID := range c.Children {
		emitEntity(b, vm, idx, theme, childID, depth+1, emitted)
	}
	fmt.Fprintf(b, "%s}\n", ind)
}

// emitLifecycle renders a lifecycle node as a D2 class table with entry/exit/
// invoke rows. The styling matches the forge v5 Session plate: steel header,
// steelDark body, soft-orange keys (post-processed), font-size 13.
func emitLifecycle(b *strings.Builder, theme Theme, n viewmodel.ViewNode, key string, depth int) {
	ind := indent(depth)
	fmt.Fprintf(b, "%s%s: %s {\n", ind, key, quote(n.Name))
	fmt.Fprintf(b, "%s  shape: class\n", ind)
	fmt.Fprintf(b, "%s  style.fill: %s\n", ind, quote(theme.Steel))
	fmt.Fprintf(b, "%s  style.stroke: %s\n", ind, quote(theme.SteelDark))
	fmt.Fprintf(b, "%s  style.font-color: %s\n", ind, quote(theme.SoftOrange))
	fmt.Fprintf(b, "%s  style.font-size: 13\n", ind)
	emitLifecycleRows(b, ind, "entry", n.Entry)
	emitLifecycleRows(b, ind, "exit", n.Exit)
	emitLifecycleRows(b, ind, "invoke", n.Invoke)
	fmt.Fprintf(b, "%s}\n", ind)
}

// emitLifecycleRows writes one class field row per detail item under a label.
// A single item collapses to `label: value`; multiple items number the rows.
func emitLifecycleRows(b *strings.Builder, ind, label string, items []viewmodel.DetailItem) {
	switch len(items) {
	case 0:
		return
	case 1:
		fmt.Fprintf(b, "%s  %s: %s\n", ind, label, quote(items[0].Name))
	default:
		for i := range items {
			fmt.Fprintf(b, "%s  %s%d: %s\n", ind, label, i+1, quote(items[i].Name))
		}
	}
}

// emitEdge writes one transition edge with a class chosen by on-path status and
// edge kind. The label folds event, guards, effects, assigns and after.
func emitEdge(b *strings.Builder, idx *d2Index, e viewmodel.ViewEdge) {
	from := idx.dottedPath(e.From)
	to := idx.dottedPath(e.To)
	label := edgeLabel(e)
	class := edgeClass(e)
	if label == "" {
		fmt.Fprintf(b, "%s -> %s: \"\" { class: %s }\n", from, to, class)
		return
	}
	fmt.Fprintf(b, "%s -> %s: %s { class: %s }\n", from, to, quote(label), class)
}

// edgeClass picks the D2 edge class. On-path edges are hot_edge. Off-path edges
// map by kind: eventless -> eventless, delayed -> delayed, and every other kind
// (event/internal/forbidden/wildcard) falls through to dim_edge. We collapse the
// remaining kinds to dim_edge deliberately: the forge look only distinguishes
// the cold dashed families (eventless/delayed) from the solid cold default, and
// the viewmodel's richer kind taxonomy has no separate forge styling.
func edgeClass(e viewmodel.ViewEdge) string {
	if e.OnPath {
		return "hot_edge"
	}
	switch e.Kind {
	case viewmodel.EdgeEventless:
		return "eventless"
	case viewmodel.EdgeDelayed:
		return "delayed"
	default:
		return "dim_edge"
	}
}

// edgeLabel builds a compact, deterministic edge label from the edge fields.
// Order: event, then "after", then guards in [..], then effects/assigns as a
// trailing list. Empty sections are skipped.
func edgeLabel(e viewmodel.ViewEdge) string {
	var parts []string
	if e.Event != "" {
		parts = append(parts, e.Event)
	}
	if e.After != "" {
		parts = append(parts, "after "+e.After)
	}
	if g := joinItems(e.Guards); g != "" {
		parts = append(parts, "["+g+"]")
	}
	var eff []string
	eff = append(eff, itemNames(e.Effects)...)
	eff = append(eff, itemNames(e.Assigns)...)
	if len(eff) > 0 {
		parts = append(parts, "/ "+strings.Join(eff, ", "))
	}
	return strings.Join(parts, " ")
}

func itemNames(items []viewmodel.DetailItem) []string {
	out := make([]string, 0, len(items))
	for i := range items {
		out = append(out, items[i].Name)
	}
	return out
}

func joinItems(items []viewmodel.DetailItem) string {
	return strings.Join(itemNames(items), ", ")
}

// hasLifecycle reports whether a node carries any lifecycle/invoke detail that
// should render as a compartment table.
func hasLifecycle(n viewmodel.ViewNode) bool {
	return len(n.Entry) > 0 || len(n.Exit) > 0 || len(n.Invoke) > 0
}

// historyLabel derives the history marker: deep history -> "H*", shallow -> "H".
func historyLabel(n viewmodel.ViewNode) string {
	if strings.Contains(n.Name, "*") || strings.Contains(strings.ToLower(n.Name), "deep") {
		return "H*"
	}
	return "H"
}

// writeHeader emits the vars/d2-config block pinning the ELK engine.
func writeHeader(b *strings.Builder, _ Theme) {
	b.WriteString("vars: {\n")
	b.WriteString("  d2-config: {\n")
	b.WriteString("    layout-engine: elk\n")
	b.WriteString("  }\n")
	b.WriteString("}\n\n")
}

// writeClasses emits the theme-parameterised classes block.
func writeClasses(b *strings.Builder, t Theme) {
	fmt.Fprintf(b, "classes: {\n")
	// atomic state: extruded steel slab.
	fmt.Fprintf(b, "  state: {\n    shape: rectangle\n    style: {\n      fill: %s\n      stroke: %s\n      font-color: %s\n      stroke-width: 2\n      3d: true\n    }\n  }\n",
		quote(t.Steel), quote(t.Copper), quote(t.TextWarm))
	// invoke: hexagon.
	fmt.Fprintf(b, "  invoke: {\n    shape: hexagon\n    style: {\n      fill: %s\n      stroke: %s\n      font-color: %s\n      stroke-width: 2\n    }\n  }\n",
		quote(t.InvokeFill), quote(t.Copper), quote(t.InvokeText))
	// init: small ember circle.
	fmt.Fprintf(b, "  init: {\n    shape: circle\n    width: 26\n    style: {\n      fill: %s\n      stroke: %s\n      font-color: %s\n    }\n  }\n",
		quote(t.Ember), quote(t.Hot), quote(t.Bg))
	// history: copper circle.
	fmt.Fprintf(b, "  history: {\n    shape: circle\n    width: 34\n    style: {\n      fill: %s\n      stroke: %s\n      font-color: %s\n    }\n  }\n",
		quote(t.Copper), quote(t.Copper), quote(t.HistoryText))
	// final: double-ring ember.
	fmt.Fprintf(b, "  final: {\n    shape: circle\n    width: 34\n    style: {\n      fill: %s\n      stroke: %s\n      stroke-width: 3\n      font-color: %s\n      multiple: true\n    }\n  }\n",
		quote(t.Bg), quote(t.Ember), quote(t.InvokeText))
	// region: dashed copper border.
	fmt.Fprintf(b, "  region: {\n    shape: rectangle\n    style: {\n      fill: %s\n      stroke: %s\n      font-color: %s\n      stroke-dash: 3\n      border-radius: 8\n    }\n  }\n",
		quote(t.SteelDark), quote(t.Copper), quote(t.SoftOrange))
	// hot_edge: crisp ember, heavier.
	fmt.Fprintf(b, "  hot_edge: {\n    style: {\n      stroke: %s\n      stroke-width: 3\n      font-color: %s\n      bold: true\n    }\n  }\n",
		quote(t.Hot), quote(t.HotBright))
	// guard_edge.
	fmt.Fprintf(b, "  guard_edge: {\n    style: {\n      stroke: %s\n      font-color: %s\n    }\n  }\n",
		quote(t.ScaleGrey), quote(t.ScaleText))
	// eventless: cold dashed.
	fmt.Fprintf(b, "  eventless: {\n    style: {\n      stroke: %s\n      stroke-dash: 4\n      font-color: %s\n    }\n  }\n",
		quote(t.ScaleGrey), quote(t.ScaleText))
	// delayed: copper dashed.
	fmt.Fprintf(b, "  delayed: {\n    style: {\n      stroke: %s\n      stroke-dash: 4\n      font-color: %s\n    }\n  }\n",
		quote(t.Copper), quote(t.SoftOrange))
	// dim_node: cold extruded.
	fmt.Fprintf(b, "  dim_node: {\n    shape: rectangle\n    style: {\n      fill: %s\n      stroke: %s\n      font-color: %s\n      stroke-width: 1\n      opacity: 0.85\n      3d: true\n    }\n  }\n",
		quote(t.DimNodeFill), quote(t.ScaleGrey), quote(t.ScaleText))
	// dim_edge: cold faint.
	fmt.Fprintf(b, "  dim_edge: {\n    style: {\n      stroke: %s\n      font-color: %s\n      opacity: 0.7\n    }\n  }\n",
		quote(t.ScaleGrey), quote(t.ScaleText))
	fmt.Fprintf(b, "}\n")
}

// sanitizeKeyRe matches characters not allowed in a bare D2 key.
var sanitizeKeyRe = regexp.MustCompile(`[^A-Za-z0-9_]`)

// sanitizeKey turns an arbitrary ID into a valid, stable D2 identifier.
func sanitizeKey(id string) string {
	k := sanitizeKeyRe.ReplaceAllString(id, "_")
	if k == "" {
		k = "n"
	}
	// A key must not start with a digit for a clean bare identifier.
	if k[0] >= '0' && k[0] <= '9' {
		k = "n_" + k
	}
	return k
}

// quote wraps a label/value in double quotes when it contains D2-special
// characters (spaces, the middle dot, > - : or a quote) or begins with '#' (the
// D2 comment marker — color values like "#ff7a18" MUST be quoted or D2 treats
// the rest of the line as a comment). Embedded quotes are escaped. Plain
// identifiers are returned bare.
func quote(s string) string {
	if s == "" {
		return `""`
	}
	if strings.HasPrefix(s, "#") || strings.ContainsAny(s, " ·>-:\"\n") {
		return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
	}
	return s
}

// indent returns 2*depth spaces.
func indent(depth int) string {
	return strings.Repeat("  ", depth)
}
