package state

import (
	"fmt"
	"sort"
	"strings"
)

// This file renders a machine to the two diagram formats described by the
// JSON / Mermaid / DOT design: Mermaid stateDiagram-v2 and GraphViz DOT. Both
// are pure functions of the machine graph — the same structure the IR carries —
// so a machine loaded from JSON and re-quenched renders identically to one
// forged in code. Output is deterministic: states keep their declared order and
// edges are sorted, so repeated calls are byte-identical and golden-stable.
//
// Rendering is plain string building over the standard library only, preserving
// the kernel's stdlib-only import boundary.

// ToMermaid renders the machine as a GitHub-renderable Mermaid stateDiagram-v2.
//
// Transitions render as labeled edges (Event, with guards as a bracketed
// suffix); the initial state is reached from the [*] start marker and final
// states point back to [*]. Compound states render as nested state blocks and
// parallel states use the -- region divider. Owner tags render as classDef
// color-coding, since stateDiagram-v2 has no native swim lanes.
func (m *Machine[S, E, C]) ToMermaid(opts ...VizOption) string {
	cfg := vizConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	var b strings.Builder
	b.WriteString("stateDiagram-v2\n")
	if cfg.dirSet {
		if cfg.leftRight {
			b.WriteString("    direction LR\n")
		} else {
			b.WriteString("    direction TB\n")
		}
	}
	if m.hasInitial {
		fmt.Fprintf(&b, "    [*] --> %s\n", mermaidID(fmt.Sprint(m.initial)))
	}

	// Edges are grouped by the scope (prefix) that lexically contains both
	// endpoints, so a composite state's internal transitions render inside its
	// block and cross-boundary transitions render at the top level — Mermaid
	// nests by lexical scope.
	byScope := map[string][]edge{}
	for _, e := range collectEdges(m.states) {
		byScope[e.scope] = append(byScope[e.scope], e)
	}

	for i := range m.states {
		writeMermaidState(&b, &m.states[i], &cfg, "", 1, byScope)
	}
	writeMermaidScopeEdges(&b, byScope[""], &cfg, 1)

	if !cfg.hideOwners {
		writeMermaidOwners(&b, m.states)
	}
	return b.String()
}

// writeMermaidScopeEdges emits the sorted edges that belong to one lexical
// scope, at the given indent depth.
func writeMermaidScopeEdges(b *strings.Builder, edges []edge, cfg *vizConfig, depth int) {
	indent := strings.Repeat("    ", depth)
	sortEdges(edges)
	for _, e := range edges {
		fmt.Fprintf(b, "%s%s --> %s%s\n",
			indent, mermaidID(e.from), mermaidID(e.to), mermaidLabel(e, cfg))
	}
}

// writeMermaidState emits a single state. A leaf declares its bare id (so a
// nested leaf is contained by its block) plus a final marker; compound and
// parallel states open a nested block holding their members, internal edges,
// and region dividers. prefix qualifies members to dodge id collisions.
func writeMermaidState[S comparable, E comparable, C any](b *strings.Builder, s *State[S, E, C], cfg *vizConfig, prefix string, depth int, byScope map[string][]edge) {
	indent := strings.Repeat("    ", depth)
	id := mermaidID(qualify(prefix, fmt.Sprint(s.Name)))

	switch {
	case len(s.Regions) > 0:
		fmt.Fprintf(b, "%sstate %s {\n", indent, id)
		for ri := range s.Regions {
			if ri > 0 {
				fmt.Fprintf(b, "%s    --\n", indent)
			}
			r := &s.Regions[ri]
			rPrefix := joinPrefix(prefix, fmt.Sprint(s.Name)+"_"+r.Name)
			if r.InitialChild != nil {
				fmt.Fprintf(b, "%s    [*] --> %s\n", indent, mermaidID(qualify(rPrefix, fmt.Sprint(*r.InitialChild))))
			}
			for i := range r.States {
				writeMermaidState(b, &r.States[i], cfg, rPrefix, depth+1, byScope)
			}
			writeMermaidScopeEdges(b, byScope[rPrefix], cfg, depth+1)
		}
		fmt.Fprintf(b, "%s}\n", indent)
	case len(s.Children) > 0:
		fmt.Fprintf(b, "%sstate %s {\n", indent, id)
		childPrefix := joinPrefix(prefix, fmt.Sprint(s.Name))
		if s.InitialChild != nil {
			fmt.Fprintf(b, "%s    [*] --> %s\n", indent, mermaidID(qualify(childPrefix, fmt.Sprint(*s.InitialChild))))
		}
		for i := range s.Children {
			writeMermaidState(b, &s.Children[i], cfg, childPrefix, depth+1, byScope)
		}
		writeMermaidScopeEdges(b, byScope[childPrefix], cfg, depth+1)
		fmt.Fprintf(b, "%s}\n", indent)
	default:
		// A nested leaf needs a bare id line so it is lexically contained by its
		// block; a top-level leaf is already implied by its edges. A final leaf
		// also draws the terminal marker.
		if depth > 1 {
			fmt.Fprintf(b, "%s%s\n", indent, id)
		}
		if s.IsFinal {
			fmt.Fprintf(b, "%s%s --> [*]\n", indent, id)
		}
	}
}

// writeMermaidOwners emits a classDef per distinct owner plus a class assignment
// per owned state, deterministically ordered.
func writeMermaidOwners[S comparable, E comparable, C any](b *strings.Builder, states []State[S, E, C]) {
	owners := map[string][]string{}
	collectOwners(states, "", owners)
	if len(owners) == 0 {
		return
	}
	names := make([]string, 0, len(owners))
	for o := range owners {
		names = append(names, o)
	}
	sort.Strings(names)
	for _, o := range names {
		fmt.Fprintf(b, "    classDef owner_%s fill:%s\n", classToken(o), ownerColor(o))
	}
	for _, o := range names {
		members := owners[o]
		sort.Strings(members)
		fmt.Fprintf(b, "    class %s owner_%s\n", strings.Join(members, ","), classToken(o))
	}
}

// collectOwners walks the hierarchy and records each owned state's qualified id
// under its owner.
func collectOwners[S comparable, E comparable, C any](states []State[S, E, C], prefix string, out map[string][]string) {
	for i := range states {
		s := &states[i]
		id := mermaidID(qualify(prefix, fmt.Sprint(s.Name)))
		if s.OwnedBy != "" {
			out[s.OwnedBy] = append(out[s.OwnedBy], id)
		}
		if len(s.Children) > 0 {
			collectOwners(s.Children, joinPrefix(prefix, fmt.Sprint(s.Name)), out)
		}
		for ri := range s.Regions {
			r := &s.Regions[ri]
			collectOwners(r.States, joinPrefix(prefix, fmt.Sprint(s.Name)+"_"+r.Name), out)
		}
	}
}

// ToDOT renders the machine as GraphViz DOT for richer SVG output — slides, docs
// sites, and large hierarchical machines where Mermaid grows unreadable.
//
// Compound and parallel states become subgraph clusters, final states draw a
// double border, owners encode as node fillcolor, and the layout defaults to
// rankdir=LR (well suited to lifecycles).
func (m *Machine[S, E, C]) ToDOT(opts ...VizOption) string {
	cfg := vizConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	rankdir := "LR"
	if cfg.dirSet && !cfg.leftRight {
		rankdir = "TB"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "digraph %s {\n", dotQuote(m.name))
	fmt.Fprintf(&b, "    rankdir=%s;\n", rankdir)
	b.WriteString("    node [shape=box, style=rounded];\n")

	if m.hasInitial {
		b.WriteString("    __start [shape=point];\n")
		fmt.Fprintf(&b, "    __start -> %s;\n", dotQuote(fmt.Sprint(m.initial)))
	}

	clusterSeq := 0
	for i := range m.states {
		writeDOTState(&b, &m.states[i], &cfg, 1, &clusterSeq)
	}

	edges := collectEdges(m.states)
	sortEdges(edges)
	for _, e := range edges {
		fmt.Fprintf(&b, "    %s -> %s%s;\n", dotQuote(e.fromRaw), dotQuote(e.toRaw), dotLabel(e, &cfg))
	}

	b.WriteString("}\n")
	return b.String()
}

// writeDOTState emits a node for a leaf or a cluster subgraph for a compound or
// parallel state. clusterSeq supplies stable, unique cluster ids in walk order.
func writeDOTState[S comparable, E comparable, C any](b *strings.Builder, s *State[S, E, C], cfg *vizConfig, depth int, clusterSeq *int) {
	indent := strings.Repeat("    ", depth)
	name := fmt.Sprint(s.Name)

	switch {
	case len(s.Regions) > 0:
		id := *clusterSeq
		*clusterSeq++
		fmt.Fprintf(b, "%ssubgraph cluster_%d {\n", indent, id)
		fmt.Fprintf(b, "%s    label=%s;\n", indent, dotQuote(name))
		for ri := range s.Regions {
			r := &s.Regions[ri]
			rid := *clusterSeq
			*clusterSeq++
			fmt.Fprintf(b, "%s    subgraph cluster_%d {\n", indent, rid)
			fmt.Fprintf(b, "%s        label=%s;\n", indent, dotQuote(r.Name))
			fmt.Fprintf(b, "%s        style=dashed;\n", indent)
			for i := range r.States {
				writeDOTState(b, &r.States[i], cfg, depth+2, clusterSeq)
			}
			fmt.Fprintf(b, "%s    }\n", indent)
		}
		fmt.Fprintf(b, "%s}\n", indent)
	case len(s.Children) > 0:
		id := *clusterSeq
		*clusterSeq++
		fmt.Fprintf(b, "%ssubgraph cluster_%d {\n", indent, id)
		fmt.Fprintf(b, "%s    label=%s;\n", indent, dotQuote(name))
		if attrs := dotNodeAttrs(s, cfg); attrs != "" {
			fmt.Fprintf(b, "%s    %s %s;\n", indent, dotQuote(name), attrs)
		}
		for i := range s.Children {
			writeDOTState(b, &s.Children[i], cfg, depth+1, clusterSeq)
		}
		fmt.Fprintf(b, "%s}\n", indent)
	default:
		if attrs := dotNodeAttrs(s, cfg); attrs != "" {
			fmt.Fprintf(b, "%s%s %s;\n", indent, dotQuote(name), attrs)
		} else {
			fmt.Fprintf(b, "%s%s;\n", indent, dotQuote(name))
		}
	}
}

// dotNodeAttrs builds the bracketed attribute list for a leaf node: a double
// border for final states and a fill color for owned ones. Returns "" when no
// attributes apply.
func dotNodeAttrs[S comparable, E comparable, C any](s *State[S, E, C], cfg *vizConfig) string {
	var attrs []string
	if s.IsFinal {
		attrs = append(attrs, "peripheries=2")
	}
	if !cfg.hideOwners && s.OwnedBy != "" {
		attrs = append(attrs, "style=\"rounded,filled\"", "fillcolor="+dotQuote(ownerColor(s.OwnedBy)))
	}
	if len(attrs) == 0 {
		return ""
	}
	return "[" + strings.Join(attrs, ", ") + "]"
}

// edge is a flattened transition with rendered endpoints, label parts, and the
// lexical scope (prefix) that contains it. scope is the declaring prefix when
// the target is a sibling in that same scope, else "" so a cross-boundary
// transition renders at the top level.
type edge struct {
	from    string // qualified id, for Mermaid nesting
	to      string
	fromRaw string // bare state name, for DOT's flat node ids
	toRaw   string
	on      string
	guards  []string
	scope   string
}

// collectEdges flattens every transition across the hierarchy into render-ready
// edges. Each endpoint resolves to the qualified id of the state where it is
// declared (via a name index), so a cross-boundary transition targets the right
// block rather than a phantom qualified-in-place id. An edge whose endpoints sit
// in different scopes renders at the top level.
func collectEdges[S comparable, E comparable, C any](states []State[S, E, C]) []edge {
	index := map[string]string{} // raw name -> qualified id
	indexNames(states, "", index)

	resolve := func(name string) string {
		if id, ok := index[name]; ok {
			return id
		}
		return mermaidID(name)
	}

	var out []edge
	var visit func(states []State[S, E, C], prefix string)
	visit = func(states []State[S, E, C], prefix string) {
		siblings := map[string]bool{}
		for i := range states {
			siblings[fmt.Sprint(states[i].Name)] = true
		}
		for i := range states {
			s := &states[i]
			for ti := range s.Transitions {
				t := &s.Transitions[ti]
				fromName, toName := fmt.Sprint(t.From), fmt.Sprint(t.To)
				scope := prefix
				if !siblings[fromName] || !siblings[toName] {
					scope = ""
				}
				// A forbidden transition is a block, not a graph edge: it has no
				// target and produces no state change, so it is not rendered. A
				// targetless wildcard (an internal action-only catch-all) likewise has
				// no edge to draw.
				if t.Forbidden {
					continue
				}
				if t.Wildcard && isZero(t.To) {
					continue
				}
				e := edge{
					from:    resolve(fromName),
					to:      resolve(toName),
					fromRaw: fromName,
					toRaw:   toName,
					scope:   scope,
				}
				switch {
				case t.Wildcard:
					// Render the catch-all as the conventional "*" event label.
					e.on = "*"
				case !t.EventLess:
					e.on = fmt.Sprint(t.On)
				}
				// A delayed (`after`) transition is annotated with its delay so the
				// diagram distinguishes a timed edge from an event-driven one.
				if t.After != nil {
					label := "after(" + t.After.String() + ")"
					if e.on != "" {
						label += " " + e.on
					}
					e.on = label
				}
				for _, g := range t.Guards {
					e.guards = append(e.guards, g.Name)
				}
				if t.GuardExpr != nil {
					e.guards = append(e.guards, renderGuardExpr(t.GuardExpr))
				}
				out = append(out, e)
			}
			if len(s.Children) > 0 {
				visit(s.Children, joinPrefix(prefix, fmt.Sprint(s.Name)))
			}
			for ri := range s.Regions {
				r := &s.Regions[ri]
				visit(r.States, joinPrefix(prefix, fmt.Sprint(s.Name)+"_"+r.Name))
			}
		}
	}
	visit(states, "")
	return out
}

// indexNames records the qualified Mermaid id of every state keyed by its bare
// name, mirroring the prefixing used when the state is declared in a block.
func indexNames[S comparable, E comparable, C any](states []State[S, E, C], prefix string, out map[string]string) {
	for i := range states {
		s := &states[i]
		name := fmt.Sprint(s.Name)
		out[name] = mermaidID(qualify(prefix, name))
		if len(s.Children) > 0 {
			indexNames(s.Children, joinPrefix(prefix, name), out)
		}
		for ri := range s.Regions {
			r := &s.Regions[ri]
			indexNames(r.States, joinPrefix(prefix, name+"_"+r.Name), out)
		}
	}
}

// sortEdges orders edges by (from, on, to) for deterministic, golden-stable
// output.
func sortEdges(edges []edge) {
	sort.Slice(edges, func(i, j int) bool {
		a, b := edges[i], edges[j]
		if a.from != b.from {
			return a.from < b.from
		}
		if a.on != b.on {
			return a.on < b.on
		}
		return a.to < b.to
	})
}

// mermaidLabel renders the ": Event [Guards]" suffix for a Mermaid edge, or ""
// when neither an event nor guards apply.
func mermaidLabel(e edge, cfg *vizConfig) string {
	label := edgeLabel(e, cfg)
	if label == "" {
		return ""
	}
	return ": " + label
}

// dotLabel renders the [label="..."] attribute for a DOT edge, or "" when the
// edge carries no event or guards.
func dotLabel(e edge, cfg *vizConfig) string {
	label := edgeLabel(e, cfg)
	if label == "" {
		return ""
	}
	return " [label=" + dotQuote(label) + "]"
}

// edgeLabel composes the shared "Event [Guard1, Guard2]" text used by both
// renderers, honoring WithoutGuards.
func edgeLabel(e edge, cfg *vizConfig) string {
	var sb strings.Builder
	sb.WriteString(e.on)
	if !cfg.hideGuards && len(e.guards) > 0 {
		if e.on != "" {
			sb.WriteByte(' ')
		}
		sb.WriteByte('[')
		sb.WriteString(strings.Join(e.guards, ", "))
		sb.WriteByte(']')
	}
	return sb.String()
}

// qualify joins a region/child prefix to a state name. The empty prefix yields
// the bare name so top-level states keep their plain identifiers.
func qualify(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "__" + name
}

// joinPrefix extends a prefix with another segment, used while descending into
// nested compound states and regions.
func joinPrefix(prefix, segment string) string {
	if prefix == "" {
		return segment
	}
	return prefix + "__" + segment
}

// mermaidID sanitizes an identifier for Mermaid, which accepts a limited
// character set for state ids. Disallowed characters collapse to underscores.
func mermaidID(s string) string {
	var sb strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			sb.WriteRune(r)
		default:
			sb.WriteByte('_')
		}
	}
	if sb.Len() == 0 {
		return "s"
	}
	return sb.String()
}

// classToken sanitizes an owner name for use in a Mermaid classDef name.
func classToken(s string) string { return mermaidID(s) }

// dotQuote wraps a value in double quotes, escaping embedded quotes and
// backslashes for DOT.
func dotQuote(s string) string {
	r := strings.NewReplacer("\\", "\\\\", "\"", "\\\"")
	return "\"" + r.Replace(s) + "\""
}

// ownerColor maps an owner name to a stable color from a fixed palette, so the
// same owner always renders the same shade across machines and runs.
func ownerColor(owner string) string {
	palette := []string{
		"#cfe8ff", // blue
		"#d6f5d6", // green
		"#ffe0cc", // orange
		"#f0d9ff", // purple
		"#fff2cc", // yellow
		"#ffd6e0", // pink
		"#d9f2f2", // teal
		"#e8e8e8", // grey
	}
	var sum uint32
	for i := 0; i < len(owner); i++ {
		sum = sum*31 + uint32(owner[i])
	}
	return palette[sum%uint32(len(palette))]
}
