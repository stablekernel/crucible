package analysis

import "fmt"

// checkUndefinedTargets reports every transition whose target state is not a
// declared state of the machine. Such an edge can never complete — the kernel
// cannot enter a state that does not exist — and would otherwise be silently
// skipped by the reachability and path walks (which defensively ignore unknown
// targets), hiding the defect. Forbidden transitions are already excluded from
// the edge set, so their meaningless To never reaches here.
func checkUndefinedTargets(g *graph, r *Report) {
	var found []Finding
	for _, e := range g.edges {
		if _, ok := g.nodes[e.to]; ok {
			continue
		}
		found = append(found, Finding{
			Kind:       KindUndefinedTarget,
			Severity:   SeverityError,
			State:      e.from,
			Transition: e.label(),
			Message:    fmt.Sprintf("transition from %q targets undeclared state %q; it can never complete", e.from, e.to),
		})
	}
	sortFindings(found)
	r.Findings = append(r.Findings, found...)
}

// checkUnreachable reports every declared state with no inbound path from the
// initial state. The initial state itself is always reachable. A machine with
// no declared initial state cannot be analyzed for reachability, so the check
// reports nothing rather than flagging every state.
func checkUnreachable(g *graph, r *Report) {
	if !g.hasInitial {
		return
	}
	reach := g.reachable()
	var found []Finding
	for _, name := range g.order {
		if reach[name] {
			continue
		}
		found = append(found, Finding{
			Kind:     KindUnreachableState,
			Severity: SeverityError,
			State:    name,
			Message:  fmt.Sprintf("state %q is declared but has no path from the initial state %q; it can never be entered", name, g.initial),
		})
	}
	sortFindings(found)
	r.Findings = append(r.Findings, found...)
}

// checkDeadTransitions reports every transition whose source state is itself
// unreachable: the edge can never fire because its source can never be entered.
// Transitions out of reachable states are live (whether their guard ever passes
// is a runtime question this static pass does not decide).
func checkDeadTransitions(g *graph, r *Report) {
	if !g.hasInitial {
		return
	}
	reach := g.reachable()
	var found []Finding
	for _, e := range g.edges {
		if reach[e.from] {
			continue
		}
		found = append(found, Finding{
			Kind:       KindDeadTransition,
			Severity:   SeverityError,
			State:      e.from,
			Transition: e.label(),
			Message:    fmt.Sprintf("transition can never fire: its source state %q is unreachable", e.from),
		})
	}
	sortFindings(found)
	r.Findings = append(r.Findings, found...)
}

// checkNondeterminism reports states with statically ambiguous transition
// selection: two or more guardless transitions on the same event, or two or
// more guardless eventless ("always") transitions. Only guardless overlaps are
// reported — a tie the kernel cannot break and a human cannot reason away.
// Guarded overlaps (every colliding transition carries a guard) are intentionally
// not reported: whether the guards are mutually exclusive is a runtime property
// the IR cannot prove, so flagging them would be noise.
func checkNondeterminism(g *graph, r *Report) {
	var found []Finding
	for _, name := range g.order {
		// Count guardless transitions per event, guardless "always" edges, and
		// guardless wildcard ("*") edges. A wildcard's On field is meaningless, so it
		// is tallied as a catch-all rather than as a specific (zero-value) event.
		guardlessByEvent := map[string]int{}
		guardlessAlways := 0
		guardlessWildcard := 0
		for _, e := range g.outgoing[name] {
			if e.guarded {
				continue
			}
			switch {
			case e.wildcard:
				guardlessWildcard++
			case e.eventLess:
				guardlessAlways++
			default:
				guardlessByEvent[e.on]++
			}
		}
		for ev, count := range guardlessByEvent {
			if count >= 2 {
				found = append(found, Finding{
					Kind:     KindNondeterministic,
					Severity: SeverityError,
					State:    name,
					Message:  fmt.Sprintf("state %q has %d guardless transitions on event %q; selection is ambiguous (which wins?)", name, count, ev),
				})
			}
		}
		if guardlessAlways >= 2 {
			found = append(found, Finding{
				Kind:     KindNondeterministic,
				Severity: SeverityError,
				State:    name,
				Message:  fmt.Sprintf("state %q has %d guardless eventless (always) transitions; selection is ambiguous", name, guardlessAlways),
			})
		}
		if guardlessWildcard >= 2 {
			found = append(found, Finding{
				Kind:     KindNondeterministic,
				Severity: SeverityError,
				State:    name,
				Message:  fmt.Sprintf("state %q has %d guardless wildcard (catch-all) transitions; selection is ambiguous", name, guardlessWildcard),
			})
		}
	}
	sortFindings(found)
	r.Findings = append(r.Findings, found...)
}

// checkDeadEnds reports non-final states with no outgoing transitions: a state
// you can enter but never leave, which is not declared terminal. A composite
// state (compound or parallel) is exempt — it is left via its children's or
// regions' transitions, and a parent with no edges of its own is normal. Final
// states are exempt by definition. This is a heuristic warning: a leaf may be
// terminal by convention without the IsFinal flag, or be exited only by an
// ancestor transition in a deep hierarchy.
func checkDeadEnds(g *graph, r *Report) {
	var found []Finding
	for _, name := range g.order {
		n := g.nodes[name]
		if n.final || n.compound || n.parallel {
			continue
		}
		hasOut := false
		for _, e := range g.outgoing[name] {
			if e.internal {
				continue // a self-transition does not leave the state
			}
			hasOut = true
			break
		}
		// A nested leaf whose parent or an ancestor carries an outgoing transition
		// can still be exited via that ancestor, so it is not a true dead end.
		if !hasOut && ancestorHasExit(g, name) {
			hasOut = true
		}
		if hasOut {
			continue
		}
		found = append(found, Finding{
			Kind:     KindDeadEnd,
			Severity: SeverityWarning,
			State:    name,
			Message:  fmt.Sprintf("non-final state %q has no outgoing transitions; once entered it can never be left (mark it final if this is intended)", name),
		})
	}
	sortFindings(found)
	r.Findings = append(r.Findings, found...)
}

// ancestorHasExit reports whether any ancestor of name carries a non-internal
// outgoing transition, which would let an instance leave name by bubbling the
// event up the hierarchy.
func ancestorHasExit(g *graph, name string) bool {
	cur := g.nodes[name].parent
	for cur != "" {
		for _, e := range g.outgoing[cur] {
			if !e.internal {
				return true
			}
		}
		n, ok := g.nodes[cur]
		if !ok {
			break
		}
		cur = n.parent
	}
	return false
}

// checkLiveness reports every reachable state from which no final state is
// reachable, in a machine that declares at least one final state. Such a state
// can never complete. Machines with no final states are skipped: "completion"
// is undefined for them, so the absence of a reachable final state is not a
// defect. This is a heuristic warning: the static graph treats every edge as
// traversable, so a state that depends on an always-false guard to reach a final
// state will look live here when it is in fact stuck.
func checkLiveness(g *graph, r *Report) {
	if !g.hasFinal {
		return
	}
	can := g.canReachFinal()
	// Only consider states that are actually reachable; an unreachable state's
	// inability to complete is already covered by KindUnreachableState.
	reach := g.reachable()
	var found []Finding
	for _, name := range g.order {
		if g.hasInitial && !reach[name] {
			continue
		}
		n := g.nodes[name]
		if n.final || can[name] {
			continue
		}
		// A composite state completes when its children/regions complete; if any
		// descendant can reach a final state, the composite is not stuck.
		if (n.compound || n.parallel) && descendantCanReachFinal(g, name, can) {
			continue
		}
		found = append(found, Finding{
			Kind:     KindCannotReachFinal,
			Severity: SeverityWarning,
			State:    name,
			Message:  fmt.Sprintf("no final state is reachable from %q; an instance here can never complete", name),
		})
	}
	sortFindings(found)
	r.Findings = append(r.Findings, found...)
}

// descendantCanReachFinal reports whether any descendant of a composite state
// can reach a final state.
func descendantCanReachFinal(g *graph, name string, can map[string]bool) bool {
	n := g.nodes[name]
	for _, c := range n.children {
		if can[c] {
			return true
		}
		if cn, ok := g.nodes[c]; ok && (cn.compound || cn.parallel) {
			if descendantCanReachFinal(g, c, can) {
				return true
			}
		}
	}
	return false
}
