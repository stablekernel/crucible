package state

import "fmt"

// GuardOp tags the kind of a node in a guard expression tree.
type GuardOp string

// Guard expression operators. A leaf is either a named-ref guard (resolved
// against the host registry) or the built-in stateIn guard; the internal nodes
// compose child results with boolean and/or/not. The string form is stable so
// the tree round-trips losslessly through JSON.
const (
	// GuardLeaf is a named-ref guard leaf: it carries a Ref bound to a host
	// GuardFn at Provide/Quench time, exactly like a plain transition guard.
	GuardLeaf GuardOp = "leaf"
	// GuardStateIn is the built-in in-state guard leaf: it is true when the
	// instance's active configuration includes the named state. It needs no
	// registration — the kernel evaluates it directly against the live spine.
	GuardStateIn GuardOp = "stateIn"
	// GuardAnd is true when every child is true; it short-circuits at the first
	// false child.
	GuardAnd GuardOp = "and"
	// GuardOr is true when any child is true; it short-circuits at the first
	// true child.
	GuardOr GuardOp = "or"
	// GuardNot inverts its single child.
	GuardNot GuardOp = "not"
)

// GuardNode is one node of a serializable guard expression tree. A leaf
// references a host-provided guard by name (with serializable params) or is the
// built-in stateIn guard; internal nodes compose children with and/or/not.
//
// The tree is pure, serializable data: like every other behavioral reference in
// the IR, leaf guards are named — never embedded closures — so a UI- or
// JSON-authored composite guard binds against the host registry at Provide and
// round-trips to and from JSON without losing structure. Arbitrary nesting is
// supported, e.g. And(Or(g1, g2), Not(g3)).
//
// The common case — a single named guard — stays the plain Transition.Guards
// slice; GuardNode is used only when a transition needs boolean composition or
// the stateIn built-in.
type GuardNode[S comparable] struct {
	Op GuardOp `json:"op"`

	// Ref is the named-ref guard for a GuardLeaf node. Zero for every other op.
	Ref *Ref `json:"ref,omitempty"`

	// In is the target state for a GuardStateIn node: the guard is true when this
	// state is in the instance's active configuration (its leaves and their
	// ancestor spine). Zero for every other op.
	In *S `json:"in,omitempty"`

	// Children are the operands of an and/or/not node. And/Or take one or more;
	// Not takes exactly one. Empty for leaf and stateIn nodes.
	Children []GuardNode[S] `json:"children,omitempty"`
}

// Guard builds a named-ref guard leaf with optional serializable params, the
// composable form of a single transition guard. It is the leaf used inside
// And/Or/Not.
func Guard[S comparable](name string, params ...map[string]any) GuardNode[S] {
	return GuardNode[S]{Op: GuardLeaf, Ref: &Ref{Name: name, Params: firstParams(params)}}
}

// StateIn builds the built-in in-state guard leaf: true when the instance's
// active configuration includes state. It is config-aware — it reads the live
// active leaves and their ancestors at evaluation time, so it works for atomic,
// compound, and parallel configurations ("in" means the state is somewhere in
// the active set/spine). It is a first-class built-in: the consumer never
// registers it. The name deliberately mirrors xstate v5 stateIn(...) for guard
// parity; renaming to In would break that documented parity contract.
//
//nolint:revive // StateIn mirrors xstate v5's stateIn built-in (parity).
func StateIn[S comparable](state S) GuardNode[S] {
	s := state
	return GuardNode[S]{Op: GuardStateIn, In: &s}
}

// And composes guards into a node true only when every operand is true,
// short-circuiting at the first false — consistent with the AND short-circuit
// of a plain multi-guard transition. Operands may be named-ref leaves, stateIn,
// or other combinators, nested arbitrarily. Mirrors xstate v5 and([...]).
func And[S comparable](nodes ...GuardNode[S]) GuardNode[S] {
	return GuardNode[S]{Op: GuardAnd, Children: append([]GuardNode[S](nil), nodes...)}
}

// Or composes guards into a node true when any operand is true, short-circuiting
// at the first true. Operands may be named-ref leaves, stateIn, or other
// combinators, nested arbitrarily. Mirrors xstate v5 or([...]).
func Or[S comparable](nodes ...GuardNode[S]) GuardNode[S] {
	return GuardNode[S]{Op: GuardOr, Children: append([]GuardNode[S](nil), nodes...)}
}

// Not inverts a single guard. Mirrors xstate v5 not(...).
func Not[S comparable](node GuardNode[S]) GuardNode[S] {
	return GuardNode[S]{Op: GuardNot, Children: []GuardNode[S]{node}}
}

// LeafRefs returns the named-ref guard leaves of a guard expression tree, in
// left-to-right order. The stateIn built-in carries no host ref and is omitted.
// It lets tooling (e.g. evolution diffing) enumerate the host guards a composite
// expression depends on.
func (g *GuardNode[S]) LeafRefs() []Ref { return g.leafRefs() }

// StateInTargets returns the target states of every stateIn leaf in the tree, in
// left-to-right order, so tooling can account for in-state dependencies a
// composite guard introduces.
func (g *GuardNode[S]) StateInTargets() []S {
	if g == nil {
		return nil
	}
	var out []S
	switch g.Op {
	case GuardStateIn:
		if g.In != nil {
			out = append(out, *g.In)
		}
	case GuardLeaf:
	default:
		for i := range g.Children {
			out = append(out, g.Children[i].StateInTargets()...)
		}
	}
	return out
}

// leafRefs collects the named-ref guard leaves of a guard expression tree, in
// left-to-right order, so the builder can validate that every leaf binds and the
// kernel can adopt them. The stateIn built-in carries no ref and is skipped.
func (g *GuardNode[S]) leafRefs() []Ref {
	if g == nil {
		return nil
	}
	var out []Ref
	switch g.Op {
	case GuardLeaf:
		if g.Ref != nil {
			out = append(out, *g.Ref)
		}
	case GuardStateIn:
		// built-in: no host ref to bind.
	default:
		for i := range g.Children {
			out = append(out, g.Children[i].leafRefs()...)
		}
	}
	return out
}

// validate reports the first structural problem in a guard expression tree, used
// by the builder lint so a malformed tree fails at Quench (a programmer error)
// rather than at Fire. It checks that leaves carry their payload and that the
// boolean nodes have the right arity.
func (g *GuardNode[S]) validate() error {
	if g == nil {
		return nil
	}
	switch g.Op {
	case GuardLeaf:
		if g.Ref == nil || g.Ref.Name == "" {
			return fmt.Errorf("guard leaf has no ref name")
		}
	case GuardStateIn:
		if g.In == nil {
			return fmt.Errorf("stateIn guard has no target state")
		}
	case GuardAnd, GuardOr:
		if len(g.Children) == 0 {
			return fmt.Errorf("%s guard has no operands", g.Op)
		}
		for i := range g.Children {
			if err := g.Children[i].validate(); err != nil {
				return err
			}
		}
	case GuardNot:
		if len(g.Children) != 1 {
			return fmt.Errorf("not guard requires exactly one operand, got %d", len(g.Children))
		}
		return g.Children[0].validate()
	default:
		return fmt.Errorf("unknown guard op %q", g.Op)
	}
	return nil
}

// guardEval is the outcome of evaluating a guard expression node: whether it
// passed, and — when it failed and the failing leaf is unambiguous — the leaf
// name(s) that caused the failure, so the kernel can report which leaf failed
// cheaply. A guard that panicked or hit an unbound ref surfaces err.
type guardEval struct {
	ok          bool
	err         error
	failedLeafs []string
}

// evalGuardExpr evaluates a guard expression tree against the entity and the
// instance's live active configuration, with the same short-circuit semantics
// as xstate v5: And stops at the first false, Or stops at the first true, Not
// inverts. A leaf guard that panics or fails to bind stops evaluation and
// surfaces the typed error (ErrGuardPanic), exactly like a plain transition
// guard. The stateIn built-in reads the active spine, so it is correct for
// atomic, compound, and parallel configurations.
func (i *Instance[S, E, C]) evalGuardExpr(g *GuardNode[S], entity C, tr *Trace) guardEval {
	if g == nil {
		return guardEval{ok: true}
	}
	switch g.Op {
	case GuardLeaf:
		name := ""
		if g.Ref != nil {
			name = g.Ref.Name
		}
		if tr != nil {
			tr.GuardsEvaluated = append(tr.GuardsEvaluated, name)
		}
		ok, err := i.machine.evalGuard(*g.Ref, entity)
		if err != nil {
			return guardEval{err: err}
		}
		if !ok {
			return guardEval{failedLeafs: []string{name}}
		}
		return guardEval{ok: true}

	case GuardStateIn:
		name := stateInName(*g.In)
		if tr != nil {
			tr.GuardsEvaluated = append(tr.GuardsEvaluated, name)
		}
		if i.inConfiguration(*g.In) {
			return guardEval{ok: true}
		}
		return guardEval{failedLeafs: []string{name}}

	case GuardAnd:
		for k := range g.Children {
			res := i.evalGuardExpr(&g.Children[k], entity, tr)
			if res.err != nil {
				return res
			}
			if !res.ok {
				// Short-circuit at the first false: the composite failed because of
				// this operand, so report its failing leaf(s).
				return res
			}
		}
		return guardEval{ok: true}

	case GuardOr:
		var failed []string
		for k := range g.Children {
			res := i.evalGuardExpr(&g.Children[k], entity, tr)
			if res.err != nil {
				return res
			}
			if res.ok {
				// Short-circuit at the first true.
				return guardEval{ok: true}
			}
			failed = append(failed, res.failedLeafs...)
		}
		// No operand passed: the composite failed; report every leaf that failed.
		return guardEval{failedLeafs: failed}

	case GuardNot:
		res := i.evalGuardExpr(&g.Children[0], entity, tr)
		if res.err != nil {
			return res
		}
		if res.ok {
			// The child passed, so Not fails; the failure is the negation itself.
			return guardEval{failedLeafs: []string{"not(" + joinLeafs(res.failedLeafs) + ")"}}
		}
		return guardEval{ok: true}

	default:
		return guardEval{err: &ErrGuardPanic{GuardName: string(g.Op), Recovered: "unknown guard op"}}
	}
}

// inConfiguration reports whether state is in the instance's active
// configuration: any active leaf, or any ancestor of an active leaf (the active
// spine). This is the in-state test stateIn relies on — true when the named
// state is part of the currently-active compound/parallel configuration, not
// only when it is a leaf.
func (i *Instance[S, E, C]) inConfiguration(state S) bool {
	for _, leaf := range i.Configuration() {
		for _, anc := range i.machine.ancestors(leaf) {
			if anc == state {
				return true
			}
		}
	}
	return false
}

// projectGuardNode erases a guard expression tree's state-type parameter into
// the any-typed shape the Trace exposes, preserving structure, leaf refs, and
// stateIn targets.
func projectGuardNode[S comparable](g *GuardNode[S]) *GuardNode[any] {
	if g == nil {
		return nil
	}
	out := &GuardNode[any]{Op: g.Op}
	if g.Ref != nil {
		r := *g.Ref
		out.Ref = &r
	}
	if g.In != nil {
		var in any = *g.In
		out.In = &in
	}
	for k := range g.Children {
		if c := projectGuardNode(&g.Children[k]); c != nil {
			out.Children = append(out.Children, *c)
		}
	}
	return out
}

// cloneGuardNode deep-copies a guard expression tree so a deep-copied transition
// never shares backing slices or pointers with the live machine.
func cloneGuardNode[S comparable](g *GuardNode[S]) *GuardNode[S] {
	if g == nil {
		return nil
	}
	out := &GuardNode[S]{Op: g.Op}
	if g.Ref != nil {
		r := *g.Ref
		out.Ref = &r
	}
	if g.In != nil {
		in := *g.In
		out.In = &in
	}
	for k := range g.Children {
		if c := cloneGuardNode(&g.Children[k]); c != nil {
			out.Children = append(out.Children, *c)
		}
	}
	return out
}

// renderGuardExpr renders a guard expression tree as a compact human-readable
// label for visualization, e.g. and(or(a,stateIn(x)),not(c)).
func renderGuardExpr[S comparable](g *GuardNode[S]) string {
	if g == nil {
		return ""
	}
	switch g.Op {
	case GuardLeaf:
		if g.Ref != nil {
			return g.Ref.Name
		}
		return "?"
	case GuardStateIn:
		if g.In != nil {
			return stateInName(*g.In)
		}
		return "stateIn(?)"
	case GuardNot:
		return "not(" + renderGuardExpr(&g.Children[0]) + ")"
	case GuardAnd, GuardOr:
		parts := make([]string, 0, len(g.Children))
		for k := range g.Children {
			parts = append(parts, renderGuardExpr(&g.Children[k]))
		}
		return string(g.Op) + "(" + joinLeafs(parts) + ")"
	default:
		return string(g.Op)
	}
}

// stateInName renders a stateIn leaf name for the trace and diagnostics.
func stateInName[S comparable](s S) string { return "stateIn(" + fmtState(s) + ")" }

// joinLeafs renders a set of leaf names for a composite failure message.
func joinLeafs(names []string) string {
	switch len(names) {
	case 0:
		return ""
	case 1:
		return names[0]
	default:
		out := names[0]
		for _, n := range names[1:] {
			out += "," + n
		}
		return out
	}
}
