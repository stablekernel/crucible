package state

import "fmt"

// isZero reports whether a comparable value equals its type's zero value. Used
// to tell a targetless wildcard (no GoTo) from one with an explicit target.
func isZero[S comparable](s S) bool {
	var zero S
	return s == zero
}

// Diagnostic severities used by lint/Temper/Quench.
const (
	diagError   = "error"
	diagWarning = "warning"
)

// diagnostic is the internal lint finding; it carries the public Diagnostic
// plus an optional typed error for unbound refs so Quench can panic with the
// exact errors.As-able value.
type diagnostic struct {
	Diagnostic
	unboundRef *ErrUnboundRef
}

// quenchError wraps a non-ref lint finding so Quench panics with an error value
// (rather than a bare string), keeping the panic recover()-able as an error.
type quenchError struct {
	Diagnostic diagnostic
}

func (e *quenchError) Error() string {
	d := e.Diagnostic
	if d.SrcFile != "" {
		return fmt.Sprintf("crucible/state: quench: %s (%s:%d)", d.Message, d.SrcFile, d.SrcLine)
	}
	return fmt.Sprintf("crucible/state: quench: %s", d.Message)
}

// lint validates the builder's current definition and returns all findings.
// It never mutates the builder. Quench turns errors (and, under Strict,
// warnings) into panics; Temper returns them verbatim.
func (b *Builder[S, E, C]) lint() []diagnostic {
	var diags []diagnostic

	// HSM findings recorded during the chained build (SubState outside a block,
	// missing region/superstate Initial, unclosed blocks).
	diags = append(diags, b.hsmDiags...)

	// Unclosed SuperState / Region blocks at Quench.
	for _, blk := range b.blocks {
		kind := "SuperState"
		if blk.kind == blockRegion {
			kind = "Region"
		}
		diags = append(diags, diagnostic{Diagnostic: Diagnostic{
			Severity: diagError,
			Message:  "unclosed " + kind + " block",
			SrcFile:  blk.srcFile,
			SrcLine:  blk.srcLine,
		}})
	}

	// An outgoing transition on a Final state is a programmer error.
	for _, sd := range b.states {
		if sd.state.IsFinal && len(sd.state.Transitions) > 0 {
			t := sd.state.Transitions[0]
			diags = append(diags, diagnostic{Diagnostic: Diagnostic{
				Severity: diagError,
				Message:  "final state declares an outgoing transition",
				SrcFile:  t.SrcFile,
				SrcLine:  t.SrcLine,
			}})
		}
		// A state declaring both substates and regions is invalid.
		if len(sd.state.Regions) > 0 && b.hasChildSubstates(sd) {
			diags = append(diags, diagnostic{Diagnostic: Diagnostic{
				Severity: diagError,
				Message:  "state declares both substates and regions",
			}})
		}

		// History pseudo-states (DSL path: tagged on a flat stateDef) must live
		// directly in a compound state, carry no substructure, and any DefaultTo
		// target must be declared.
		if sd.isHistory || (sd.state.HistoryType != HistoryNone && sd.hasParent) {
			b.checkHistoryFlat(&diags, sd)
		}
	}

	// IR (prebuilt) path: history pseudo-states arrive nested under their parent
	// compound, so walk the nested structure to validate them.
	if b.prebuilt {
		for _, sd := range b.states {
			b.checkHistoryNested(&diags, &sd.state, nil)
		}
	}

	// Missing initial state.
	if !b.hasInitial {
		diags = append(diags, diagnostic{Diagnostic: Diagnostic{
			Severity: diagError,
			Message:  "missing Initial state",
		}})
	} else if _, ok := b.stateIndex[b.initial]; !ok {
		diags = append(diags, diagnostic{Diagnostic: Diagnostic{
			Severity: diagError,
			Message:  fmt.Sprintf("Initial state %v was never declared", b.initial),
		}})
	}

	// Missing CurrentStateFn. This is a warning, not a hard error: the function
	// is not serializable, so a machine rehydrated from JSON legitimately lacks
	// it (callers Cast from an explicit state). Strict mode rejects it.
	if b.currentStateFn == nil {
		diags = append(diags, diagnostic{Diagnostic: Diagnostic{
			Severity: diagWarning,
			Message:  "missing CurrentStateFn (cannot derive current state from an entity)",
		}})
	}

	for _, sd := range b.states {
		// Guardless-transition ambiguity: at most one guardless transition per
		// (from, event) pair, else firing is non-deterministic.
		guardlessByEvent := map[E]int{}

		// Validate state-level refs against the registry.
		b.checkRefs(&diags, "action", sd.state.OnEntry, sd.state.OwnedBy, 0)
		b.checkRefs(&diags, "action", sd.state.OnExit, sd.state.OwnedBy, 0)
		b.checkRefs(&diags, "action", sd.state.OnDone, sd.state.OwnedBy, 0)

		guardlessWildcard := 0
		for ti := range sd.state.Transitions {
			t := &sd.state.Transitions[ti]

			// A forbidden transition is a pure block: it declares no target, guards,
			// or effects, so the remaining target/ref checks do not apply.
			if t.Forbidden {
				if t.Reenter || len(t.Guards) > 0 || t.GuardExpr != nil || len(t.Effects) > 0 || len(t.Raise) > 0 {
					diags = append(diags, diagnostic{Diagnostic: Diagnostic{
						Severity: diagError,
						Message:  fmt.Sprintf("forbidden transition from %v must not declare a target, guards, effects, or raise", t.From),
						SrcFile:  t.SrcFile,
						SrcLine:  t.SrcLine,
					}})
				}
				continue
			}

			// Undeclared transition target. A targetless wildcard catch-all (no
			// GoTo) is an internal action-only transition and is exempt.
			targetlessWildcard := t.Wildcard && isZero(t.To)
			if _, ok := b.stateIndex[t.To]; !ok && !targetlessWildcard {
				diags = append(diags, diagnostic{Diagnostic: Diagnostic{
					Severity: diagError,
					Message:  fmt.Sprintf("transition from %v on %v targets undeclared state %v", t.From, t.On, t.To),
					SrcFile:  t.SrcFile,
					SrcLine:  t.SrcLine,
				}})
			}

			// Ambiguity check. Wildcard catch-alls are counted separately: more than
			// one guardless wildcard at a state is ambiguous, but a wildcard never
			// conflicts with a specific-event transition (specific outranks it). A
			// composite guard expression makes a transition guarded, so it is exempt
			// from the guardless-ambiguity check just like a plain guard.
			if len(t.Guards) == 0 && t.GuardExpr == nil {
				switch {
				case t.Wildcard:
					if guardlessWildcard >= 1 {
						diags = append(diags, diagnostic{Diagnostic: Diagnostic{
							Severity: diagWarning,
							Message:  fmt.Sprintf("ambiguous guardless wildcard transitions from %v", t.From),
							SrcFile:  t.SrcFile,
							SrcLine:  t.SrcLine,
						}})
					}
					guardlessWildcard++
				default:
					if _, seen := guardlessByEvent[t.On]; seen {
						diags = append(diags, diagnostic{Diagnostic: Diagnostic{
							Severity: diagWarning,
							Message:  fmt.Sprintf("ambiguous guardless transitions from %v on %v", t.From, t.On),
							SrcFile:  t.SrcFile,
							SrcLine:  t.SrcLine,
						}})
					}
					guardlessByEvent[t.On]++
				}
			}

			// Unresolved guard/action refs.
			b.checkRefs(&diags, "guard", t.Guards, "", t.SrcLine, refSrc{t.SrcFile, t.SrcLine})
			b.checkRefs(&diags, "action", t.Effects, "", t.SrcLine, refSrc{t.SrcFile, t.SrcLine})

			// A composite guard expression must be structurally well-formed, and
			// every named-ref leaf must bind against the registry. The stateIn
			// built-in carries no ref and needs no binding.
			if t.GuardExpr != nil {
				if err := t.GuardExpr.validate(); err != nil {
					diags = append(diags, diagnostic{Diagnostic: Diagnostic{
						Severity: diagError,
						Message:  fmt.Sprintf("malformed guard expression on transition from %v: %v", t.From, err),
						SrcFile:  t.SrcFile,
						SrcLine:  t.SrcLine,
					}})
				}
				b.checkRefs(&diags, "guard", t.GuardExpr.leafRefs(), "", t.SrcLine, refSrc{t.SrcFile, t.SrcLine})
			}
		}
	}

	return diags
}

// checkHistoryFlat validates a history pseudo-state declared via the DSL, where
// its placement is recorded on the flat stateDef. It must sit directly inside a
// compound state, declare no substructure or transitions, and any DefaultTo
// target must be a declared state.
func (b *Builder[S, E, C]) checkHistoryFlat(diags *[]diagnostic, sd *stateDef[S, E, C]) {
	s := &sd.state
	if !sd.hasParent {
		*diags = append(*diags, diagnostic{Diagnostic: Diagnostic{
			Severity: diagError,
			Message:  fmt.Sprintf("history state %v is not contained in a compound state", s.Name),
		}})
	} else if parent, ok := b.stateIndex[sd.parent]; ok {
		if sd.region != "" || len(parent.state.Regions) > 0 {
			*diags = append(*diags, diagnostic{Diagnostic: Diagnostic{
				Severity: diagError,
				Message:  fmt.Sprintf("history state %v must live in a compound (non-parallel) state", s.Name),
			}})
		}
	}
	b.checkHistoryShape(diags, s)
}

// checkHistoryNested validates a history pseudo-state reached by walking the
// nested IR structure. parent is the immediately enclosing state (nil at top
// level).
func (b *Builder[S, E, C]) checkHistoryNested(diags *[]diagnostic, s *State[S, E, C], parent *State[S, E, C]) {
	if s.HistoryType != HistoryNone {
		switch {
		case parent == nil:
			*diags = append(*diags, diagnostic{Diagnostic: Diagnostic{
				Severity: diagError,
				Message:  fmt.Sprintf("history state %v is not contained in a compound state", s.Name),
			}})
		case !isCompound(parent):
			*diags = append(*diags, diagnostic{Diagnostic: Diagnostic{
				Severity: diagError,
				Message:  fmt.Sprintf("history state %v must live in a compound (non-parallel) state", s.Name),
			}})
		}
		b.checkHistoryShape(diags, s)
	}
	for i := range s.Children {
		b.checkHistoryNested(diags, &s.Children[i], s)
	}
	for ri := range s.Regions {
		for i := range s.Regions[ri].States {
			// A region is parallel substructure: a history state nested here is in a
			// region, not a compound — flag via the parent check above.
			b.checkHistoryNested(diags, &s.Regions[ri].States[i], s)
		}
	}
}

// checkHistoryShape enforces that a history pseudo-state carries no substructure
// (children, regions, transitions) and that its DefaultTo target is declared.
func (b *Builder[S, E, C]) checkHistoryShape(diags *[]diagnostic, s *State[S, E, C]) {
	if len(s.Children) > 0 || len(s.Regions) > 0 || len(s.Transitions) > 0 {
		*diags = append(*diags, diagnostic{Diagnostic: Diagnostic{
			Severity: diagError,
			Message:  fmt.Sprintf("history state %v must not declare substates, regions, or transitions", s.Name),
		}})
	}
	if s.HistoryDefault != nil {
		if _, ok := b.stateIndex[*s.HistoryDefault]; !ok {
			*diags = append(*diags, diagnostic{Diagnostic: Diagnostic{
				Severity: diagError,
				Message:  fmt.Sprintf("history state %v default target %v was never declared", s.Name, *s.HistoryDefault),
			}})
		}
	}
}

// refSrc optionally carries a source site for a ref check.
type refSrc struct {
	file string
	line int
}

// checkRefs appends an ErrUnboundRef diagnostic for every ref that does not
// resolve in the builder's registry. The trailing src is optional.
func (b *Builder[S, E, C]) checkRefs(diags *[]diagnostic, kind string, refs []Ref, _ string, _ int, src ...refSrc) {
	var file string
	var line int
	if len(src) > 0 {
		file, line = src[0].file, src[0].line
	}
	for _, r := range refs {
		var ok bool
		switch kind {
		case "guard":
			_, ok = b.reg.guards[r.Name]
		case "action":
			// Kernel built-in actions (e.g. the Cancel built-in) resolve without a
			// host registration, mirroring the stateIn guard built-in.
			ok = isBuiltinAction(r.Name) || func() bool { _, f := b.reg.actions[r.Name]; return f }()
		}
		if !ok {
			ub := &ErrUnboundRef{Kind: kind, Name: r.Name}
			*diags = append(*diags, diagnostic{
				Diagnostic: Diagnostic{
					Severity: diagError,
					Message:  ub.Error(),
					SrcFile:  file,
					SrcLine:  line,
				},
				unboundRef: ub,
			})
		}
	}
}

// Temper returns public Diagnostics (the internal typed-error carrier is
// dropped). lint() returns the internal form; this converts it.
func toPublicDiagnostics(in []diagnostic) []Diagnostic {
	if len(in) == 0 {
		return nil
	}
	out := make([]Diagnostic, 0, len(in))
	for _, d := range in {
		out = append(out, d.Diagnostic)
	}
	return out
}
