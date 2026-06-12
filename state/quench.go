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
	unboundRef *UnboundRefError
	// regionEscape and historyCrossRegion carry the typed region-lint errors so
	// Quench can panic with the exact errors.As-able value, mirroring unboundRef.
	regionEscape       *RegionEscapeError
	historyCrossRegion *HistoryCrossRegionError
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

	// Region-scoped transition validity: a region-internal transition must not
	// target a state outside its owning region (T7 escape), nor a history
	// pseudo-state owned by a different region (K2 cross-region history target).
	b.checkRegionTransitions(&diags)

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
		b.checkRefs(&diags, "assign", sd.state.OnEntryAssign, sd.state.OwnedBy, 0)
		b.checkRefs(&diags, "assign", sd.state.OnExitAssign, sd.state.OwnedBy, 0)

		// Validate every invoked service's Src ref against the service registry,
		// surfacing an unbound service as the same typed *UnboundRefError the DSL and
		// IR paths raise for guards and actions. Child-MACHINE actor invocations
		// (ActorKindMachine) are exempt: their Src binds at the host ActorSystem's
		// actor palette, not the service registry, and an unbound actor src is
		// surfaced at spawn time (routed through the parent's onError) rather than at
		// Quench.
		for ix := range sd.state.Invoke {
			if sd.state.Invoke[ix].Kind == ActorKindMachine {
				continue
			}
			b.checkRefs(&diags, "service", []Ref{sd.state.Invoke[ix].Src}, "", 0)
		}

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
			b.checkRefs(&diags, "assign", t.Assigns, "", t.SrcLine, refSrc{t.SrcFile, t.SrcLine})

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

				// Type-check Core expression leaves (field-refs and the literals
				// they compare against) against the attached ContextSchema, when one
				// is present. A guard with no schema still evaluates dynamically; the
				// check is skipped rather than failing the build.
				if b.envelope.context != nil {
					for _, err := range typeCheckCoreExpr(t.GuardExpr, b.envelope.context) {
						diags = append(diags, diagnostic{Diagnostic: Diagnostic{
							Severity: diagError,
							Message:  fmt.Sprintf("guard expression on transition from %v: %v", t.From, err),
							SrcFile:  t.SrcFile,
							SrcLine:  t.SrcLine,
						}})
					}
				}
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

// regionMembership records, for one state, the ordered chain of orthogonal
// regions that enclose it, outermost first. Each entry pairs the owning parallel
// state's label with the region name. A flat (non-region) state has an empty
// chain.
type regionMembership struct {
	parallels []string // owning parallel-state labels, outermost first
	regions   []string // region names, aligned with parallels
}

// innermost returns the innermost (parallel, region) pair and whether the state
// sits inside any region at all.
func (rm regionMembership) innermost() (parallel, region string, ok bool) {
	if len(rm.regions) == 0 {
		return "", "", false
	}
	last := len(rm.regions) - 1
	return rm.parallels[last], rm.regions[last], true
}

// contains reports whether (parallel, region) appears anywhere in the chain — i.e.
// whether this state lies inside that specific region (possibly nested deeper).
func (rm regionMembership) contains(parallel, region string) bool {
	for i := range rm.regions {
		if rm.parallels[i] == parallel && rm.regions[i] == region {
			return true
		}
	}
	return false
}

// regionChains computes the region membership of every declared state for the
// DSL (flat stateDef) path by walking each state's parent chain. The chain is
// built outermost-first. Keyed by the state's label.
func (b *Builder[S, E, C]) regionChains() map[S]regionMembership {
	out := make(map[S]regionMembership, len(b.states))
	var chainOf func(sd *stateDef[S, E, C]) regionMembership
	chainOf = func(sd *stateDef[S, E, C]) regionMembership {
		if !sd.hasParent {
			return regionMembership{}
		}
		parent, ok := b.stateIndex[sd.parent]
		var rm regionMembership
		if ok {
			rm = chainOf(parent)
		}
		// sd.region is set only when sd sits directly inside a Region block; its
		// parent is then the owning parallel state.
		if sd.region != "" {
			rm.parallels = append(append([]string(nil), rm.parallels...), fmt.Sprint(sd.parent))
			rm.regions = append(append([]string(nil), rm.regions...), sd.region)
		}
		return rm
	}
	for _, sd := range b.states {
		out[sd.state.Name] = chainOf(sd)
	}
	return out
}

// checkRegionTransitions validates that region-internal transitions stay within
// their owning region. It is a no-op for the prebuilt (IR) path, where region
// substructure is nested rather than flat; the DSL Forge path (where region
// states are flat stateDefs indexed in stateIndex) is the one that can express
// an escaping target.
func (b *Builder[S, E, C]) checkRegionTransitions(diags *[]diagnostic) {
	if b.prebuilt {
		return
	}
	chains := b.regionChains()
	for _, sd := range b.states {
		parallel, region, inRegion := chains[sd.state.Name].innermost()
		if !inRegion {
			continue // flat / compound transitions are unconstrained here
		}
		for ti := range sd.state.Transitions {
			t := &sd.state.Transitions[ti]
			if isZero(t.To) || t.Forbidden {
				continue
			}
			target, ok := b.stateIndex[t.To]
			if !ok {
				continue // undeclared target is reported by the main target check
			}
			// Cross-region history target (K2 reject variant): a transition that
			// targets a history pseudo-state belonging to a different region is
			// ambiguous. The in-region history restore is well-defined and handled on
			// the commit path; only the cross-region target is rejected here. This is
			// checked before the escape rule so the more specific history message wins.
			isHistory := target.isHistory || target.state.HistoryType != HistoryNone
			if isHistory && !chains[t.To].contains(parallel, region) {
				he := &HistoryCrossRegionError{
					Region:  region,
					From:    fmt.Sprint(t.From),
					History: fmt.Sprint(t.To),
				}
				*diags = append(*diags, diagnostic{
					Diagnostic: Diagnostic{
						Severity: diagError,
						Message:  he.Error(),
						SrcFile:  t.SrcFile,
						SrcLine:  t.SrcLine,
					},
					historyCrossRegion: he,
				})
				continue
			}
			// Region escape (T7): the target lies outside the source's region.
			if !chains[t.To].contains(parallel, region) {
				re := &RegionEscapeError{
					Region: region,
					From:   fmt.Sprint(t.From),
					To:     fmt.Sprint(t.To),
				}
				*diags = append(*diags, diagnostic{
					Diagnostic: Diagnostic{
						Severity: diagError,
						Message:  re.Error(),
						SrcFile:  t.SrcFile,
						SrcLine:  t.SrcLine,
					},
					regionEscape: re,
				})
			}
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

// checkRefs appends an UnboundRefError diagnostic for every ref that does not
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
		case "service":
			_, ok = b.reg.services[r.Name]
		case "assign":
			_, ok = b.reg.assigns[r.Name]
		}
		if !ok {
			ub := &UnboundRefError{Kind: kind, Name: r.Name}
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
