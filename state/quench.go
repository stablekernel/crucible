package state

import "fmt"

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

		for ti := range sd.state.Transitions {
			t := &sd.state.Transitions[ti]

			// Undeclared transition target.
			if _, ok := b.stateIndex[t.To]; !ok {
				diags = append(diags, diagnostic{Diagnostic: Diagnostic{
					Severity: diagError,
					Message:  fmt.Sprintf("transition from %v on %v targets undeclared state %v", t.From, t.On, t.To),
					SrcFile:  t.SrcFile,
					SrcLine:  t.SrcLine,
				}})
			}

			// Ambiguity check.
			if len(t.Guards) == 0 {
				if prev, seen := guardlessByEvent[t.On]; seen {
					_ = prev
					diags = append(diags, diagnostic{Diagnostic: Diagnostic{
						Severity: diagWarning,
						Message:  fmt.Sprintf("ambiguous guardless transitions from %v on %v", t.From, t.On),
						SrcFile:  t.SrcFile,
						SrcLine:  t.SrcLine,
					}})
				}
				guardlessByEvent[t.On]++
			}

			// Unresolved guard/action refs.
			b.checkRefs(&diags, "guard", t.Guards, "", t.SrcLine, refSrc{t.SrcFile, t.SrcLine})
			b.checkRefs(&diags, "action", t.Effects, "", t.SrcLine, refSrc{t.SrcFile, t.SrcLine})
		}
	}

	return diags
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
			_, ok = b.reg.actions[r.Name]
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
