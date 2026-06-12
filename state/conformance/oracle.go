package conformance

import (
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// CompareOption configures an oracle or round-trip comparison.
type CompareOption func(*compareConfig)

type compareConfig struct {
	ignoreEffects bool
	ignoreTrace   bool
}

// IgnoreEffects skips the emitted-effects comparison. Use it only when the two
// sides legitimately differ on effects (each use is a coverage hole).
func IgnoreEffects() CompareOption {
	return func(c *compareConfig) { c.ignoreEffects = true }
}

// IgnoreTrace skips the per-step trace comparison, comparing final state and
// effects only.
func IgnoreTrace() CompareOption {
	return func(c *compareConfig) { c.ignoreTrace = true }
}

// freshEntity returns an entity for a single scenario run. Both halves of an
// oracle comparison must start from an equivalent entity, so the harness asks
// the caller for a fresh one per run rather than reusing a mutated instance.
type freshEntity[C any] func() C

// CompareMachines runs every scenario against two machines built from the same
// state/event/context types and reports the divergences. It is the oracle
// pillar generalized to two machine implementations: the reference (canonical)
// and the subject (under test). Both are Cast from an entity drawn fresh per
// scenario so a mutated run never bleeds into the next comparison.
//
// A nil error means the subject conforms to the reference across every scenario.
func CompareMachines[S comparable, E comparable, C any](
	reference, subject *state.Machine[S, E, C],
	scenarios []Scenario,
	codec EventCodec[E],
	startState S,
	newEntity freshEntity[C],
	opts ...CompareOption,
) error {
	cfg := compareConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	var mismatches []Mismatch
	for _, sc := range scenarios {
		refRes := RunAgainst(reference, sc, newEntity(), codec, startState)
		subRes := RunAgainst(subject, sc, newEntity(), codec, startState)
		mismatches = append(mismatches, diffResults(sc.Name, refRes, subRes, cfg)...)
	}
	if len(mismatches) == 0 {
		return nil
	}
	return &ErrConformance{Mismatches: mismatches}
}

// diffResults compares two scenario results positionally and returns the
// field-level divergences.
func diffResults[S comparable](name string, ref, sub ScenarioResult[S], cfg compareConfig) []Mismatch {
	var out []Mismatch
	if fmt.Sprint(ref.FinalState) != fmt.Sprint(sub.FinalState) {
		out = append(out, Mismatch{
			Scenario:  name,
			Field:     "finalState",
			Reference: fmt.Sprint(ref.FinalState),
			Subject:   fmt.Sprint(sub.FinalState),
		})
	}
	if !cfg.ignoreEffects {
		// Effects are compared ORDER-SENSITIVELY: a reordered emission sequence is a
		// regression, not an equivalence.
		if !sameSequence(effectRefNames(ref.Effects), effectRefNames(sub.Effects)) {
			out = append(out, Mismatch{
				Scenario:  name,
				Field:     "effects",
				Reference: fmt.Sprint(effectRefNames(ref.Effects)),
				Subject:   fmt.Sprint(effectRefNames(sub.Effects)),
			})
		} else if !sameSequence(ref.EffectDetails, sub.EffectDetails) {
			// Names agree in order but a payload diverged (e.g. a changed timer
			// duration). Payload-aware comparison catches what the name comparison
			// cannot.
			out = append(out, Mismatch{
				Scenario:  name,
				Field:     "effects.payload",
				Reference: fmt.Sprint(ref.EffectDetails),
				Subject:   fmt.Sprint(sub.EffectDetails),
			})
		}
	}
	if fmt.Sprint(ref.FinalContext) != fmt.Sprint(sub.FinalContext) {
		out = append(out, Mismatch{
			Scenario:  name,
			Field:     "finalContext",
			Reference: ref.FinalContext,
			Subject:   sub.FinalContext,
		})
	}
	if !cfg.ignoreTrace {
		out = append(out, diffTraces(name, ref.Trace, sub.Trace)...)
	}
	return out
}

// diffTraces compares two traces step-by-step, surfacing length and per-step
// outcome divergences.
func diffTraces(name string, ref, sub Trace) []Mismatch {
	var out []Mismatch
	if len(ref.Steps) != len(sub.Steps) {
		out = append(out, Mismatch{
			Scenario:  name,
			Field:     "trace.len",
			Reference: fmt.Sprint(len(ref.Steps)),
			Subject:   fmt.Sprint(len(sub.Steps)),
		})
		return out
	}
	for i := range ref.Steps {
		rs, ss := ref.Steps[i], sub.Steps[i]
		if rs.Outcome != ss.Outcome {
			out = append(out, Mismatch{
				Scenario:  name,
				Field:     fmt.Sprintf("trace.step[%d].outcome", i),
				Reference: rs.Outcome,
				Subject:   ss.Outcome,
			})
		}
		if rs.ToState != ss.ToState {
			out = append(out, Mismatch{
				Scenario:  name,
				Field:     fmt.Sprintf("trace.step[%d].toState", i),
				Reference: rs.ToState,
				Subject:   ss.ToState,
			})
		}
		// Per-step effects are compared ORDER-SENSITIVELY by ref name, so a step
		// that reorders its emissions diverges here even when the whole-run effect
		// multiset is unchanged.
		if !sameSequence(rs.EffectsEmitted, ss.EffectsEmitted) {
			out = append(out, Mismatch{
				Scenario:  name,
				Field:     fmt.Sprintf("trace.step[%d].effects", i),
				Reference: fmt.Sprint(rs.EffectsEmitted),
				Subject:   fmt.Sprint(ss.EffectsEmitted),
			})
		} else if !sameSequence(rs.EffectPayloads, ss.EffectPayloads) {
			// Effect labels agree but a payload diverged within the step.
			out = append(out, Mismatch{
				Scenario:  name,
				Field:     fmt.Sprintf("trace.step[%d].effects.payload", i),
				Reference: fmt.Sprint(rs.EffectPayloads),
				Subject:   fmt.Sprint(ss.EffectPayloads),
			})
		}
	}
	return out
}
