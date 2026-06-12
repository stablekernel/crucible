package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/stablekernel/crucible/state"
)

// schemaVersion is the serialized scenario/trace schema version. It is emitted
// on every artifact so future additions are detectable without ambiguity.
const schemaVersion = 1

// AssertionType names a declarative scenario assertion.
type AssertionType string

// The v1 assertion set covers final-state, emitted effects, trace length, and
// the absence of errors — enough for generated and hand-authored scenarios.
const (
	AssertFinalState     AssertionType = "FinalState"
	AssertEffectsEmitted AssertionType = "EffectsEmitted"
	// AssertEffectsPayloads asserts the ORDERED, PAYLOAD-AWARE rendering of every
	// emitted effect ("name=payload"). Unlike AssertEffectsEmitted, which compares
	// only ref names, this catches a changed payload (e.g. a wrong timer duration)
	// even when the effect name is unchanged.
	AssertEffectsPayloads AssertionType = "EffectsPayloads"
	AssertTraceLength     AssertionType = "TraceLength"
	AssertNoErrors        AssertionType = "NoErrors"
)

// Assertion is a declarative expectation about a scenario run. Assertions are
// descriptions, not predicates: a run records each as pass or fail and leaves
// the caller to decide whether a failure is fatal.
type Assertion struct {
	Type     AssertionType `json:"type"`
	Expected any           `json:"expected"`
}

// Event is one step of a scenario: the event to fire, named so the artifact is
// portable across the typed event domain.
type Event struct {
	Event string `json:"event"`
}

// Scenario describes what to do — fire this sequence of events against a machine
// from a starting state — plus the assertions to evaluate. It is a serializable
// artifact: generated scenarios can be committed as goldens and replayed.
type Scenario struct {
	SchemaVersion int         `json:"schemaVersion"`
	MachineID     string      `json:"machineId"`
	Name          string      `json:"name,omitempty"`
	InitialState  string      `json:"initialState"`
	Events        []Event     `json:"events"`
	Assertions    []Assertion `json:"assertions,omitempty"`
}

// MarshalJSON emits the scenario with its schema version pinned.
func (s Scenario) MarshalJSON() ([]byte, error) {
	type alias Scenario
	a := alias(s)
	a.SchemaVersion = schemaVersion
	return json.Marshal(a)
}

// LoadScenario parses a scenario from its JSON form, rejecting an unsupported
// schema version.
func LoadScenario(data []byte) (Scenario, error) {
	var s Scenario
	if err := json.Unmarshal(data, &s); err != nil {
		return Scenario{}, fmt.Errorf("conformance: load scenario: %w", err)
	}
	if s.SchemaVersion != 0 && s.SchemaVersion != schemaVersion {
		return Scenario{}, &ErrSchemaVersion{Got: s.SchemaVersion, Want: schemaVersion}
	}
	return s, nil
}

// EventNamer renders a typed event to the stable name used in scenarios and
// traces. It must agree with the kernel's own rendering (fmt.Sprint of the
// event), which is what GenerateScenarios reads back from the IR.
type EventNamer[E comparable] func(E) string

// EventResolver maps a scenario's event name back to its typed value so a
// serialized scenario can be replayed against a typed machine. It returns false
// for an unknown name.
type EventResolver[E comparable] func(name string) (E, bool)

// EventCodec carries both directions of the event-name mapping. A consumer
// supplies one per event type; for an int-backed enum with a String method the
// Named direction is fmt.Sprint and the Resolve direction is a small lookup map.
type EventCodec[E comparable] struct {
	Named   EventNamer[E]
	Resolve EventResolver[E]
}

// TraceStep is one Fire's worth of recorded behavior, in serializable form. It
// mirrors the kernel Trace but renders effects and outcome as their stable
// string names so a step is portable and diffable.
type TraceStep struct {
	Event           string   `json:"event"`
	FromState       string   `json:"fromState"`
	ToState         string   `json:"toState"`
	MatchedAt       string   `json:"matchedAt,omitempty"`
	GuardsEvaluated []string `json:"guardsEvaluated,omitempty"`
	EffectsEmitted  []string `json:"effectsEmitted,omitempty"`
	// EffectPayloads renders each emitted effect's payload ("name=payload") in
	// emission order, so a trace divergence on payload (not just on the effect's
	// type/name) is captured and diffable.
	EffectPayloads []string `json:"effectPayloads,omitempty"`
	ExitedStates   []string `json:"exitedStates,omitempty"`
	EnteredStates  []string `json:"enteredStates,omitempty"`
	Outcome        string   `json:"outcome"`
	Err            string   `json:"err,omitempty"`
}

// Trace is the serializable record of a whole scenario run: the ordered steps
// plus the spanning from/to state. It is the unifying primitive — it renders a
// past run and is diffable against a committed expectation.
type Trace struct {
	SchemaVersion int         `json:"schemaVersion"`
	MachineID     string      `json:"machineId"`
	FromState     string      `json:"fromState"`
	ToState       string      `json:"toState"`
	Steps         []TraceStep `json:"steps"`
	// FinalContext is a stable rendering of the entity after the run, captured so a
	// divergence in context mutation is diffable from the serialized trace alone.
	FinalContext string `json:"finalContext,omitempty"`
}

// MarshalJSON emits the trace with its schema version pinned.
func (t Trace) MarshalJSON() ([]byte, error) {
	type alias Trace
	a := alias(t)
	a.SchemaVersion = schemaVersion
	return json.Marshal(a)
}

// AssertionResult records one assertion's verdict after a run.
type AssertionResult struct {
	Type     AssertionType `json:"type"`
	Expected any           `json:"expected"`
	Actual   any           `json:"actual"`
	Pass     bool          `json:"pass"`
}

// ScenarioResult is the outcome of running a scenario against a machine: the
// resulting state, the captured trace, the per-assertion verdicts, and any
// kernel error encountered along the way.
type ScenarioResult[S comparable] struct {
	FinalState S
	Trace      Trace
	Assertions []AssertionResult
	// Effects lists each emitted effect's ref name in EMISSION ORDER. Order is
	// significant: a reordered sequence is a regression, not an equivalence.
	Effects []string
	// EffectDetails lists each emitted effect's PAYLOAD-AWARE rendering
	// ("name=payload") in emission order, so a changed payload (e.g. a wrong timer
	// duration) is caught even when the ref name is unchanged.
	EffectDetails []string
	// FinalContext is a stable rendering of the entity after the last Fire,
	// captured so a divergence in context mutation is observable in a golden or an
	// oracle diff. It is best-effort: it renders whatever the entity's value is.
	FinalContext string
	Err          error
}

// Passed reports whether every assertion in the result passed.
func (r ScenarioResult[S]) Passed() bool {
	for _, a := range r.Assertions {
		if !a.Pass {
			return false
		}
	}
	return true
}

// RunAgainst fires the scenario's event sequence against a freshly Cast instance
// of the machine and builds a ScenarioResult. The codec resolves each event name
// to its typed value; an unresolved name is a fatal scenario error. The entity
// is supplied by the caller (the kernel binds guards and actions to it).
//
// The run starts from the typed startState the caller resolved. When the scenario
// also declares a non-empty InitialState, it must match startState's rendered
// form; a disagreement is reported as ErrInitialStateMismatch and the events are
// not fired, so a serialized scenario can never silently replay from a different
// state than it describes.
//
// RunAgainst accepts trailing RunOptions. They are additive: passing none
// preserves the original behavior, so the seam below (e.g. WithSnapshotSink,
// which captures the instance Snapshot after the run for snapshot/restore
// conformance) can grow without breaking existing callers.
func RunAgainst[S comparable, E comparable, C any](
	m *state.Machine[S, E, C],
	sc Scenario,
	entity C,
	codec EventCodec[E],
	startState S,
	opts ...RunOption[S, E, C],
) ScenarioResult[S] {
	cfg := runConfig[S, E, C]{}
	for _, o := range opts {
		o(&cfg)
	}
	res := ScenarioResult[S]{FinalState: startState}
	tr := Trace{MachineID: m.Name(), FromState: sc.InitialState}

	if sc.InitialState != "" {
		if resolved := fmt.Sprint(startState); resolved != sc.InitialState {
			res.Err = &ErrInitialStateMismatch{Declared: sc.InitialState, Resolved: resolved}
			tr.ToState = resolved
			res.FinalContext = renderContext(entity)
			tr.FinalContext = res.FinalContext
			res.Trace = tr
			res.Assertions = evaluate(sc.Assertions, res)
			return res
		}
	}

	// Full trace is required so EffectsEmitted, GuardsEvaluated, and the cascade
	// fields are populated for scenario assertion evaluation.
	inst := m.Cast(entity, state.WithInitialState(startState), state.WithFullTrace[S]())

	for _, ev := range sc.Events {
		typed, ok := codec.Resolve(ev.Event)
		if !ok {
			res.Err = &ErrUnknownEvent{Name: ev.Event}
			break
		}
		fr := inst.Fire(context.Background(), typed)
		res.FinalState = fr.NewState
		names := effectRefNames(traceEffectNames(fr.Trace.EffectsEmitted))
		res.Effects = append(res.Effects, names...)
		stepDetails := effectDetails(names, fr.Effects)
		res.EffectDetails = append(res.EffectDetails, stepDetails...)
		tr.Steps = append(tr.Steps, stepFromKernel(fr, stepDetails))
		if fr.Err != nil && res.Err == nil {
			res.Err = fr.Err
		}
	}

	tr.ToState = fmt.Sprint(res.FinalState)
	res.FinalContext = renderContext(entity)
	tr.FinalContext = res.FinalContext
	res.Trace = tr
	res.Assertions = evaluate(sc.Assertions, res)
	if cfg.snapshotSink != nil {
		cfg.snapshotSink(inst.Snapshot())
	}
	return res
}

// RunOption configures a RunAgainst invocation additively. Passing no options
// preserves RunAgainst's original behavior; this is the non-breaking seam through
// which snapshot/restore (and any future per-run capability) is exposed without
// changing RunAgainst's existing signature.
type RunOption[S comparable, E comparable, C any] func(*runConfig[S, E, C])

type runConfig[S comparable, E comparable, C any] struct {
	snapshotSink func(state.Snapshot[S, E, C])
}

// WithSnapshotSink captures the machine instance's Snapshot after the scenario's
// events have fired. It is the snapshot/restore conformance seam: a caller can
// snapshot the post-run instance, Restore it on another machine, and assert the
// two resume identically — without RunAgainst's signature ever changing. The sink
// is not called when the run aborts before casting an instance (e.g. an initial
// state mismatch), so it only ever observes a genuinely-run instance.
func WithSnapshotSink[S comparable, E comparable, C any](sink func(state.Snapshot[S, E, C])) RunOption[S, E, C] {
	return func(c *runConfig[S, E, C]) { c.snapshotSink = sink }
}

// effectDetails pairs each emitted effect's ref name with a PAYLOAD-AWARE
// rendering of the effect value, in emission order. The kernel reports the ref
// names (in EffectsEmitted) and the concrete effect values (in FireResult
// .Effects) as positionally-aligned slices; this zips them into "name=payload"
// labels so a changed payload is observable. When the two slices disagree in
// length (an effect failed mid-step and recorded a name without a value), the
// available names still render, with the payload shown as "?".
func effectDetails(names []string, effects []any) []string {
	if len(names) == 0 {
		return nil
	}
	out := make([]string, len(names))
	for i, n := range names {
		if i < len(effects) {
			out[i] = fmt.Sprintf("%s=%s", n, renderPayload(effects[i]))
			continue
		}
		out[i] = n + "=?"
	}
	return out
}

// renderPayload renders an effect value to a stable, payload-sensitive string.
// %#v is used so a struct's field values (e.g. a timer duration) are part of the
// rendering: a changed payload yields a different string and therefore fails an
// ordered, payload-aware comparison.
func renderPayload(v any) string {
	if v == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%#v", v)
}

// renderContext renders the entity after a run to a stable, VALUE-based string so
// a divergence in context mutation is observable without false positives from
// non-deterministic pointer addresses. It marshals the entity to JSON, which
// dereferences pointers and renders by value: two value-equal entities at
// different addresses render identically, and a real mutation diverges. When the
// entity cannot be marshaled (e.g. an unsupported type), it falls back to a
// dereferenced %+v rendering. It is best-effort: an entity whose meaningful state
// lives in unexported fields renders as "{}" under JSON, which is still
// deterministic (equal entities stay equal), just less informative.
func renderContext(entity any) string {
	if entity == nil {
		return ""
	}
	if data, err := json.Marshal(entity); err == nil {
		return string(data)
	}
	rv := reflect.ValueOf(entity)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return "<nil>"
		}
		return fmt.Sprintf("%+v", rv.Elem().Interface())
	}
	return fmt.Sprintf("%+v", entity)
}

// stepFromKernel projects a kernel FireResult onto the serializable TraceStep.
// payloads carries the per-step PAYLOAD-AWARE effect renderings ("name=payload")
// computed by the caller, so the step records both the effect labels and their
// payloads and a divergence in either is diffable.
func stepFromKernel[S comparable](fr state.FireResult[S], payloads []string) TraceStep {
	st := TraceStep{
		Event:           fr.Trace.Event,
		FromState:       fr.Trace.FromState,
		ToState:         fmt.Sprint(fr.NewState),
		MatchedAt:       fr.Trace.MatchedAt,
		GuardsEvaluated: fr.Trace.GuardsEvaluated,
		EffectsEmitted:  traceEffectNames(fr.Trace.EffectsEmitted),
		EffectPayloads:  payloads,
		ExitedStates:    fr.Trace.ExitedStates,
		EnteredStates:   fr.Trace.EnteredStates,
		Outcome:         outcomeName(fr.Trace.Outcome),
	}
	if fr.Err != nil {
		st.Err = fr.Err.Error()
	}
	return st
}

// outcomeName renders a kernel Outcome by its stable string name.
func outcomeName(o state.Outcome) string {
	switch o {
	case state.OutcomeSuccess:
		return "Success"
	case state.OutcomeInvalidTransition:
		return "InvalidTransition"
	case state.OutcomeGuardFailed:
		return "GuardFailed"
	case state.OutcomeGuardPanic:
		return "GuardPanic"
	case state.OutcomePolicyDenied:
		return "PolicyDenied"
	case state.OutcomeEffectError:
		return "EffectError"
	case state.OutcomeAssignFailed:
		return "AssignFailed"
	default:
		return "Unknown"
	}
}
