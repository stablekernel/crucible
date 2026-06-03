package conformance

import (
	"context"
	"encoding/json"
	"fmt"

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
	AssertTraceLength    AssertionType = "TraceLength"
	AssertNoErrors       AssertionType = "NoErrors"
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
	ExitedStates    []string `json:"exitedStates,omitempty"`
	EnteredStates   []string `json:"enteredStates,omitempty"`
	Outcome         string   `json:"outcome"`
	Err             string   `json:"err,omitempty"`
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
	Effects    []string
	Err        error
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
// is supplied by the caller (the kernel binds guards and actions to it) and the
// starting state is taken from the scenario.
func RunAgainst[S comparable, E comparable, C any](
	m *state.Machine[S, E, C],
	sc Scenario,
	entity C,
	codec EventCodec[E],
	startState S,
) ScenarioResult[S] {
	// Full trace is required so EffectsEmitted, GuardsEvaluated, and the cascade
	// fields are populated for scenario assertion evaluation.
	inst := m.Cast(entity, state.WithInitialState(startState), state.WithFullTrace[S]())
	res := ScenarioResult[S]{FinalState: startState}
	tr := Trace{MachineID: m.Name(), FromState: sc.InitialState}

	for _, ev := range sc.Events {
		typed, ok := codec.Resolve(ev.Event)
		if !ok {
			res.Err = &ErrUnknownEvent{Name: ev.Event}
			break
		}
		fr := inst.Fire(context.Background(), typed)
		res.FinalState = fr.NewState
		res.Effects = append(res.Effects, traceEffectNames(fr.Trace.EffectsEmitted)...)
		tr.Steps = append(tr.Steps, stepFromKernel(fr))
		if fr.Err != nil && res.Err == nil {
			res.Err = fr.Err
		}
	}

	tr.ToState = fmt.Sprint(res.FinalState)
	res.Trace = tr
	res.Assertions = evaluate(sc.Assertions, res)
	return res
}

// stepFromKernel projects a kernel FireResult onto the serializable TraceStep.
func stepFromKernel[S comparable](fr state.FireResult[S]) TraceStep {
	st := TraceStep{
		Event:           fr.Trace.Event,
		FromState:       fr.Trace.FromState,
		ToState:         fmt.Sprint(fr.NewState),
		MatchedAt:       fr.Trace.MatchedAt,
		GuardsEvaluated: fr.Trace.GuardsEvaluated,
		EffectsEmitted:  traceEffectNames(fr.Trace.EffectsEmitted),
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
	default:
		return "Unknown"
	}
}
