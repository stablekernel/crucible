package state

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"time"
)

// P is a convenience alias for serializable params attached to a named Ref.
type P = map[string]any

// Ref is a named reference to a host-provided implementation plus serializable
// params. The IR carries Refs; the registry binds Name -> func at
// Provide/Quench time.
type Ref struct {
	Name   string         `json:"name"`
	Params map[string]any `json:"params,omitempty"`
}

// HistoryType is the reserved drop-in surface for shallow/deep history states.
type HistoryType int

// History kinds. HistoryNone is the v1 default (no history); shallow and deep
// are reserved for the deferred history-state feature.
const (
	HistoryNone HistoryType = iota
	HistoryShallow
	HistoryDeep
)

// WaitMode tags a transition's synchronization expectation. The kernel only
// stores the tag; the consumer acts on it.
type WaitMode int

// Wait modes. SyncReply awaits a reply, FireAndForget emits and moves on, and
// ValidatePoll signals the consumer to poll the entity (re-running Assay) until
// it validates.
const (
	SyncReply WaitMode = iota
	FireAndForget
	ValidatePoll
)

// State is a node in the machine graph.
type State[S comparable, E comparable, C any] struct {
	Name        S                     `json:"name"`
	OwnedBy     string                `json:"ownedBy,omitempty"`
	Transitions []Transition[S, E, C] `json:"transitions,omitempty"`

	OnEntry []Ref `json:"onEntry,omitempty"`
	OnExit  []Ref `json:"onExit,omitempty"`
	IsFinal bool  `json:"isFinal,omitempty"`
	OnDone  []Ref `json:"onDone,omitempty"`

	// Reserved drop-in surface.
	HistoryType HistoryType       `json:"historyType,omitempty"`
	Invoke      []Ref             `json:"invoke,omitempty"`
	Parent      *State[S, E, C]   `json:"-"`
	Children    []*State[S, E, C] `json:"-"`
}

// Transition is a directed edge.
type Transition[S comparable, E comparable, C any] struct {
	From S `json:"from"`
	To   S `json:"to"`
	On   E `json:"on"`

	Guards   []Ref    `json:"guards,omitempty"`
	Effects  []Ref    `json:"effects,omitempty"`
	WaitMode WaitMode `json:"waitMode,omitempty"`

	Internal  bool           `json:"internal,omitempty"`
	EventLess bool           `json:"eventLess,omitempty"`
	After     *time.Duration `json:"after,omitempty"`

	SrcFile string `json:"srcFile,omitempty"`
	SrcLine int    `json:"srcLine,omitempty"`
}

// Effect is an abstract, domain-defined payload. The kernel never inspects it.
type Effect = any

// Outcome classifies the result recorded in a Trace.
type Outcome int

// Outcomes recorded in a Trace, one per Fire: success or the specific failure
// class that stopped the transition.
const (
	OutcomeSuccess Outcome = iota
	OutcomeInvalidTransition
	OutcomeGuardFailed
	OutcomeGuardPanic
	OutcomePolicyDenied
	OutcomeEffectError
)

// Trace is the kernel's canonical observability surface — pure data recorded
// on every Fire.
type Trace struct {
	Machine            string                     `json:"machine,omitempty"`
	Event              string                     `json:"event,omitempty"`
	FromState          string                     `json:"fromState,omitempty"`
	SelectedTransition *Transition[any, any, any] `json:"-"`
	GuardsEvaluated    []string                   `json:"guardsEvaluated,omitempty"`
	PoliciesEvaluated  []string                   `json:"policiesEvaluated,omitempty"`
	EffectsEmitted     []string                   `json:"effectsEmitted,omitempty"`
	Microsteps         []string                   `json:"microsteps,omitempty"`
	Outcome            Outcome                    `json:"outcome"`
}

// FireResult is the result of a single Fire.
type FireResult[S comparable] struct {
	NewState S
	Effects  []Effect
	Trace    Trace
	Err      error
}

// BatchResult is the result of a batch fire (FireSeq / FireEach).
type BatchResult[S comparable] struct {
	Steps []FireResult[S]
	Trace Trace
	Err   error
}

// GuardCtx is passed to a bound guard function at run time.
type GuardCtx[C any] struct {
	Entity C
	Params map[string]any
}

// GuardFn is a pure predicate on the entity.
type GuardFn[C any] func(ctx GuardCtx[C]) bool

// ActionCtx is passed to a bound action function at run time.
type ActionCtx[C any] struct {
	Entity C
	Params map[string]any
}

// ActionFn produces an effect (or error) for a transition.
type ActionFn[C any] func(ctx ActionCtx[C]) (Effect, error)

// Requirement is a declarative condition for a state, used by Assay.
type Requirement[C any] struct {
	Name      string
	Predicate func(C) bool
	Setter    func(C) // optional: mutate a zero entity to satisfy Predicate
}

// RequirementFailure records one unmet requirement.
type RequirementFailure struct {
	Name   string
	Reason string
}

// Registry holds the host behavior palette, by name.
type Registry[C any] struct {
	guards  map[string]GuardFn[C]
	actions map[string]ActionFn[C]
}

// NewRegistry returns an empty host registry.
func NewRegistry[C any]() *Registry[C] {
	return &Registry[C]{
		guards:  map[string]GuardFn[C]{},
		actions: map[string]ActionFn[C]{},
	}
}

// Guard registers a named guard implementation.
func (r *Registry[C]) Guard(name string, fn GuardFn[C]) *Registry[C] {
	r.guards[name] = fn
	return r
}

// Action registers a named action implementation.
func (r *Registry[C]) Action(name string, fn ActionFn[C]) *Registry[C] {
	r.actions[name] = fn
	return r
}

// Middleware wraps a Fire, outside-in.
type Middleware[S comparable, E comparable, C any] func(next FireFunc[S, E, C]) FireFunc[S, E, C]

// FireFunc is the inner step the middleware chain wraps.
type FireFunc[S comparable, E comparable, C any] func(ctx context.Context, event E) FireResult[S]

// Diagnostic is a non-failing finding from Temper.
type Diagnostic struct {
	Severity string
	Message  string
	SrcFile  string
	SrcLine  int
}

// stateDef is the builder's mutable record of a declared state, including the
// declarative requirements attached via Requires.
type stateDef[S comparable, E comparable, C any] struct {
	state        State[S, E, C]
	requirements []Requirement[C]
}

// Builder is the Forge DSL front-end. It builds the IR and registers
// implementations by name.
type Builder[S comparable, E comparable, C any] struct {
	name string
	reg  *Registry[C]

	states     []*stateDef[S, E, C]
	stateIndex map[S]*stateDef[S, E, C]

	initial    S
	hasInitial bool

	currentStateFn func(C) S

	middleware []Middleware[S, E, C]

	// cursor bookkeeping for the chained DSL.
	curState      *stateDef[S, E, C]
	curTransition *Transition[S, E, C] // points into curTransState.state.Transitions
	curTransState *stateDef[S, E, C]
}

// Forge opens a builder.
func Forge[S comparable, E comparable, C any](name string, opts ...ForgeOption) *Builder[S, E, C] {
	cfg := forgeConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	return &Builder[S, E, C]{
		name:       name,
		reg:        NewRegistry[C](),
		stateIndex: map[S]*stateDef[S, E, C]{},
	}
}

// Guard registers a named guard into the builder's palette.
func (b *Builder[S, E, C]) Guard(name string, fn GuardFn[C]) *Builder[S, E, C] {
	b.reg.Guard(name, fn)
	return b
}

// Action registers a named action into the builder's palette.
func (b *Builder[S, E, C]) Action(name string, fn ActionFn[C]) *Builder[S, E, C] {
	b.reg.Action(name, fn)
	return b
}

// State declares a state node.
func (b *Builder[S, E, C]) State(name S) *Builder[S, E, C] {
	sd, ok := b.stateIndex[name]
	if !ok {
		sd = &stateDef[S, E, C]{state: State[S, E, C]{Name: name}}
		b.states = append(b.states, sd)
		b.stateIndex[name] = sd
	}
	b.curState = sd
	b.curTransition = nil
	return b
}

// OwnedBy tags the most-recent state's ownership.
func (b *Builder[S, E, C]) OwnedBy(owner string) *Builder[S, E, C] {
	if b.curState != nil {
		b.curState.state.OwnedBy = owner
	}
	return b
}

// Requires attaches a requirement to the most-recent state.
func (b *Builder[S, E, C]) Requires(req Requirement[C]) *Builder[S, E, C] {
	if b.curState != nil {
		b.curState.requirements = append(b.curState.requirements, req)
	}
	return b
}

// Initial sets the entry state.
func (b *Builder[S, E, C]) Initial(name S) *Builder[S, E, C] {
	b.initial = name
	b.hasInitial = true
	return b
}

// CurrentStateFn declares how to derive an instance's current state.
func (b *Builder[S, E, C]) CurrentStateFn(fn func(C) S) *Builder[S, E, C] {
	b.currentStateFn = fn
	return b
}

// ensureState returns (creating if necessary) the stateDef for name.
func (b *Builder[S, E, C]) ensureState(name S) *stateDef[S, E, C] {
	sd, ok := b.stateIndex[name]
	if !ok {
		sd = &stateDef[S, E, C]{state: State[S, E, C]{Name: name}}
		b.states = append(b.states, sd)
		b.stateIndex[name] = sd
	}
	return sd
}

// Transition opens a new edge from the given state.
func (b *Builder[S, E, C]) Transition(from S) *Builder[S, E, C] {
	sd := b.ensureState(from)
	file, line := callerSite()
	sd.state.Transitions = append(sd.state.Transitions, Transition[S, E, C]{
		From:    from,
		SrcFile: file,
		SrcLine: line,
	})
	b.curState = sd
	b.curTransState = sd
	b.curTransition = &sd.state.Transitions[len(sd.state.Transitions)-1]
	return b
}

// On sets the triggering event of the most-recent transition.
func (b *Builder[S, E, C]) On(event E) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.On = event
	}
	return b
}

// GoTo sets the target of the most-recent transition.
func (b *Builder[S, E, C]) GoTo(to S) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.To = to
	}
	return b
}

// When attaches a named guard ref with params to the most-recent transition.
func (b *Builder[S, E, C]) When(guardName string, params ...map[string]any) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.Guards = append(b.curTransition.Guards, Ref{Name: guardName, Params: firstParams(params)})
	}
	return b
}

// Do attaches a named action ref with params to the most-recent transition.
func (b *Builder[S, E, C]) Do(actionName string, params ...map[string]any) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.Effects = append(b.curTransition.Effects, Ref{Name: actionName, Params: firstParams(params)})
	}
	return b
}

// WaitMode tags the most-recent transition's synchronization mode.
func (b *Builder[S, E, C]) WaitMode(m WaitMode) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.WaitMode = m
	}
	return b
}

// Use installs middleware that wraps every Fire.
func (b *Builder[S, E, C]) Use(mw ...Middleware[S, E, C]) *Builder[S, E, C] {
	b.middleware = append(b.middleware, mw...)
	return b
}

// firstParams returns the first params map, or nil. The DSL takes params as a
// variadic so a zero-param call reads clean.
func firstParams(params []map[string]any) map[string]any {
	if len(params) > 0 {
		return params[0]
	}
	return nil
}

// callerSite walks the stack past the kernel frames to capture the user's
// call site for diagnostics.
func callerSite() (string, int) {
	// Skip callerSite + the Builder method that invoked it; report the caller.
	if _, file, line, ok := runtime.Caller(2); ok {
		return file, line
	}
	return "", 0
}

// Temper runs a non-failing diagnostics pass over the builder's current
// definition, returning the same findings Quench would panic on — as data.
func (b *Builder[S, E, C]) Temper(opts ...TemperOption) []Diagnostic {
	cfg := temperConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	return toPublicDiagnostics(b.lint())
}

// Quench binds refs, lints, and freezes into an immutable Machine. It panics on
// any misconfiguration (programmer error) with a file:line pointer.
func (b *Builder[S, E, C]) Quench(opts ...QuenchOption) *Machine[S, E, C] {
	cfg := quenchConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	diags := b.lint()
	for _, d := range diags {
		if d.Severity == diagError || (cfg.strict && d.Severity == diagWarning) {
			// Unbound refs surface as the typed error so callers can errors.As.
			if d.unboundRef != nil {
				panic(d.unboundRef)
			}
			panic(&quenchError{Diagnostic: d})
		}
	}

	states := make([]State[S, E, C], 0, len(b.states))
	reqs := map[S][]Requirement[C]{}
	for _, sd := range b.states {
		states = append(states, sd.state)
		if len(sd.requirements) > 0 {
			reqs[sd.state.Name] = sd.requirements
		}
	}

	m := &Machine[S, E, C]{
		name:           b.name,
		states:         states,
		stateIndex:     map[S]int{},
		initial:        b.initial,
		hasInitial:     b.hasInitial,
		currentStateFn: b.currentStateFn,
		requirements:   reqs,
		guards:         map[string]GuardFn[C]{},
		actions:        map[string]ActionFn[C]{},
		middleware:     append([]Middleware[S, E, C](nil), b.middleware...),
	}
	for i := range m.states {
		m.stateIndex[m.states[i].Name] = i
	}
	for name, fn := range b.reg.guards {
		m.guards[name] = fn
	}
	for name, fn := range b.reg.actions {
		m.actions[name] = fn
	}
	return m
}

// Machine is the immutable, Quenched definition.
type Machine[S comparable, E comparable, C any] struct {
	name           string
	states         []State[S, E, C]
	stateIndex     map[S]int
	initial        S
	hasInitial     bool
	currentStateFn func(C) S
	requirements   map[S][]Requirement[C]
	guards         map[string]GuardFn[C]
	actions        map[string]ActionFn[C]
	middleware     []Middleware[S, E, C]
}

// Name returns the machine name.
func (m *Machine[S, E, C]) Name() string { return m.name }

// stateByName returns the declared State for name, and whether it exists.
func (m *Machine[S, E, C]) stateByName(name S) (*State[S, E, C], bool) {
	i, ok := m.stateIndex[name]
	if !ok {
		return nil, false
	}
	return &m.states[i], true
}

// Cast pours a fresh running instance from the machine, binding it to the
// given entity. The instance's starting state is derived from the entity via
// the machine's CurrentStateFn; if no CurrentStateFn was declared, an explicit
// initial state must be supplied via WithInitialState. When both are present,
// WithInitialState wins. With neither, Cast panics with *ErrNoInitialState — a
// programmer error, consistent with Quench's panic-on-misuse posture.
//
// The entity value is held on the Instance and supplied to guards and actions
// at Fire time; it is never threaded through context.
func (m *Machine[S, E, C]) Cast(entity C, opts ...CastOption[S]) *Instance[S, E, C] {
	cfg := castConfig[S]{}
	for _, o := range opts {
		o(&cfg)
	}

	var current S
	switch {
	case cfg.hasInitial:
		current = cfg.initial
	case m.currentStateFn != nil:
		current = m.currentStateFn(entity)
	default:
		panic(&ErrNoInitialState{Machine: m.name})
	}

	return &Instance[S, E, C]{machine: m, entity: entity, current: current}
}

// Instance binds a Machine to one entity and carries trace history.
type Instance[S comparable, E comparable, C any] struct {
	machine *Machine[S, E, C]
	entity  C
	current S
	history []Trace
	// Reserved drop-in surface: actor mailbox.
}

// Entity returns the entity this instance is bound to.
func (i *Instance[S, E, C]) Entity() C { return i.entity }

// Current returns the instance's current state.
func (i *Instance[S, E, C]) Current() S { return i.current }

// History returns the ordered traces recorded on this instance.
func (i *Instance[S, E, C]) History() []Trace {
	return append([]Trace(nil), i.history...)
}

// fmtState renders a state value for diagnostics/trace.
func fmtState[S comparable](s S) string { return fmt.Sprint(s) }

// typeName returns the Go type name of an effect for the trace.
func typeName(v any) string {
	if v == nil {
		return "<nil>"
	}
	return reflect.TypeOf(v).String()
}
