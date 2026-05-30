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
//
// A state is one of three shapes: a leaf (no Children, no Regions), a compound
// (hierarchical) state declaring Children plus an InitialChild, or a parallel
// state declaring Regions. A state is never both compound and parallel.
type State[S comparable, E comparable, C any] struct {
	Name        S                     `json:"name"`
	OwnedBy     string                `json:"ownedBy,omitempty"`
	Transitions []Transition[S, E, C] `json:"transitions,omitempty"`

	OnEntry []Ref `json:"onEntry,omitempty"`
	OnExit  []Ref `json:"onExit,omitempty"`
	IsFinal bool  `json:"isFinal,omitempty"`
	OnDone  []Ref `json:"onDone,omitempty"`

	// Hierarchy. Children holds the nested substates of a compound state, and
	// InitialChild names the substate entered transitively when the compound
	// state is entered. Both serialize, so the hierarchy round-trips through
	// JSON. Parent is a runtime-only back-pointer rebuilt after Quench/Provide.
	Children     []State[S, E, C] `json:"children,omitempty"`
	InitialChild *S               `json:"initialChild,omitempty"`

	// Regions holds the orthogonal regions of a parallel state. Mutually
	// exclusive with Children/InitialChild.
	Regions []Region[S, E, C] `json:"regions,omitempty"`

	// History. HistoryType marks this node as a history pseudo-state (shallow or
	// deep) belonging to its parent compound; HistoryNone (the default) is an
	// ordinary state. HistoryDefault names the target entered when the owning
	// compound has no recorded history yet; nil falls back to the compound's
	// InitialChild. Both serialize, so history pseudo-states round-trip through
	// JSON; the per-instance recorded configuration is runtime state, not IR.
	HistoryType    HistoryType `json:"historyType,omitempty"`
	HistoryDefault *S          `json:"historyDefault,omitempty"`

	// Reserved drop-in surface.
	Invoke []Ref           `json:"invoke,omitempty"`
	Parent *State[S, E, C] `json:"-"`
}

// Region is one orthogonal region of a parallel state: a self-contained set of
// substates with its own initial child. When the owning parallel state is
// active, every region is active simultaneously, each tracking its own leaf.
type Region[S comparable, E comparable, C any] struct {
	Name         string           `json:"name"`
	States       []State[S, E, C] `json:"states,omitempty"`
	InitialChild *S               `json:"initialChild,omitempty"`
}

// Transition is a directed edge.
type Transition[S comparable, E comparable, C any] struct {
	From S `json:"from"`
	To   S `json:"to"`
	On   E `json:"on"`

	Guards   []Ref    `json:"guards,omitempty"`
	Effects  []Ref    `json:"effects,omitempty"`
	WaitMode WaitMode `json:"waitMode,omitempty"`

	// GuardExpr is an optional composite guard: a serializable boolean
	// expression tree over named-ref leaves, the stateIn built-in, and the
	// and/or/not combinators (xstate v5 parity). When set it is evaluated in
	// addition to every Ref in Guards — the transition is enabled only when both
	// the plain guards and the expression pass — so the common single-guard case
	// stays the plain Guards slice and composition is purely additive. The tree
	// serializes and round-trips losslessly through JSON.
	GuardExpr *GuardNode[S] `json:"guardExpr,omitempty"`

	Internal  bool           `json:"internal,omitempty"`
	EventLess bool           `json:"eventLess,omitempty"`
	After     *time.Duration `json:"after,omitempty"`

	// Wildcard marks a catch-all transition: it matches any event that no
	// specific-event transition of the same state handles. Wildcard transitions
	// are the lowest-priority candidates in a state, tried only after every
	// On-keyed match fails, and resolution still bubbles to ancestors when no
	// wildcard fires. On is ignored when Wildcard is set. This mirrors xstate v5
	// `on: { '*': ... }`.
	Wildcard bool `json:"wildcard,omitempty"`

	// Forbidden marks an event as explicitly blocked at this state: the event is
	// consumed and ignored, and — unlike "no handler declared" — it does NOT
	// bubble to ancestor states. To has no meaning for a forbidden transition.
	// This mirrors xstate v5 `on: { E: undefined }`.
	Forbidden bool `json:"forbidden,omitempty"`

	// Reenter makes a transition external. By default (v5 semantics) a transition
	// whose target is the source itself or an ancestor of the source is internal:
	// its effects run but the source is not exited and re-entered. Setting Reenter
	// forces the external form, running the full exit/entry cascade of the target.
	// For an unrelated target (an ordinary state change) the cascade always runs;
	// Reenter only changes the self/ancestor case. This mirrors xstate v5
	// `reenter: true`.
	Reenter bool `json:"reenter,omitempty"`

	// Raise lists internal events this transition enqueues. They are appended to
	// the macrostep's internal queue after the transition's own effects run, and
	// drained by Fire's run-to-completion loop within the SAME macrostep — before
	// Fire returns and before any externally-sent event. This mirrors xstate v5
	// `raise(...)`. The queue is local to the macrostep, so Fire stays pure.
	Raise []E `json:"raise,omitempty"`

	SrcFile string `json:"srcFile,omitempty"`
	SrcLine int    `json:"srcLine,omitempty"`

	// set is builder-only bookkeeping: true once On has assigned this edge's
	// event, so a following On opens a fresh transition. Never serialized.
	set bool `json:"-"`
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

	// MatchedAt names the state whose transition actually fired. For a flat
	// machine it equals FromState; for an HSM it may be an ancestor reached by
	// the child-first bubble.
	MatchedAt string `json:"matchedAt,omitempty"`
	// ExitedStates and EnteredStates record the transition's exit/entry cascade
	// in execution order (exit innermost-first, entry outermost-first).
	ExitedStates  []string `json:"exitedStates,omitempty"`
	EnteredStates []string `json:"enteredStates,omitempty"`

	Outcome Outcome `json:"outcome"`
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
// declarative requirements attached via Requires and its place in the hierarchy.
type stateDef[S comparable, E comparable, C any] struct {
	state        State[S, E, C]
	requirements []Requirement[C]

	// Hierarchy placement, set when the state is declared inside a SuperState or
	// Region block. topLevel states have hasParent false.
	parent    S
	hasParent bool
	region    string // region name when nested directly in a Region block
	order     int    // declaration order among siblings
	isHistory bool   // true for a history pseudo-state (not a real substate/leaf)

	// childDefs holds this state's direct child stateDefs in declaration order,
	// populated by assembleHierarchy so the nested State tree can be built
	// depth-first to arbitrary depth.
	childDefs []*stateDef[S, E, C]
}

// blockKind tags an open builder block.
type blockKind int

const (
	blockSuper blockKind = iota
	blockRegion
)

// block is one open SuperState/Region context on the builder's stack.
type block[S comparable, E comparable, C any] struct {
	kind       blockKind
	owner      *stateDef[S, E, C] // the superstate this block belongs to
	region     string             // region name, for blockRegion
	initial    S
	hasInitial bool
	childCount int
	srcFile    string
	srcLine    int
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

	// HSM block stack and a monotonic sibling-order counter.
	blocks   []*block[S, E, C]
	orderSeq int
	// prebuilt is set when the builder's states already carry their nested
	// structure (e.g. from Provide), so Quench skips hierarchy re-assembly.
	prebuilt bool
	// hsmDiags carries HSM-specific lint findings recorded during the chained
	// build (e.g. SubState outside a SuperState), surfaced at Temper/Quench.
	hsmDiags []diagnostic
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

// State declares a state node. Inside a SuperState or Region block it declares a
// substate of that block (equivalent to SubState); at the top level it declares
// a top-level state.
func (b *Builder[S, E, C]) State(name S) *Builder[S, E, C] {
	return b.declareState(name)
}

// declareState creates or selects the stateDef for name, placing it under the
// current open block (if any) and assigning sibling order.
func (b *Builder[S, E, C]) declareState(name S) *Builder[S, E, C] {
	sd, ok := b.stateIndex[name]
	if !ok {
		sd = &stateDef[S, E, C]{state: State[S, E, C]{Name: name}, order: b.orderSeq}
		b.orderSeq++
		b.placeState(sd)
		b.states = append(b.states, sd)
		b.stateIndex[name] = sd
	}
	b.curState = sd
	b.curTransition = nil
	return b
}

// placeState records the hierarchy placement of a freshly-declared state based
// on the current open block.
func (b *Builder[S, E, C]) placeState(sd *stateDef[S, E, C]) {
	if len(b.blocks) == 0 {
		return
	}
	top := b.blocks[len(b.blocks)-1]
	sd.parent = top.owner.state.Name
	sd.hasParent = true
	if top.kind == blockRegion {
		sd.region = top.region
	}
	top.childCount++
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

// Initial sets the entry state. At the top level it sets the machine's initial
// state; inside a SuperState or Region block it sets that block's initial child.
func (b *Builder[S, E, C]) Initial(name S) *Builder[S, E, C] {
	if len(b.blocks) > 0 {
		top := b.blocks[len(b.blocks)-1]
		top.initial = name
		top.hasInitial = true
		return b
	}
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
		sd = &stateDef[S, E, C]{state: State[S, E, C]{Name: name}, order: b.orderSeq}
		b.orderSeq++
		b.placeState(sd)
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

// On sets the triggering event of the most-recent transition. When no
// transition is currently open — or the open one already has its event set (a
// completed `.On(...).GoTo(...)` clause) — On opens a fresh transition from the
// most-recent state. This lets the hierarchical DSL read
// `.SubState(X).On(e1).GoTo(Y).On(e2).GoTo(Z)` and `.SubState(X).On(e).GoTo(Y)`
// without an explicit Transition call.
func (b *Builder[S, E, C]) On(event E) *Builder[S, E, C] {
	needNew := b.curState != nil && (b.curTransition == nil ||
		b.curTransState != b.curState || b.curTransition.set)
	if needNew {
		sd := b.curState
		file, line := callerSite()
		sd.state.Transitions = append(sd.state.Transitions, Transition[S, E, C]{
			From:    sd.state.Name,
			SrcFile: file,
			SrcLine: line,
		})
		b.curTransState = sd
		b.curTransition = &sd.state.Transitions[len(sd.state.Transitions)-1]
	}
	if b.curTransition != nil {
		b.curTransition.On = event
		b.curTransition.set = true
	}
	return b
}

// OnAny opens a wildcard (catch-all) transition from the most-recent state. It
// matches any event no specific On-keyed transition of the state handles, and is
// the lowest-priority candidate — tried only after every specific match fails,
// before the event bubbles to an ancestor. Chain GoTo/When/Do/Reenter/Raise as
// usual. This is the DSL form of xstate v5 `on: { '*': ... }`.
func (b *Builder[S, E, C]) OnAny() *Builder[S, E, C] {
	b.openTransition()
	if b.curTransition != nil {
		b.curTransition.Wildcard = true
		b.curTransition.set = true
	}
	return b
}

// Forbid declares that the most-recent state blocks the given event: the event
// is consumed and ignored there and does NOT bubble to ancestors, distinct from
// having no handler (which bubbles). This is the DSL form of xstate v5
// `on: { E: undefined }`. A forbidden transition takes no target, guards, or
// effects.
func (b *Builder[S, E, C]) Forbid(event E) *Builder[S, E, C] {
	b.openTransition()
	if b.curTransition != nil {
		b.curTransition.On = event
		b.curTransition.Forbidden = true
		b.curTransition.set = true
	}
	return b
}

// ForbidAny declares a forbidden wildcard: every event not otherwise handled is
// consumed and ignored at the most-recent state instead of bubbling. This is the
// DSL form of xstate v5 `on: { '*': undefined }`.
func (b *Builder[S, E, C]) ForbidAny() *Builder[S, E, C] {
	b.openTransition()
	if b.curTransition != nil {
		b.curTransition.Wildcard = true
		b.curTransition.Forbidden = true
		b.curTransition.set = true
	}
	return b
}

// openTransition selects the transition the next OnAny/Forbid/Always/ForbidAny
// will configure, mirroring On: it reuses a freshly-opened transition from a
// preceding Transition(from) call, and otherwise opens a new edge from the
// most-recent state. This lets `Transition(x).OnAny().GoTo(y)` and
// `.State(x).OnAny().GoTo(y)` both read cleanly.
func (b *Builder[S, E, C]) openTransition() {
	if b.curState == nil {
		return
	}
	needNew := b.curTransition == nil || b.curTransState != b.curState || b.curTransition.set
	if !needNew {
		return
	}
	sd := b.curState
	file, line := callerSite2()
	sd.state.Transitions = append(sd.state.Transitions, Transition[S, E, C]{
		From:    sd.state.Name,
		SrcFile: file,
		SrcLine: line,
	})
	b.curTransState = sd
	b.curTransition = &sd.state.Transitions[len(sd.state.Transitions)-1]
}

// GoTo sets the target of the most-recent transition.
func (b *Builder[S, E, C]) GoTo(to S) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.To = to
	}
	return b
}

// Always opens an eventless ("always") transition from the most-recent state. It
// carries no triggering event and is auto-fired by the run-to-completion loop
// whenever its guards pass and the state is active, within the firing macrostep.
// Chain GoTo/When/Do as usual. This is the DSL form of xstate v5 `always`.
func (b *Builder[S, E, C]) Always() *Builder[S, E, C] {
	b.openTransition()
	if b.curTransition != nil {
		b.curTransition.EventLess = true
		b.curTransition.set = true
	}
	return b
}

// Reenter marks the most-recent transition external: a self- or ancestor-
// targeted transition that would otherwise be internal (the v5 default) instead
// runs the full exit/entry cascade of its target. This is the DSL form of xstate
// v5 `reenter: true`.
func (b *Builder[S, E, C]) Reenter() *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.Reenter = true
	}
	return b
}

// Raise attaches internal events to the most-recent transition. After the
// transition's effects run, each raised event is processed within the same Fire
// macrostep by the run-to-completion loop, before Fire returns. This is the DSL
// form of xstate v5 `raise(...)`.
func (b *Builder[S, E, C]) Raise(events ...E) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.Raise = append(b.curTransition.Raise, events...)
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

// WhenExpr attaches a composite guard expression to the most-recent transition:
// a boolean tree over named-ref leaves (Guard), the stateIn built-in (StateIn),
// and the And/Or/Not combinators, with xstate v5 short-circuit semantics. It is
// evaluated alongside any When guards — the transition is enabled only when both
// pass. Use When for the common single-guard case and WhenExpr when a transition
// needs composition or stateIn.
func (b *Builder[S, E, C]) WhenExpr(expr GuardNode[S]) *Builder[S, E, C] {
	if b.curTransition != nil {
		e := expr
		b.curTransition.GuardExpr = &e
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

// callerSite2 is callerSite for one extra intermediate frame (a Builder method
// that calls a recordHSMDiag helper).
func callerSite2() (string, int) {
	if _, file, line, ok := runtime.Caller(3); ok {
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

	states := b.assembleHierarchy()
	reqs := map[S][]Requirement[C]{}
	for _, sd := range b.states {
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
	m.indexHierarchy()
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
	nodes          map[S]*node[S, E, C]
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

	inst := &Instance[S, E, C]{machine: m, entity: entity, current: current}
	// If the starting state is itself compound or parallel, the active
	// configuration is the set of leaves reached by descending into its initial
	// children. The primary leaf becomes Current().
	leaves := m.descendToLeaves(current)
	if len(leaves) > 0 {
		inst.config = leaves
		inst.current = leaves[0]
	} else {
		inst.config = []S{current}
	}
	return inst
}

// Instance binds a Machine to one entity and carries trace history.
type Instance[S comparable, E comparable, C any] struct {
	machine *Machine[S, E, C]
	entity  C
	current S
	// config holds all currently-active leaves. For a flat or single-spine
	// machine len(config)==1 and config[0]==current; for an active parallel
	// state it holds one leaf per region, in declaration order.
	config  []S
	history []Trace
	// historyShallow and historyDeep record per-compound history for history
	// pseudo-states: historyShallow maps a compound to its last active direct
	// child, historyDeep maps a compound to its last active leaf configuration.
	// Both are empty after Cast and are written on compound exit / read on
	// history-targeted entry. They are part of the instance's runtime state,
	// threaded through Fire — never global or IO-backed — so Fire stays pure.
	historyShallow map[S]S
	historyDeep    map[S][]S
	// raised is the macrostep-local internal-event queue fed by Raise actions and
	// drained by Fire's run-to-completion loop. It is never persisted and is empty
	// between macrosteps, so Fire stays pure.
	raised []E
	// Reserved drop-in surface: actor mailbox.
}

// Entity returns the entity this instance is bound to.
func (i *Instance[S, E, C]) Entity() C { return i.entity }

// Current returns the primary (first) active leaf — the common "what state am I
// really in?" answer, back-compatible with flat machines.
func (i *Instance[S, E, C]) Current() S { return i.current }

// Configuration returns all currently-active leaves, in declaration order.
// len == 1 for a flat or single-spine machine; len == N when N regions are
// active in parallel.
func (i *Instance[S, E, C]) Configuration() []S {
	if len(i.config) == 0 {
		return []S{i.current}
	}
	return append([]S(nil), i.config...)
}

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
