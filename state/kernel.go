package state

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"
	"time"
)

// P is a convenience alias for serializable params attached to a named Ref.
type P = map[string]any

// Ref is a named reference to a host-provided implementation plus serializable
// params. The IR carries Refs; the registry binds Name -> func at
// Provide/Quench time.
//
// Meta is the reserved extension namespace at ref granularity. It is the
// attachment point for a future polyglot binding descriptor (under the reserved
// crucible.binding key): absent any descriptor, a ref resolves to an in-process Go
// registry entry, today's behavior unchanged. The kernel never inspects Meta; it
// round-trips verbatim. extra preserves any unknown JSON keys a newer producer
// emitted so they survive a load -> save cycle (forward-compat).
type Ref struct {
	Name   string         `json:"name"`
	Params map[string]any `json:"params,omitempty"`
	Meta   map[string]any `json:"meta,omitempty"`

	extra map[string]json.RawMessage
}

// refKnownKeys is the set of JSON keys Ref models; anything else is captured into
// extra and preserved verbatim on round-trip.
var refKnownKeys = map[string]struct{}{"name": {}, "params": {}, "meta": {}}

// MarshalJSON encodes a Ref, merging its preserved unknown keys back in with
// stable key ordering.
func (r Ref) MarshalJSON() ([]byte, error) {
	type alias Ref
	return marshalWithExtra(alias(r), r.extra)
}

// UnmarshalJSON decodes a Ref and captures any unknown keys into extra so they
// survive re-serialization.
func (r *Ref) UnmarshalJSON(data []byte) error {
	type alias Ref
	var a alias
	extra, err := captureExtra(data, &a, refKnownKeys)
	if err != nil {
		return err
	}
	*r = Ref(a)
	r.extra = extra
	return nil
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

	// OnEntryAssign and OnExitAssign list the context-reducer refs folded on this
	// state's entry and exit respectively — the assign siblings of OnEntry/OnExit.
	// Exit assigns fold before transition assigns; entry assigns fold after, each
	// seeing the prior result. Both serialize and round-trip losslessly through JSON.
	OnEntryAssign []Ref `json:"onEntryAssign,omitempty"`
	OnExitAssign  []Ref `json:"onExitAssign,omitempty"`

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

	// Invoke declares the services invoked while this state is active (the
	// `invoke`). Entering the state emits a StartService effect per invocation;
	// exiting it before a service completes emits a StopService effect
	// (auto-stop-on-exit). Each invocation routes its result through OnDone and its
	// error through OnError. The whole block serializes, so it round-trips
	// losslessly through JSON. A host's ServiceRunner runs the services and re-fires
	// onDone/onError through Fire, keeping Fire pure.
	Invoke []Invocation[S, E, C] `json:"invoke,omitempty"`
	// Parent is a runtime-only back-pointer to the compound state owning this node,
	// rebuilt after Quench/Provide; it never serializes. An ActorKindMachine entry
	// in Invoke marks a child-machine actor whose lifecycle the host ActorSystem
	// drives (the actor model); the per-instance actor mailboxes live on the host
	// ActorSystem, not on this definition.
	Parent *State[S, E, C] `json:"-"`

	// Meta is the reserved extension namespace at state (node) granularity: studio
	// layout, documentation strings, tags, and codegen hints live here. The kernel
	// never inspects it; it round-trips verbatim.
	Meta map[string]any `json:"meta,omitempty"`

	// extra preserves unknown JSON keys a newer producer emitted so they survive a
	// load -> save cycle (forward-compat). Never inspected by the kernel.
	extra map[string]json.RawMessage
}

// stateKnownKeys is the set of JSON keys State models; anything else is captured
// into extra and preserved verbatim on round-trip.
var stateKnownKeys = map[string]struct{}{
	"name": {}, "ownedBy": {}, "transitions": {}, "onEntry": {}, "onExit": {},
	"isFinal": {}, "onDone": {}, "onEntryAssign": {}, "onExitAssign": {},
	"children": {}, "initialChild": {}, "regions": {},
	"historyType": {}, "historyDefault": {}, "invoke": {}, "meta": {},
}

// MarshalJSON encodes a State, merging its preserved unknown keys back in with
// stable key ordering.
func (s State[S, E, C]) MarshalJSON() ([]byte, error) {
	type alias State[S, E, C]
	return marshalWithExtra(alias(s), s.extra)
}

// UnmarshalJSON decodes a State and captures any unknown keys into extra so they
// survive re-serialization.
func (s *State[S, E, C]) UnmarshalJSON(data []byte) error {
	type alias State[S, E, C]
	var a alias
	extra, err := captureExtra(data, &a, stateKnownKeys)
	if err != nil {
		return err
	}
	*s = State[S, E, C](a)
	s.extra = extra
	return nil
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

	// Assigns lists the context-reducer refs run when this transition fires, folded
	// after the transition's effects in declaration order. Each assign sees the
	// context as folded by the assigns preceding it; the result becomes the
	// instance's context. Assigns are structurally distinct from Effects (the
	// assigner-vs-effector discriminator) so the cascade runs them in distinct
	// phases. The slice serializes and round-trips losslessly through JSON.
	Assigns []Ref `json:"assigns,omitempty"`

	// GuardExpr is an optional composite guard: a serializable boolean
	// expression tree over named-ref leaves, the stateIn built-in, and the
	// and/or/not combinators. When set it is evaluated in
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
	// wildcard fires. On is ignored when Wildcard is set. This is the
	// `on: { '*': ... }`.
	Wildcard bool `json:"wildcard,omitempty"`

	// Forbidden marks an event as explicitly blocked at this state: the event is
	// consumed and ignored, and — unlike "no handler declared" — it does NOT
	// bubble to ancestor states. To has no meaning for a forbidden transition.
	// This is a forbidden transition: the event is consumed and ignored.
	Forbidden bool `json:"forbidden,omitempty"`

	// Reenter makes a transition external. By default (v5 semantics) a transition
	// whose target is the source itself or an ancestor of the source is internal:
	// its effects run but the source is not exited and re-entered. Setting Reenter
	// forces the external form, running the full exit/entry cascade of the target.
	// For an unrelated target (an ordinary state change) the cascade always runs;
	// Reenter only changes the self/ancestor case. This is the
	// `reenter: true`.
	Reenter bool `json:"reenter,omitempty"`

	// Raise lists internal events this transition enqueues. They are appended to
	// the macrostep's internal queue after the transition's own effects run, and
	// drained by Fire's run-to-completion loop within the SAME macrostep — before
	// Fire returns and before any externally-sent event. This is the
	// `raise(...)`. The queue is local to the macrostep, so Fire stays pure.
	Raise []E `json:"raise,omitempty"`

	SrcFile string `json:"srcFile,omitempty"`
	SrcLine int    `json:"srcLine,omitempty"`

	// Meta is the reserved extension namespace at transition (edge) granularity:
	// edge layout, documentation, and codegen hints live here. The kernel never
	// inspects it; it round-trips verbatim.
	Meta map[string]any `json:"meta,omitempty"`

	// set is builder-only bookkeeping: true once On has assigned this edge's
	// event, so a following On opens a fresh transition. Never serialized.
	set bool `json:"-"`

	// extra preserves unknown JSON keys a newer producer emitted so they survive a
	// load -> save cycle (forward-compat). Never inspected by the kernel.
	extra map[string]json.RawMessage
}

// transitionKnownKeys is the set of JSON keys Transition models; anything else is
// captured into extra and preserved verbatim on round-trip.
var transitionKnownKeys = map[string]struct{}{
	"from": {}, "to": {}, "on": {}, "guards": {}, "effects": {}, "assigns": {},
	"waitMode": {}, "guardExpr": {}, "internal": {}, "eventLess": {}, "after": {},
	"wildcard": {}, "forbidden": {}, "reenter": {}, "raise": {}, "srcFile": {},
	"srcLine": {}, "meta": {},
}

// MarshalJSON encodes a Transition, merging its preserved unknown keys back in
// with stable key ordering.
func (t Transition[S, E, C]) MarshalJSON() ([]byte, error) {
	type alias Transition[S, E, C]
	return marshalWithExtra(alias(t), t.extra)
}

// UnmarshalJSON decodes a Transition and captures any unknown keys into extra so
// they survive re-serialization.
func (t *Transition[S, E, C]) UnmarshalJSON(data []byte) error {
	type alias Transition[S, E, C]
	var a alias
	extra, err := captureExtra(data, &a, transitionKnownKeys)
	if err != nil {
		return err
	}
	*t = Transition[S, E, C](a)
	t.extra = extra
	return nil
}

// Effect is an abstract, domain-defined payload. The kernel never inspects it.
type Effect = any

// Outcome classifies the result recorded in a Trace.
type Outcome int

// Outcomes recorded in a Trace, one per Fire: success or the specific failure
// class that stopped the transition. The values are a stable, ordered enumeration
// — new outcomes are appended, never reordered — so a recorded Trace stays
// comparable across versions and a consumer may switch on them safely.
const (
	// OutcomeSuccess marks a Fire that matched a transition and settled cleanly.
	OutcomeSuccess Outcome = iota
	// OutcomeInvalidTransition marks a Fire where no transition matched (current,
	// event), or every matching transition had a failing guard.
	OutcomeInvalidTransition
	// OutcomeGuardFailed marks a Fire stopped because a named guard returned false.
	OutcomeGuardFailed
	// OutcomeGuardPanic marks a Fire stopped because a guard panicked and was
	// recovered.
	OutcomeGuardPanic
	// OutcomePolicyDenied marks a Fire stopped because a policy returned Deny.
	OutcomePolicyDenied
	// OutcomeEffectError marks a Fire stopped because a bound action returned an
	// error while emitting its effect.
	OutcomeEffectError
	// OutcomeAssignFailed marks a Fire stopped because an assign reducer panicked or
	// its ref did not resolve, so the context fold could not commit.
	OutcomeAssignFailed
)

// String renders the Outcome as its stable, lower-camel discriminant
// ("success", "invalidTransition", "guardFailed", ...) for logs, the structured-
// logging seam, and tooling. An unrecognized value renders as "outcome(N)".
func (o Outcome) String() string {
	switch o {
	case OutcomeSuccess:
		return "success"
	case OutcomeInvalidTransition:
		return "invalidTransition"
	case OutcomeGuardFailed:
		return "guardFailed"
	case OutcomeGuardPanic:
		return "guardPanic"
	case OutcomePolicyDenied:
		return "policyDenied"
	case OutcomeEffectError:
		return "effectError"
	case OutcomeAssignFailed:
		return "assignFailed"
	default:
		return fmt.Sprintf("outcome(%d)", int(o))
	}
}

// Trace is the kernel's canonical observability surface — pure data recorded on
// every Fire and surfaced live on an InspectTransition event. Consumers pattern-
// match and serialize it, so its field NAMES and JSON tags are stable: fields are
// added, never renamed or repurposed, and the per-step slices are always in
// emission order (the order frozen by the determinism contract; see the package
// overview). A field that does not apply to a given Fire is left zero/empty.
//
// Rich diagnostic fields (GuardsEvaluated, EffectsEmitted, ExitedStates,
// EnteredStates, AssignsApplied, Microsteps, EventPayload, SelectedTransition)
// are populated only when the trace is in full mode — enabled by WithFullTrace,
// WithInspector, WithHistory, or WithUnboundedHistory at Cast. Lite mode (the
// default) carries Machine, Event, FromState, MatchedAt, and Outcome, which is
// sufficient for structured logging and no-overhead default operation.
type Trace struct {
	// Machine names the machine the traced instance was cast from.
	Machine string `json:"machine,omitempty"`
	// Event is the human-readable label of the event that drove this Fire — the
	// event's string rendering — kept for diagnostics, visualization, and the
	// pinned emission-ordering goldens.
	Event string `json:"event,omitempty"`
	// EventPayload is the structured, JSON-serializable form of the event value
	// that drove this Fire, recorded so a future deterministic replay can
	// reconstruct the exact event rather than re-parse its label. It is the
	// load-bearing journal companion to Event: Event stays the human label,
	// EventPayload carries the machine-readable value. It is omitted when the event
	// has no JSON form (e.g. an internal "always"/raise microstep marker), so the
	// field is additive and the trace stays deterministic across a JSON round-trip.
	// Populated only in full mode.
	EventPayload json.RawMessage `json:"eventPayload,omitempty"`
	// FromState is the primary active leaf the event was fired in, before the step.
	FromState string `json:"fromState,omitempty"`
	// SelectedTransition is the transition that fired, for in-process tooling. It is
	// not serialized (json:"-") because behavior is bound, not embedded in the IR;
	// the serializable record of what happened is the other fields.
	// Populated only in full mode.
	SelectedTransition *Transition[any, any, any] `json:"-"`
	// GuardsEvaluated names each guard the step evaluated, in evaluation order.
	// Populated only in full mode.
	GuardsEvaluated []string `json:"guardsEvaluated,omitempty"`
	// PoliciesEvaluated names each policy the step evaluated, in evaluation order.
	// Populated only in full mode.
	PoliciesEvaluated []string `json:"policiesEvaluated,omitempty"`
	// EffectsEmitted names each effect the step emitted, in emission order — the
	// human-readable companion to FireResult.Effects (the effect data itself).
	// Populated only in full mode.
	EffectsEmitted []string `json:"effectsEmitted,omitempty"`
	// AssignsApplied names each assign reducer the step folded, in fold order.
	// Populated only in full mode.
	AssignsApplied []string `json:"assignsApplied,omitempty"`
	// Microsteps records the run-to-completion interleave — each raised internal
	// event and eventless ("always") step, plus per-region markers — in the order it
	// occurred within the macrostep.
	// Populated only in full mode.
	Microsteps []string `json:"microsteps,omitempty"`

	// MatchedAt names the state whose transition actually fired. For a flat
	// machine it equals FromState; for an HSM it may be an ancestor reached by
	// the child-first bubble.
	MatchedAt string `json:"matchedAt,omitempty"`
	// ExitedStates and EnteredStates record the transition's exit/entry cascade
	// in execution order (exit innermost-first, entry outermost-first).
	// Populated only in full mode.
	ExitedStates []string `json:"exitedStates,omitempty"`
	// EnteredStates records the entry cascade in execution order (outermost-first).
	// Populated only in full mode.
	EnteredStates []string `json:"enteredStates,omitempty"`

	// Outcome classifies how the Fire settled — success or the specific failure
	// class that stopped it. It is always set (OutcomeSuccess on a clean step).
	Outcome Outcome `json:"outcome"`

	// full is the mode gate: when true, the rich diagnostic fields above are
	// populated; when false (lite mode, the default) they are skipped.
	// Unexported so it is not serialized, not carried by projectTransition, and
	// not exposed to consumers — the gate is an internal performance concern.
	full bool
}

// note appends ms to Microsteps. It is a no-op in lite mode.
func (t *Trace) note(ms string) {
	if !t.full {
		return
	}
	t.Microsteps = append(t.Microsteps, ms)
}

// recordGuard appends name to GuardsEvaluated. It is a no-op in lite mode.
func (t *Trace) recordGuard(name string) {
	if !t.full {
		return
	}
	t.GuardsEvaluated = append(t.GuardsEvaluated, name)
}

// recordEffect appends s to EffectsEmitted. It is a no-op in lite mode.
func (t *Trace) recordEffect(s string) {
	if !t.full {
		return
	}
	t.EffectsEmitted = append(t.EffectsEmitted, s)
}

// recordExit appends s to ExitedStates. It is a no-op in lite mode.
func (t *Trace) recordExit(s string) {
	if !t.full {
		return
	}
	t.ExitedStates = append(t.ExitedStates, s)
}

// recordEntry appends s to EnteredStates. It is a no-op in lite mode.
func (t *Trace) recordEntry(s string) {
	if !t.full {
		return
	}
	t.EnteredStates = append(t.EnteredStates, s)
}

// recordAssign appends name to AssignsApplied. It is a no-op in lite mode.
func (t *Trace) recordAssign(name string) {
	if !t.full {
		return
	}
	t.AssignsApplied = append(t.AssignsApplied, name)
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
	guards   map[string]GuardFn[C]
	actions  map[string]ActionFn[C]
	assigns  map[string]AssignFn[C]
	services map[string]ServiceFn[C]

	// bindings holds the per-kind binding interface recorded for every guard,
	// action, and service registration, in PARALLEL with the bare-func maps above.
	// The func maps stay the in-process fast path the pure Fire step, Quench, and
	// the palette consult unchanged; the bindings map is the seam a future
	// out-of-process binding registers under, keyed by the same name. Actors bind at
	// the host ActorSystem, not here, so they record a descriptor only.
	bindings map[string]boundBehavior[C]

	// descriptors holds the optional palette metadata attached at registration via
	// the DescribeOption tail, keyed by kind+name (descriptorKey). A registration
	// with no descriptor has no entry here and yields a minimal Descriptor from
	// Palette. actorDescs records actor-behavior names declared on the registry for
	// palette discovery; actor behaviors themselves bind at the ActorSystem, so the
	// registry carries only their descriptor metadata.
	descriptors map[string]Descriptor
	actorDescs  []string
}

// NewRegistry returns an empty host registry.
func NewRegistry[C any]() *Registry[C] {
	return &Registry[C]{
		guards:      map[string]GuardFn[C]{},
		actions:     map[string]ActionFn[C]{},
		assigns:     map[string]AssignFn[C]{},
		services:    map[string]ServiceFn[C]{},
		bindings:    map[string]boundBehavior[C]{},
		descriptors: map[string]Descriptor{},
	}
}

// describe records the palette descriptor for a registration when the option
// tail supplied one, keyed by kind+name.
func (r *Registry[C]) describe(kind DescriptorKind, name string, opts []DescribeOption) {
	if spec, ok := resolveDescribe(opts); ok {
		r.descriptors[descriptorKey(kind, name)] = descriptorFrom(kind, name, spec)
	}
}

// Guard registers a named guard implementation. An optional Describe option adds
// palette metadata (description, parameter schema, read/write hints); registering
// without one still works and yields a minimal palette descriptor.
func (r *Registry[C]) Guard(name string, fn GuardFn[C], opts ...DescribeOption) *Registry[C] {
	r.guards[name] = fn
	r.bindGuard(name, inProcessGuard(fn))
	r.describe(KindGuard, name, opts)
	return r
}

// Action registers a named action implementation. An optional Describe option
// adds palette metadata; registering without one still works and yields a minimal
// palette descriptor.
func (r *Registry[C]) Action(name string, fn ActionFn[C], opts ...DescribeOption) *Registry[C] {
	r.actions[name] = fn
	r.bindAction(name, inProcessAction(fn))
	r.describe(KindAction, name, opts)
	return r
}

// Assign registers a named assign reducer — the sole context writer. The reducer
// takes the prior context by value, the triggering event, and the ref's static
// params, and returns the next context; the kernel folds the assigns declared on
// a transition's exit/transition/entry phases to produce the instance's context.
// An optional Describe option adds palette metadata; registering without one still
// works and yields a minimal palette descriptor.
//
// Naming: the assign verb appears three times with distinct roles. Registry.Assign
// (here) and its builder alias Builder.Reducer both REGISTER a reducer impl under a
// name; Builder.Assign WIRES a registered reducer (by name) onto a transition. So
// you register once (Reducer / Registry.Assign) and wire each use (.Assign(name)).
func (r *Registry[C]) Assign(name string, fn AssignFn[C], opts ...DescribeOption) *Registry[C] {
	r.assigns[name] = fn
	r.bindAssign(name, inProcessAssign(fn))
	r.describe(KindAssign, name, opts)
	return r
}

// Service registers a named invoked-service implementation. An invoke's Src ref
// binds to it at Provide/Quench time exactly like a guard or action ref; an
// unbound service ref fails Quench with the typed *ErrUnboundRef (Kind
// "service"). The runner resolves and runs it when the owning state is entered.
// An optional Describe option adds palette metadata; registering without one
// still works and yields a minimal palette descriptor.
func (r *Registry[C]) Service(name string, fn ServiceFn[C], opts ...DescribeOption) *Registry[C] {
	r.services[name] = fn
	r.bindService(name, inProcessService(fn))
	r.describe(KindService, name, opts)
	return r
}

// Actor declares a named actor behavior in the registry's palette. Actor
// behaviors bind at the host ActorSystem (Register), not at the registry, so this
// records only the palette metadata a builder needs to enumerate and configure
// the actor — it does not register a runnable behavior. An optional Describe
// option adds description, parameter schema, and read/write hints; declaring
// without one yields a minimal palette descriptor with just Kind and Name.
func (r *Registry[C]) Actor(name string, opts ...DescribeOption) *Registry[C] {
	for _, existing := range r.actorDescs {
		if existing == name {
			r.describe(KindActor, name, opts)
			return r
		}
	}
	r.actorDescs = append(r.actorDescs, name)
	r.describe(KindActor, name, opts)
	return r
}

// Middleware wraps a Fire, outside-in.
type Middleware[S comparable, E comparable, C any] func(next FireFunc[S, E, C]) FireFunc[S, E, C]

// FireFunc is the inner step the middleware chain wraps.
type FireFunc[S comparable, E comparable, C any] func(ctx context.Context, event E) FireResult[S]

// Diagnostic is a non-failing finding from Temper — a lint/static-analysis result
// surfaced before Quench. Consumers pattern-match on it, so its field names are
// stable.
type Diagnostic struct {
	// Severity is the finding's level ("warning" | "error"); under Strict, Quench
	// rejects any finding, otherwise only "error".
	Severity string
	// Message is the human-readable description of the finding.
	Message string
	// SrcFile and SrcLine point at the builder call site that produced the finding,
	// captured via runtime.Caller. They are diagnostic-only and may be empty for a
	// finding with no single source position.
	SrcFile string
	SrcLine int
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

	// envelope carries the IR's identity/version/input-output/extension metadata
	// from Provide through Quench so a rehydrated machine re-emits it on ToJSON.
	envelope irEnvelope
}

// irEnvelope groups the additive IR envelope metadata (identity, definition
// version, opaque IO slots, and the extension namespace plus preserved unknown
// keys) so it threads as a unit from a loaded IR through Provide/Quench back out
// to ToJSON without losing forward-compat fields.
type irEnvelope struct {
	id      string
	version string
	input   *IOSpec
	output  *IOSpec
	context *ContextSchema
	meta    map[string]any
	extra   map[string]json.RawMessage
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
		envelope:   irEnvelope{version: cfg.version, id: cfg.id},
	}
}

// Guard registers a named guard into the builder's palette. An optional Describe
// option attaches palette metadata, mirroring Registry.Guard.
func (b *Builder[S, E, C]) Guard(name string, fn GuardFn[C], opts ...DescribeOption) *Builder[S, E, C] {
	b.reg.Guard(name, fn, opts...)
	return b
}

// Action registers a named action into the builder's palette. An optional
// Describe option attaches palette metadata, mirroring Registry.Action.
func (b *Builder[S, E, C]) Action(name string, fn ActionFn[C], opts ...DescribeOption) *Builder[S, E, C] {
	b.reg.Action(name, fn, opts...)
	return b
}

// Reducer registers a named assign reducer into the builder's palette — the sole
// context writer, wired onto a transition with the Assign DSL verb or onto a state
// with OnEntryAssign / OnExitAssign. It is the builder-side registration of an
// assign (the Do verb wires an Action that Action registers; the Assign verb wires
// a reducer that Reducer registers), forwarding to Registry.Assign. An optional
// Describe option attaches palette metadata.
func (b *Builder[S, E, C]) Reducer(name string, fn AssignFn[C], opts ...DescribeOption) *Builder[S, E, C] {
	b.reg.Assign(name, fn, opts...)
	return b
}

// Service registers a named invoked-service implementation into the builder's
// palette, bound by an invoke's Src ref. An unbound service ref fails Quench with
// the typed *ErrUnboundRef, mirroring guards and actions. An optional Describe
// option attaches palette metadata.
func (b *Builder[S, E, C]) Service(name string, fn ServiceFn[C], opts ...DescribeOption) *Builder[S, E, C] {
	b.reg.Service(name, fn, opts...)
	return b
}

// Actor declares a named actor behavior in the builder's palette for discovery.
// Like Registry.Actor it records palette metadata only — the runnable behavior
// binds at the host ActorSystem — so it never affects Quench binding or lint.
func (b *Builder[S, E, C]) Actor(name string, opts ...DescribeOption) *Builder[S, E, C] {
	b.reg.Actor(name, opts...)
	return b
}

// WithContextSchema attaches a serializable description of the machine's context
// data model to the IR envelope (the IR.Context slot), so a rehydrated machine
// re-emits it on ToJSON and an expression layer or studio can read the context's
// shape. Pair it with SchemaOf to derive the schema from the Go context type:
//
//	state.Forge[S, E, *Order]("checkout").
//	    WithContextSchema(state.SchemaOf[*Order]())
//
// It is opt-in and additive: deriving is never automatic at Forge, and a machine
// with no schema is valid and simply limits later type-checking. The schema is
// metadata only — the kernel never inspects it and Fire never reads it.
func (b *Builder[S, E, C]) WithContextSchema(schema ContextSchema) *Builder[S, E, C] {
	cp := cloneContextSchema(&schema)
	b.envelope.context = cp
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

// Invoke declares an invoked service on the most-recent state (an
// `invoke`). src names the service in the registry (bind it with Service); onDone
// and onError name the events the host re-fires through Fire when the service
// completes or fails, routed by ordinary transitions from this state. Configure
// the input passed to the service and an explicit id with the variadic
// InvokeOptions (WithInput, WithInvokeID); omitting WithInvokeID derives a stable
// id via InvokeID. On entering this state the kernel emits a StartService effect;
// on exiting it before the service completes, a StopService effect
// (auto-stop-on-exit). The kernel never runs the service — a host ServiceRunner
// does, keeping Fire pure.
func (b *Builder[S, E, C]) Invoke(src string, onDone, onError E, opts ...InvokeOption) *Builder[S, E, C] {
	if b.curState == nil {
		return b
	}
	cfg := invokeConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	b.curState.state.Invoke = append(b.curState.state.Invoke, Invocation[S, E, C]{
		ID:      cfg.id,
		Src:     Ref{Name: src, Params: cfg.params},
		Input:   cfg.input,
		OnDone:  onDone,
		OnError: onError,
	})
	return b
}

// InvokeActor declares a child-MACHINE actor invoked while the most-recent state
// is active (invoke of a child machine). src names the child-machine
// factory registered in the host's ActorSystem actor palette; onDone and onError
// name the events the host re-fires through the PARENT's Fire when the child
// reaches its final state (carrying its output) or fails (carrying the error),
// routed by ordinary transitions from this state. Configure the input passed to
// the child, an explicit id, and a system-scoped id with WithInput / WithInvokeID
// / WithSystemID. On entering this state the kernel emits a SpawnActor effect; on
// exiting it before the child completes, a StopActor effect (auto-stop-on-exit).
// The kernel never runs the actor — a host ActorSystem does, keeping Fire pure.
// Unlike Invoke (a host-run service), the src here is bound at the ActorSystem,
// not the registry, so it is not subject to the registry's unbound-ref lint.
func (b *Builder[S, E, C]) InvokeActor(src string, onDone, onError E, opts ...InvokeOption) *Builder[S, E, C] {
	if b.curState == nil {
		return b
	}
	cfg := invokeConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	b.curState.state.Invoke = append(b.curState.state.Invoke, Invocation[S, E, C]{
		ID:       cfg.id,
		Src:      Ref{Name: src, Params: cfg.params},
		Input:    cfg.input,
		OnDone:   onDone,
		OnError:  onError,
		Kind:     ActorKindMachine,
		SystemID: cfg.systemID,
	})
	return b
}

// Spawn attaches the kernel spawn built-in to the most-recent transition: when
// the transition fires, the kernel emits a SpawnActor effect so a machine creates
// an actor dynamically (spawn). src names the child-machine factory
// in the host's ActorSystem actor palette; id is the actor's registry key (the
// holder later stores the ActorSystem-returned ActorRef in its context to address
// it). Configure input and a system-scoped id with the SpawnOptions. The built-in
// needs no host registration, mirroring Cancel. The ActorSystem creates and runs
// the actor; routing the spawned actor's done/error is configured with
// WithSpawnOnDone / WithSpawnOnError.
func (b *Builder[S, E, C]) Spawn(src, id string, opts ...SpawnOption) *Builder[S, E, C] {
	if b.curTransition == nil {
		return b
	}
	cfg := spawnConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	params := map[string]any{spawnSrcParam: src, spawnIDParam: id}
	if cfg.input != nil {
		params[spawnInputParam] = cfg.input
	}
	if cfg.systemID != "" {
		params[spawnSystemIDParam] = cfg.systemID
	}
	if cfg.onDone != nil {
		params[spawnOnDoneParam] = cfg.onDone
	}
	if cfg.onError != nil {
		params[spawnOnErrorParam] = cfg.onError
	}
	b.curTransition.Effects = append(b.curTransition.Effects,
		Ref{Name: spawnBuiltinName, Params: params})
	return b
}

// StopActor attaches the kernel stop-actor built-in to the most-recent
// transition: when the transition fires, the kernel emits a StopActor effect for
// the given actor id, so a machine can explicitly stop a spawned actor before its
// natural completion (stopping an actor). Stopping an unknown id is a
// host-side no-op. The built-in needs no host registration, mirroring Cancel.
func (b *Builder[S, E, C]) StopActor(id string) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.Effects = append(b.curTransition.Effects,
			Ref{Name: stopActorBuiltinName, Params: map[string]any{stopActorIDParam: id}})
	}
	return b
}

// SendTo attaches the kernel sendTo built-in to the most-recent transition: when
// the transition fires, the kernel emits a SendTo effect so the host's ActorSystem
// delivers event to the actor registered under targetID. Address an actor by its
// system-scoped id instead with WithSendToSystemID. The built-in needs no host
// registration, mirroring Spawn / Cancel. This is the DSL form of
// `sendTo(target, event)`.
func (b *Builder[S, E, C]) SendTo(targetID string, event E, opts ...SendOption) *Builder[S, E, C] {
	if b.curTransition == nil {
		return b
	}
	cfg := sendConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	params := map[string]any{sendEventParam: event}
	if cfg.systemID != "" {
		params[sendToSystemIDParam] = cfg.systemID
	} else {
		params[sendToTargetParam] = targetID
	}
	b.curTransition.Effects = append(b.curTransition.Effects,
		Ref{Name: sendToBuiltinName, Params: params})
	return b
}

// SendParent attaches the kernel sendParent built-in to the most-recent
// transition: when the transition fires, the kernel emits a SendParent effect so
// the host's ActorSystem delivers event to the emitting actor's parent. Emitted by
// a top-level machine with no parent it is a host-side no-op. The built-in needs no
// host registration. This is the DSL form of sending an event to the parent.
func (b *Builder[S, E, C]) SendParent(event E) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.Effects = append(b.curTransition.Effects,
			Ref{Name: sendParentBuiltinName, Params: map[string]any{sendEventParam: event}})
	}
	return b
}

// Respond attaches the kernel respond built-in to the most-recent transition: when
// the transition fires, the kernel emits a RespondToSender effect so the host's
// ActorSystem delivers event back to the sender of the event currently being
// handled (the actor that sent it via SendTo / ForwardTo). When the current event
// has no identifiable sender it is a host-side no-op. The built-in needs no host
// registration. This is the DSL form of replying to an event's origin (the
// `respond` / `sendBack`).
func (b *Builder[S, E, C]) Respond(event E) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.Effects = append(b.curTransition.Effects,
			Ref{Name: respondBuiltinName, Params: map[string]any{sendEventParam: event}})
	}
	return b
}

// ForwardTo attaches the kernel forwardTo built-in to the most-recent transition:
// when the transition fires, the kernel emits a ForwardEvent effect so the host's
// ActorSystem forwards the event the emitting actor is currently handling, verbatim,
// to the actor registered under targetID. Address an actor by its system-scoped id
// instead with WithSendToSystemID. The built-in needs no host registration. This is
// the DSL form of forwarding the current event to another actor.
func (b *Builder[S, E, C]) ForwardTo(targetID string, opts ...SendOption) *Builder[S, E, C] {
	if b.curTransition == nil {
		return b
	}
	cfg := sendConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	params := map[string]any{}
	if cfg.systemID != "" {
		params[sendToSystemIDParam] = cfg.systemID
	} else {
		params[sendToTargetParam] = targetID
	}
	b.curTransition.Effects = append(b.curTransition.Effects,
		Ref{Name: forwardToBuiltinName, Params: params})
	return b
}

// StopChild attaches the kernel stopChild built-in to the most-recent transition:
// when the transition fires, the kernel emits a StopActor effect for the given
// actor id, so a machine can explicitly stop a spawned child actor (the
// `stopChild`). It is the action-level twin of StopActor and shares its effect;
// stopping an unknown id is a host-side no-op. The built-in needs no host
// registration.
func (b *Builder[S, E, C]) StopChild(id string) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.Effects = append(b.curTransition.Effects,
			Ref{Name: stopChildBuiltinName, Params: map[string]any{stopChildIDParam: id}})
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
// usual. This is the DSL form of a wildcard transition.
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
// having no handler (which bubbles). This is the DSL form of a
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
// DSL form of a forbidden wildcard transition.
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
// Chain GoTo/When/Do as usual. This is the DSL form of an eventless transition.
func (b *Builder[S, E, C]) Always() *Builder[S, E, C] {
	b.openTransition()
	if b.curTransition != nil {
		b.curTransition.EventLess = true
		b.curTransition.set = true
	}
	return b
}

// After opens a delayed ("after") transition from the most-recent state: a
// transition that the host's runtime fires once `delay` elapses while the source
// state stays active. Chain On(event).GoTo(target) to name the delayed event the
// host re-fires and the target it lands in (When/Do as usual). On entering the
// source state the kernel emits a ScheduleAfter effect; on exiting it before the
// delay elapses, a CancelScheduled effect (auto-cancel-on-exit). The
// kernel never sleeps — the host owns the timer and feeds the delayed event back
// through Fire. This is the DSL form of a delayed (after) transition.
func (b *Builder[S, E, C]) After(delay time.Duration) *Builder[S, E, C] {
	b.openTransition()
	if b.curTransition != nil {
		d := delay
		b.curTransition.After = &d
	}
	return b
}

// Reenter marks the most-recent transition external: a self- or ancestor-
// targeted transition that would otherwise be internal (the v5 default) instead
// runs the full exit/entry cascade of its target. This is the DSL form of the
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
// form of raising an internal event.
func (b *Builder[S, E, C]) Raise(events ...E) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.Raise = append(b.curTransition.Raise, events...)
	}
	return b
}

// Cancel attaches the kernel Cancel built-in to the most-recent transition: when
// the transition fires, the kernel emits a CancelScheduled effect for the given
// schedule id, so a machine can explicitly cancel a pending delayed (`after`)
// event before its delay elapses. The id is the ScheduleAfter ID the host
// received; ScheduleID derives it for a known source state and delayed-edge
// index. Canceling an unknown id is a host-side no-op. The built-in needs no
// host registration, mirroring the stateIn guard built-in.
func (b *Builder[S, E, C]) Cancel(id string) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.Effects = append(b.curTransition.Effects,
			Ref{Name: cancelBuiltinName, Params: map[string]any{cancelIDParam: id}})
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
// and the And/Or/Not combinators, with short-circuit semantics. It is
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

// Assign attaches a named context-reducer ref with params to the most-recent
// transition. The reducer folds onto the instance's context when the transition
// fires — the sole context-mutation site under the value-semantics contract. It is
// distinct from Do: Do emits an effect, Assign computes the next context. The
// referenced reducer is registered separately by Builder.Reducer (alias of
// Registry.Assign); this WIRES a registered reducer by name onto the transition.
func (b *Builder[S, E, C]) Assign(assignName string, params ...map[string]any) *Builder[S, E, C] {
	if b.curTransition != nil {
		b.curTransition.Assigns = append(b.curTransition.Assigns, Ref{Name: assignName, Params: firstParams(params)})
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
		assigns:        map[string]AssignFn[C]{},
		services:       map[string]ServiceFn[C]{},
		middleware:     append([]Middleware[S, E, C](nil), b.middleware...),
		envelope:       b.envelope,
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
	for name, fn := range b.reg.assigns {
		m.assigns[name] = fn
	}
	for name, fn := range b.reg.services {
		m.services[name] = fn
	}
	m.reg = b.reg
	return m
}

// Palette returns the registry's discoverable descriptor set — every registered
// guard, action, service, and declared actor behavior — sorted deterministically.
// It is the Builder-side convenience for Registry.Palette, surfacing the palette
// of a DSL-authored machine before Quench.
func (b *Builder[S, E, C]) Palette() []Descriptor { return b.reg.Palette() }

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
	assigns        map[string]AssignFn[C]
	services       map[string]ServiceFn[C]
	middleware     []Middleware[S, E, C]
	// envelope retains the IR identity/version/IO/extension metadata so ToJSON
	// re-emits it for a machine rehydrated from a versioned document.
	envelope irEnvelope
	// reg is the registry the machine was Quenched from, retained so Palette can
	// surface the discoverable descriptor set of a built machine. It is never
	// consulted by Fire.
	reg *Registry[C]
	// labels is the precomputed state-name string cache built at Quench time.
	// label() looks up declared names here (avoiding fmt.Sprint's reflection boxing
	// on every hot-path call) and falls back to fmt.Sprint for any dynamic or
	// undeclared value. It is read-only after indexHierarchy returns.
	labels map[S]string
}

// label returns the string rendering of state s, using a precomputed cache for
// declared states and falling back to fmt.Sprint for undeclared or dynamic ones.
// It is a hot-path replacement for the free fmtState function at sites that hold
// a machine pointer.
func (m *Machine[S, E, C]) label(s S) string {
	if v, ok := m.labels[s]; ok {
		return v
	}
	return fmt.Sprint(s)
}

// Palette returns the discoverable descriptor set of the machine's registry —
// every registered guard, action, service, and declared actor behavior — sorted
// deterministically. It mirrors Registry.Palette for a Quenched machine so a
// builder API can enumerate the host behavior a loaded machine binds against.
func (m *Machine[S, E, C]) Palette() []Descriptor {
	if m.reg == nil {
		return nil
	}
	return m.reg.Palette()
}

// Services returns the machine's bound invoked-service palette by name, for a
// host that constructs a ServiceRunner from the machine's own registry. The map
// is a copy; mutating it does not affect the machine.
func (m *Machine[S, E, C]) Services() map[string]ServiceFn[C] {
	out := make(map[string]ServiceFn[C], len(m.services))
	for k, v := range m.services {
		out[k] = v
	}
	return out
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

	clock := cfg.clock
	if clock == nil {
		clock = systemClock{}
	}
	// Elevation to full trace: inspector attached, or any history retention mode,
	// or an explicit WithFullTrace option. Logger-only stays lite: the logger only
	// reads Machine, Event, FromState, MatchedAt, and Outcome, all of which are
	// always present in lite mode.
	historyRetained := cfg.histLimit > 0 || cfg.histUnbounded
	traceFull := cfg.inspector != nil || historyRetained || cfg.traceFull

	inst := &Instance[S, E, C]{
		machine:       m,
		entity:        entity,
		current:       current,
		clock:         clock,
		inspector:     cfg.inspector,
		logger:        cfg.logger,
		traceFull:     traceFull,
		histLimit:     cfg.histLimit,
		histUnbounded: cfg.histUnbounded,
	}
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
	config []S

	// traceFull is the per-instance gate for rich trace fields. It is set at Cast
	// when any observing consumer is attached: inspector, history retention, or an
	// explicit WithFullTrace option. Logger-only instances stay lite.
	traceFull bool

	// history is the ring buffer of settled traces. Its shape depends on the
	// retention mode selected at Cast:
	//   - histUnbounded true: append-only slice (current unbounded behavior).
	//   - histLimit > 0: fixed-capacity ring; histHead is the write position mod limit.
	//   - neither: no retention; history is always nil.
	history       []Trace
	histLimit     int
	histUnbounded bool
	histHead      int

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
	// fireData carries the optional event payload supplied to a single Fire via
	// WithEventData: the service result, actor output, or error a host re-fires
	// with so the onDone/onError transition's Assign reads it from AssignCtx.Event
	// with no side channel. It is set at the top of the triggering Fire and consumed
	// by the first commit of that macrostep; it is macrostep-local and never
	// persisted, so Fire stays pure. hasFireData distinguishes a supplied nil
	// payload from the absent default (the boxed triggering event).
	fireData    any
	hasFireData bool
	// clock is the time seam a delayed-transition driver reads to schedule
	// `after` timers. It is never consulted by the pure Fire step — only by a
	// Scheduler/host driver wired to this instance — so Fire stays clock-free.
	// Defaults to SystemClock() when no WithClock is supplied at Cast.
	clock Clock
	// inspector is the optional live observer sink fed inspection events as the
	// instance advances (the inspection stream). It is nil by default — an
	// un-inspected instance never calls it, so inspection is zero-overhead off and
	// the pure Fire step performs no IO when one is absent. Wired with
	// WithInspector at Cast.
	inspector Inspector
	// logger is the optional structured-logging seam written to as each Fire
	// settles — the conventional *slog.Logger a host already threads through, kept
	// distinct from the event-shaped inspector. It is nil by default: an instance
	// with no WithLogger never logs, so the pure Fire step performs no IO when one
	// is absent. Wired with WithLogger at Cast.
	logger *slog.Logger
	// The actor model's per-instance mailbox lives on the host ActorSystem, which
	// runs this instance as a child actor and routes events into its mailbox; the
	// pure Fire step neither owns a mailbox nor sends messages. InFinal reports when
	// this instance (as a child actor) has reached completion.
}

// Clock returns the time seam wired to this instance at Cast (SystemClock() by
// default). A host driver reads it to schedule delayed (`after`) transitions;
// the pure Fire step never consults it.
func (i *Instance[S, E, C]) Clock() Clock { return i.clock }

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

// History returns a copy of the traces recorded on this instance, in
// chronological order (oldest first). It returns nil when no history retention
// mode was selected at Cast (the default).
//
// For a bounded ring buffer (WithHistory), the returned slice contains at most
// limit entries; if fewer than limit fires have occurred it contains all of them.
// For unbounded retention (WithUnboundedHistory) it contains every fired trace.
func (i *Instance[S, E, C]) History() []Trace {
	n := len(i.history)
	if n == 0 {
		return nil
	}
	if i.histUnbounded || i.histLimit <= 0 {
		// Append-only or no wrap: a simple copy is already in order.
		return append([]Trace(nil), i.history...)
	}
	// Ring buffer: reconstruct chronological order from histHead. histHead is the
	// next-write position, so the oldest entry is at histHead (mod n) when the
	// ring is full, or at index 0 when fewer than limit entries have been written.
	out := make([]Trace, n)
	if n < i.histLimit {
		// Buffer not yet full: entries are in order starting at index 0.
		copy(out, i.history)
		return out
	}
	// Buffer full: oldest entry is at histHead.
	for k := 0; k < n; k++ {
		out[k] = i.history[(i.histHead+k)%n]
	}
	return out
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
