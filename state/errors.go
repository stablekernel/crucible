package state

import (
	"fmt"
	"strings"
	"time"
)

// ErrInvalidTransition is returned when no transition matched (current, event),
// or all matching transitions had failing guards.
type ErrInvalidTransition struct {
	From   string
	To     string
	Event  string
	Reason string
}

func (e *ErrInvalidTransition) Error() string {
	return fmt.Sprintf("crucible/state: invalid transition from %q on %q: %s", e.From, e.Event, e.Reason)
}

// ErrGuardFailed is returned when a named guard returned false.
type ErrGuardFailed struct {
	GuardName string
	Reason    string
}

func (e *ErrGuardFailed) Error() string {
	return fmt.Sprintf("crucible/state: guard %q failed: %s", e.GuardName, e.Reason)
}

// ErrGuardPanic is returned when a guard panicked and was recovered.
type ErrGuardPanic struct {
	GuardName string
	Recovered any
}

func (e *ErrGuardPanic) Error() string {
	return fmt.Sprintf("crucible/state: guard %q panicked: %v", e.GuardName, e.Recovered)
}

// ErrPolicyDenied is returned when a policy returned Deny.
type ErrPolicyDenied struct {
	PolicyName string
	Reason     string
}

func (e *ErrPolicyDenied) Error() string {
	return fmt.Sprintf("crucible/state: policy %q denied: %s", e.PolicyName, e.Reason)
}

// ErrUndeclaredState is returned when a state value was never declared.
type ErrUndeclaredState struct {
	State string
}

func (e *ErrUndeclaredState) Error() string {
	return fmt.Sprintf("crucible/state: undeclared state %q", e.State)
}

// ErrUnboundRef is returned when a guard/action/effect ref in the IR did not
// resolve against the registry (raised at Quench / Provide).
type ErrUnboundRef struct {
	Kind string // "guard" | "action" | "effect"
	Name string
}

func (e *ErrUnboundRef) Error() string {
	return fmt.Sprintf("crucible/state: unbound %s ref %q", e.Kind, e.Name)
}

// ErrActionFailed wraps a bound action that returned an error during emission.
type ErrActionFailed struct {
	TransitionName string
	ActionName     string
	Cause          error
}

func (e *ErrActionFailed) Error() string {
	return fmt.Sprintf("crucible/state: action %q on transition %q failed: %v", e.ActionName, e.TransitionName, e.Cause)
}

func (e *ErrActionFailed) Unwrap() error { return e.Cause }

// ErrMicrostepOverflow is returned when a single Fire macrostep does not reach a
// stable configuration within the run-to-completion step budget. It indicates a
// cycle of raised internal events or eventless ("always") transitions that never
// settles.
type ErrMicrostepOverflow struct {
	Limit int
	State string
}

func (e *ErrMicrostepOverflow) Error() string {
	return fmt.Sprintf("crucible/state: run-to-completion did not stabilize within %d microsteps (at %q): a raise/eventless cycle", e.Limit, e.State)
}

// ErrNoPath is returned by PlanPath when no event sequence connects from->to.
type ErrNoPath struct {
	From string
	To   string
}

func (e *ErrNoPath) Error() string {
	return fmt.Sprintf("crucible/state: no path from %q to %q", e.From, e.To)
}

// WaitTimeoutError is returned by WaitFor when its wait budget elapses (measured
// on the instance's clock) before the predicate ever held — the typed timeout
// returned when a WaitFor budget elapses. Machine names
// the instance's machine, Timeout the budget that elapsed, and Last the primary
// active leaf the instance was in when the wait gave up, for diagnostics.
type WaitTimeoutError struct {
	Machine string
	Timeout time.Duration
	Last    string
}

func (e *WaitTimeoutError) Error() string {
	return fmt.Sprintf("crucible/state: WaitFor on machine %q timed out after %s in state %q", e.Machine, e.Timeout, e.Last)
}

// ErrNoInitialState is returned/panicked by Cast when neither a CurrentStateFn
// is declared on the machine nor an explicit initial state is supplied via
// WithInitialState — there is no way to derive the instance's starting state.
// This is a programmer error, consistent with Quench's panic-on-misuse posture.
type ErrNoInitialState struct {
	Machine string
}

func (e *ErrNoInitialState) Error() string {
	return fmt.Sprintf("crucible/state: cannot Cast machine %q: no CurrentStateFn declared and no WithInitialState supplied", e.Machine)
}

// ErrUnknownBuiltin is returned when a ref names a kernel built-in action the
// kernel does not recognize. It is a defensive programmer-error signal: the DSL
// and lint only ever produce known built-in names, so this surfaces only a
// hand-constructed or corrupted ref.
type ErrUnknownBuiltin struct {
	Name string
}

func (e *ErrUnknownBuiltin) Error() string {
	return fmt.Sprintf("crucible/state: unknown built-in action %q", e.Name)
}

// ErrUnboundActor is returned by an ActorSystem when a SpawnActor's Src does not
// resolve against the system's actor palette — no child-machine factory was
// registered under that name. The actor is settled as an error so the parent
// still routes its onError rather than hanging.
type ErrUnboundActor struct {
	Name string
}

func (e *ErrUnboundActor) Error() string {
	return fmt.Sprintf("crucible/state: unbound actor ref %q", e.Name)
}

// SnapshotError is returned by Restore / MarshalSnapshot / UnmarshalSnapshot when
// an instance snapshot cannot be captured, serialized, or restored: a snapshot
// whose Machine does not match the target, a configuration leaf that is not a
// declared state, an empty configuration with an unknown current state, or a
// context encode/decode failure. Op names the failing operation
// ("restore" | "marshal" | "unmarshal"), State (when set) names the offending
// configuration leaf, and Reason carries the detail.
type SnapshotError struct {
	Op     string
	State  string
	Reason string
}

func (e *SnapshotError) Error() string {
	if e.State != "" {
		return fmt.Sprintf("crucible/state: snapshot %s failed at %q: %s", e.Op, e.State, e.Reason)
	}
	return fmt.Sprintf("crucible/state: snapshot %s failed: %s", e.Op, e.Reason)
}

// MultiRegionErr aggregates the errors raised by more than one orthogonal
// region firing on a single event. Its Unwrap returns each region's error so
// errors.As finds any region's typed error.
type MultiRegionErr struct {
	Errors []error
}

func (e *MultiRegionErr) Error() string {
	msgs := make([]string, 0, len(e.Errors))
	for _, err := range e.Errors {
		msgs = append(msgs, err.Error())
	}
	return fmt.Sprintf("crucible/state: %d regions errored: %s", len(e.Errors), strings.Join(msgs, "; "))
}

// Unwrap exposes the per-region errors for errors.As / errors.Is traversal.
func (e *MultiRegionErr) Unwrap() []error { return e.Errors }

// AssayError aggregates one or more failing requirements found by Assay.
type AssayError struct {
	Failures []RequirementFailure
}

func (e *AssayError) Error() string {
	names := make([]string, 0, len(e.Failures))
	for _, f := range e.Failures {
		names = append(names, f.Name)
	}
	return fmt.Sprintf("crucible/state: assay failed: %s", strings.Join(names, ", "))
}

// ErrUnsupportedSchema is returned by LoadFromJSON when an IR document declares a
// schema major version newer than the loader supports. The reject-higher-major
// policy is the reserved compatibility seam: a higher minor (same major) loads,
// preserving unknown fields for forward-compat, but a higher major signals a wire
// form this build cannot safely interpret and is refused rather than guessed at.
type ErrUnsupportedSchema struct {
	// Got is the schemaVersion declared in the document.
	Got string
	// Supported is the loader's own schema version.
	Supported string
}

func (e *ErrUnsupportedSchema) Error() string {
	return fmt.Sprintf("crucible/state: unsupported schema version %q (loader supports %q)", e.Got, e.Supported)
}

// ErrUnknownEffectKind is returned by EffectRegistry.Dispatchable when an effect
// carries a kind the registry does not recognize. It realizes the reject half of
// the closed-enum extension policy for effect kinds: an unknown kind is preserved
// on load (as an UnknownEffect) so a foreign effect round-trips losslessly, but
// it is refused at dispatch rather than silently applied — the host must register
// the kind (RegisterEffect) or drop the effect deliberately.
type ErrUnknownEffectKind struct {
	// Kind is the unrecognized effect discriminant.
	Kind string
}

func (e *ErrUnknownEffectKind) Error() string {
	return fmt.Sprintf("crucible/state: unknown effect kind %q (not registered for dispatch)", e.Kind)
}
