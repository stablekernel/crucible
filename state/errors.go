package state

import (
	"fmt"
	"strings"
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

// ErrNoPath is returned by PlanPath when no event sequence connects from->to.
type ErrNoPath struct {
	From string
	To   string
}

func (e *ErrNoPath) Error() string {
	return fmt.Sprintf("crucible/state: no path from %q to %q", e.From, e.To)
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
