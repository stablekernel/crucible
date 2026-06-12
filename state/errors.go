package state

import (
	"fmt"
	"strings"
	"time"
)

// InvalidTransitionError is returned when no transition matched (current, event),
// or all matching transitions had failing guards. From names the state the event
// was fired in, Event the rejected event, and Reason the specific cause (no
// declared transition, a final-state exit, an undeclared current state, ...). To
// names the intended target when the rejected transition had one (a targeted
// transition whose guards all failed); it is empty for an unmatched event with no
// candidate target.
type InvalidTransitionError struct {
	From   string
	To     string
	Event  string
	Reason string
}

func (e *InvalidTransitionError) Error() string {
	if e.To != "" {
		return fmt.Sprintf("crucible/state: invalid transition from %q to %q on %q: %s", e.From, e.To, e.Event, e.Reason)
	}
	return fmt.Sprintf("crucible/state: invalid transition from %q on %q: %s", e.From, e.Event, e.Reason)
}

// GuardFailedError is returned when a named guard returned false.
type GuardFailedError struct {
	GuardName string
	Reason    string
}

func (e *GuardFailedError) Error() string {
	return fmt.Sprintf("crucible/state: guard %q failed: %s", e.GuardName, e.Reason)
}

// GuardPanicError is returned when a guard panicked and was recovered.
type GuardPanicError struct {
	GuardName string
	Recovered any
}

func (e *GuardPanicError) Error() string {
	return fmt.Sprintf("crucible/state: guard %q panicked: %v", e.GuardName, e.Recovered)
}

// Unwrap exposes the recovered value when it is an error, so errors.Is / errors.As
// can traverse to the inner cause; it returns nil for non-error panic values.
func (e *GuardPanicError) Unwrap() error {
	if err, ok := e.Recovered.(error); ok {
		return err
	}
	return nil
}

// AssignPanicError is returned when an assign reducer panicked and was recovered,
// or when an assign ref did not resolve at fire time. An assign is a total reducer,
// so a panic is a programmer error the kernel surfaces as a typed failure that
// stops the commit rather than leaving context partly folded.
type AssignPanicError struct {
	AssignName string
	Recovered  any
}

func (e *AssignPanicError) Error() string {
	return fmt.Sprintf("crucible/state: assign %q panicked: %v", e.AssignName, e.Recovered)
}

// Unwrap exposes the recovered value when it is an error, so errors.Is / errors.As
// can traverse to the inner cause; it returns nil for non-error panic values.
func (e *AssignPanicError) Unwrap() error {
	if err, ok := e.Recovered.(error); ok {
		return err
	}
	return nil
}

// ActionPanicError is returned when a host action (an OnEntry/OnExit action or a
// transition action) panicked and was recovered. An action is host code the
// kernel runs during a fire; a panic is a programmer error the kernel surfaces as
// a typed failure on FireResult.Err rather than letting it crash Fire. When the
// recovered value is itself an error, Unwrap exposes it so errors.Is / errors.As
// can reach the inner cause.
type ActionPanicError struct {
	ActionName string
	Recovered  any
}

func (e *ActionPanicError) Error() string {
	return fmt.Sprintf("crucible/state: action %q panicked: %v", e.ActionName, e.Recovered)
}

// Unwrap exposes the recovered value when it is an error, so errors.Is / errors.As
// can traverse to the inner cause; it returns nil for non-error panic values.
func (e *ActionPanicError) Unwrap() error {
	if err, ok := e.Recovered.(error); ok {
		return err
	}
	return nil
}

// PolicyDeniedError is returned when a policy returned Deny.
type PolicyDeniedError struct {
	PolicyName string
	Reason     string
}

func (e *PolicyDeniedError) Error() string {
	return fmt.Sprintf("crucible/state: policy %q denied: %s", e.PolicyName, e.Reason)
}

// UndeclaredStateError is returned when a state value was never declared.
type UndeclaredStateError struct {
	State string
}

func (e *UndeclaredStateError) Error() string {
	return fmt.Sprintf("crucible/state: undeclared state %q", e.State)
}

// UnboundRefError is returned when a guard/action/effect ref in the IR did not
// resolve against the registry (raised at Quench / Provide).
type UnboundRefError struct {
	Kind string // "guard" | "action" | "assign" | "service"
	Name string
}

func (e *UnboundRefError) Error() string {
	return fmt.Sprintf("crucible/state: unbound %s ref %q", e.Kind, e.Name)
}

// ActionFailedError wraps a bound action that returned an error during emission.
type ActionFailedError struct {
	TransitionName string
	ActionName     string
	Cause          error
}

func (e *ActionFailedError) Error() string {
	return fmt.Sprintf("crucible/state: action %q on transition %q failed: %v", e.ActionName, e.TransitionName, e.Cause)
}

func (e *ActionFailedError) Unwrap() error { return e.Cause }

// MicrostepOverflowError is returned when a single Fire macrostep does not reach a
// stable configuration within the run-to-completion step budget. It indicates a
// cycle of raised internal events or eventless ("always") transitions that never
// settles.
type MicrostepOverflowError struct {
	Limit int
	State string
}

func (e *MicrostepOverflowError) Error() string {
	return fmt.Sprintf("crucible/state: run-to-completion did not stabilize within %d microsteps (at %q): a raise/eventless cycle", e.Limit, e.State)
}

// NoPathError is returned by PlanPath when no event sequence connects from->to.
type NoPathError struct {
	From string
	To   string
}

func (e *NoPathError) Error() string {
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

// NoInitialStateError is returned/panicked by Cast when neither a CurrentStateFn
// is declared on the machine nor an explicit initial state is supplied via
// WithInitialState — there is no way to derive the instance's starting state.
// This is a programmer error, consistent with Quench's panic-on-misuse posture.
type NoInitialStateError struct {
	Machine string
}

func (e *NoInitialStateError) Error() string {
	return fmt.Sprintf("crucible/state: cannot Cast machine %q: no CurrentStateFn declared and no WithInitialState supplied", e.Machine)
}

// UnknownBuiltinError is returned when a ref names a kernel built-in action the
// kernel does not recognize. It is a defensive programmer-error signal: the DSL
// and lint only ever produce known built-in names, so this surfaces only a
// hand-constructed or corrupted ref.
type UnknownBuiltinError struct {
	Name string
}

func (e *UnknownBuiltinError) Error() string {
	return fmt.Sprintf("crucible/state: unknown built-in action %q", e.Name)
}

// UnboundActorError is returned by an ActorSystem when a SpawnActor's Src does not
// resolve against the system's actor palette — no child-machine factory was
// registered under that name. The actor is settled as an error so the parent
// still routes its onError rather than hanging.
type UnboundActorError struct {
	Name string
}

func (e *UnboundActorError) Error() string {
	return fmt.Sprintf("crucible/state: unbound actor ref %q", e.Name)
}

// SnapshotError is returned by Restore / MarshalSnapshot / UnmarshalSnapshot when
// an instance snapshot cannot be captured, serialized, or restored: a snapshot
// whose Machine does not match the target, a configuration leaf that is not a
// declared state, an empty configuration with an unknown current state, or a
// context encode/decode failure. Op names the failing operation
// ("restore" | "marshal" | "unmarshal"), State (when set) names the offending
// configuration leaf, and Reason carries the detail.
//
// Cause is the wrapped underlying error when the failure originated in one (a
// JSON encode/decode error, for example), exposed via Unwrap so errors.Is /
// errors.As can reach it. Reason stays the human-readable message for backward
// compatibility — when a cause is present, Reason carries its text — so existing
// callers that read Reason are unaffected.
type SnapshotError struct {
	Op     string
	State  string
	Reason string
	Cause  error
}

func (e *SnapshotError) Error() string {
	if e.State != "" {
		return fmt.Sprintf("crucible/state: snapshot %s failed at %q: %s", e.Op, e.State, e.Reason)
	}
	return fmt.Sprintf("crucible/state: snapshot %s failed: %s", e.Op, e.Reason)
}

// Unwrap exposes the wrapped cause for errors.Is / errors.As traversal; it
// returns nil when the snapshot failure had no underlying error.
func (e *SnapshotError) Unwrap() error { return e.Cause }

// SnapshotVersionError is returned by Restore when a snapshot's version identity
// is incompatible with the target: a snapshot-format schema version across a major
// boundary (always rejected, under the lenient restore-version posture), or — only
// when RejectMachineVersionMismatch is set — a machine definition version that does
// not match the target machine. Kind discriminates the two ("snapshotFormat" |
// "machineVersion"); Machine names the target; Got and Want carry the offending and
// expected versions; Reason carries the detail. It is the typed signal a migrator
// or host keys version-mismatch handling on.
type SnapshotVersionError struct {
	Kind    string
	Machine string
	Got     string
	Want    string
	Reason  string
}

func (e *SnapshotVersionError) Error() string {
	return fmt.Sprintf("crucible/state: snapshot %s version mismatch for machine %q: got %q, want %q: %s",
		e.Kind, e.Machine, e.Got, e.Want, e.Reason)
}

// MultiRegionError aggregates the errors raised by more than one orthogonal
// region firing on a single event. Its Unwrap returns each region's error so
// errors.As finds any region's typed error.
type MultiRegionError struct {
	Errors []error
}

func (e *MultiRegionError) Error() string {
	msgs := make([]string, 0, len(e.Errors))
	for _, err := range e.Errors {
		msgs = append(msgs, err.Error())
	}
	return fmt.Sprintf("crucible/state: %d regions errored: %s", len(e.Errors), strings.Join(msgs, "; "))
}

// Unwrap exposes the per-region errors for errors.As / errors.Is traversal.
func (e *MultiRegionError) Unwrap() []error { return e.Errors }

// RegionEscapeError reports a region-internal transition whose target lies
// outside the owning parallel region. The construct is ill-defined: SCXML
// semantics would exit the whole parallel state, which the region-scoped builder
// API does not express, so it is rejected at Quench rather than corrupting the
// configuration at runtime. Region names the owning region, From the source
// state, and To the escaping target.
type RegionEscapeError struct {
	Region string
	From   string
	To     string
}

func (e *RegionEscapeError) Error() string {
	return fmt.Sprintf("crucible/state: region %q transition from %q targets %q outside the region",
		e.Region, e.From, e.To)
}

// HistoryCrossRegionError reports a region-internal transition targeting a
// history pseudo-state owned by a different region (or by a state outside the
// owning parallel). A history target is only meaningful within its own region's
// scope; a cross-region history target is ambiguous and rejected at Quench.
// Region names the transition's owning region, From the source state, and
// History the targeted history pseudo-state.
type HistoryCrossRegionError struct {
	Region  string
	From    string
	History string
}

func (e *HistoryCrossRegionError) Error() string {
	return fmt.Sprintf("crucible/state: region %q transition from %q targets history state %q in another region",
		e.Region, e.From, e.History)
}

// VerifyError aggregates one or more failing requirements found by Verify.
type VerifyError struct {
	Failures []RequirementFailure
}

func (e *VerifyError) Error() string {
	names := make([]string, 0, len(e.Failures))
	for _, f := range e.Failures {
		names = append(names, f.Name)
	}
	return fmt.Sprintf("crucible/state: verify failed: %s", strings.Join(names, ", "))
}

// UnsupportedSchemaError is returned by LoadFromJSON when an IR document declares a
// schema major version newer than the loader supports. The reject-higher-major
// policy is the reserved compatibility seam: a higher minor (same major) loads,
// preserving unknown fields for forward-compat, but a higher major signals a wire
// form this build cannot safely interpret and is refused rather than guessed at.
type UnsupportedSchemaError struct {
	// Got is the schemaVersion declared in the document.
	Got string
	// Supported is the loader's own schema version.
	Supported string
}

func (e *UnsupportedSchemaError) Error() string {
	return fmt.Sprintf("crucible/state: unsupported schema version %q (loader supports %q)", e.Got, e.Supported)
}

// UnknownEffectKindError is returned by EffectRegistry.Dispatchable when an effect
// carries a kind the registry does not recognize. It realizes the reject half of
// the closed-enum extension policy for effect kinds: an unknown kind is preserved
// on load (as an UnknownEffect) so a foreign effect round-trips losslessly, but
// it is refused at dispatch rather than silently applied — the host must register
// the kind (RegisterEffect) or drop the effect deliberately.
type UnknownEffectKindError struct {
	// Kind is the unrecognized effect discriminant.
	Kind string
}

func (e *UnknownEffectKindError) Error() string {
	return fmt.Sprintf("crucible/state: unknown effect kind %q (not registered for dispatch)", e.Kind)
}
