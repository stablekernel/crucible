package state_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// TestTypedErrors_Messages asserts every typed kernel error renders a non-empty,
// self-describing message that names the offending entity. These are the
// diagnostic surface a host logs and matches on, so the messages are part of the
// contract.
func TestTypedErrors_Messages(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		contains []string
	}{
		{
			name:     "InvalidTransition with target",
			err:      &state.InvalidTransitionError{From: "a", To: "b", Event: "go", Reason: "guard failed"},
			contains: []string{"invalid transition", "a", "b", "go", "guard failed"},
		},
		{
			name:     "InvalidTransition without target",
			err:      &state.InvalidTransitionError{From: "a", Event: "go", Reason: "no transition"},
			contains: []string{"invalid transition", "a", "go", "no transition"},
		},
		{
			name:     "GuardFailed",
			err:      &state.GuardFailedError{GuardName: "isReady", Reason: "predicate returned false"},
			contains: []string{"guard", "isReady", "predicate returned false"},
		},
		{
			name:     "GuardPanic",
			err:      &state.GuardPanicError{GuardName: "isReady", Recovered: "nil deref"},
			contains: []string{"guard", "isReady", "panicked", "nil deref"},
		},
		{
			name:     "AssignPanic",
			err:      &state.AssignPanicError{AssignName: "fold", Recovered: "boom"},
			contains: []string{"assign", "fold", "panicked", "boom"},
		},
		{
			name:     "PolicyDenied",
			err:      &state.PolicyDeniedError{PolicyName: "rbac", Reason: "no role"},
			contains: []string{"policy", "rbac", "denied", "no role"},
		},
		{
			name:     "UndeclaredState",
			err:      &state.UndeclaredStateError{State: "ghost"},
			contains: []string{"undeclared state", "ghost"},
		},
		{
			name:     "UnboundRef",
			err:      &state.UnboundRefError{Kind: "guard", Name: "g"},
			contains: []string{"unbound", "guard", "g"},
		},
		{
			name:     "MicrostepOverflow",
			err:      &state.MicrostepOverflowError{Limit: 64, State: "loop"},
			contains: []string{"run-to-completion", "64", "loop"},
		},
		{
			name:     "NoPath",
			err:      &state.NoPathError{From: "a", To: "z"},
			contains: []string{"no path", "a", "z"},
		},
		{
			name:     "WaitTimeout",
			err:      &state.WaitTimeoutError{Machine: "m", Timeout: 5 * time.Second, Last: "idle"},
			contains: []string{"WaitFor", "m", "5s", "idle"},
		},
		{
			name:     "NoInitialState",
			err:      &state.NoInitialStateError{Machine: "m"},
			contains: []string{"cannot Cast", "m", "no CurrentStateFn"},
		},
		{
			name:     "UnknownBuiltin",
			err:      &state.UnknownBuiltinError{Name: "crucible.bogus"},
			contains: []string{"unknown built-in action", "crucible.bogus"},
		},
		{
			name:     "UnboundActor",
			err:      &state.UnboundActorError{Name: "child"},
			contains: []string{"unbound actor ref", "child"},
		},
		{
			name:     "Snapshot with state",
			err:      &state.SnapshotError{Op: "restore", State: "leaf", Reason: "not declared"},
			contains: []string{"snapshot restore", "leaf", "not declared"},
		},
		{
			name:     "Snapshot without state",
			err:      &state.SnapshotError{Op: "marshal", Reason: "encode failed"},
			contains: []string{"snapshot marshal", "encode failed"},
		},
		{
			name:     "SnapshotVersion",
			err:      &state.SnapshotVersionError{Kind: "machineVersion", Machine: "m", Got: "2", Want: "1", Reason: "major bump"},
			contains: []string{"version mismatch", "machineVersion", "m", "2", "1", "major bump"},
		},
		{
			name:     "UnsupportedSchema",
			err:      &state.UnsupportedSchemaError{Got: "2.0", Supported: "1.0"},
			contains: []string{"unsupported schema version", "2.0", "1.0"},
		},
		{
			name:     "UnknownEffectKind",
			err:      &state.UnknownEffectKindError{Kind: "foreign"},
			contains: []string{"unknown effect kind", "foreign"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.err.Error()
			if msg == "" {
				t.Fatal("error message is empty")
			}
			for _, want := range tc.contains {
				if !strings.Contains(msg, want) {
					t.Fatalf("message %q does not contain %q", msg, want)
				}
			}
		})
	}
}

// TestActionFailedError_WrapsCause asserts ActionFailedError renders its context
// and unwraps to the underlying cause for errors.Is / errors.As.
func TestActionFailedError_WrapsCause(t *testing.T) {
	cause := errors.New("downstream boom")
	err := &state.ActionFailedError{TransitionName: "a->b", ActionName: "charge", Cause: cause}

	msg := err.Error()
	for _, want := range []string{"action", "charge", "a->b", "downstream boom"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q does not contain %q", msg, want)
		}
	}
	if !errors.Is(err, cause) {
		t.Fatal("ActionFailedError should unwrap to its cause")
	}
}

// TestMultiRegionError_AggregatesAndUnwraps asserts MultiRegionError renders each
// region's message and exposes them for errors.As traversal.
func TestMultiRegionError_AggregatesAndUnwraps(t *testing.T) {
	g := &state.GuardFailedError{GuardName: "g", Reason: "false"}
	a := &state.AssignPanicError{AssignName: "fold", Recovered: "boom"}
	err := &state.MultiRegionError{Errors: []error{g, a}}

	msg := err.Error()
	for _, want := range []string{"2 regions errored", "g", "fold"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q does not contain %q", msg, want)
		}
	}

	var gf *state.GuardFailedError
	if !errors.As(err, &gf) {
		t.Fatal("MultiRegionError should expose a *GuardFailedError via errors.As")
	}
	var ap *state.AssignPanicError
	if !errors.As(err, &ap) {
		t.Fatal("MultiRegionError should expose an *AssignPanicError via errors.As")
	}
}
