package state_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestErrNoTransition_MatchesOnlyTheKernelsOwnRefusal pins that errors.Is(err,
// ErrNoTransition) is true for the refusal a Fire returns when the active
// configuration declares no transition for the event, flat or parallel, wrapped or
// not, and false for any other error, including a host-built InvalidTransitionError
// worded the same way.
func TestErrNoTransition_MatchesOnlyTheKernelsOwnRefusal(t *testing.T) {
	ctx := context.Background()
	flat := state.ForgeFor[acct]("flat").
		State("a").
		State("b").Final().
		Initial("a").
		Transition("a").On("go").GoTo("b").
		Quench()
	parallel := state.ForgeFor[acct]("par").
		SuperState("live").
		Region("r1").
		Initial("r1a").
		SubState("r1a").On("tick").GoTo("r1b").
		SubState("r1b").
		EndRegion().
		Region("r2").
		Initial("r2a").
		SubState("r2a").
		EndRegion().
		EndSuperState().
		Initial("live").
		Quench()
	flatErr := flat.Cast(acct{}, state.WithInitialState[string]("a")).Fire(ctx, "nope").Err
	parErr := parallel.Cast(acct{}, state.WithInitialState[string]("live")).Fire(ctx, "nope").Err
	final := flat.Cast(acct{}, state.WithInitialState[string]("a"))
	if res := final.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("Fire(go): %v", res.Err)
	}
	finalErr := final.Fire(ctx, "go").Err

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"flat no transition", flatErr, true},
		{"parallel no transition", parErr, true},
		{"wrapped", fmt.Errorf("host: %w", flatErr), true},
		{"other refusal", finalErr, false},
		{"lookalike", &state.InvalidTransitionError{Event: "nope", Reason: "no transition declared for this state and event"}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.want && tc.err == nil {
				t.Fatal("the Fire returned no error")
			}
			if got := errors.Is(tc.err, state.ErrNoTransition); got != tc.want {
				t.Fatalf("errors.Is(%v, ErrNoTransition) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
	var it *state.InvalidTransitionError
	if !errors.As(flatErr, &it) || it.Reason == "" {
		t.Fatalf("the refusal %v is no longer an *InvalidTransitionError with a Reason", flatErr)
	}
}
