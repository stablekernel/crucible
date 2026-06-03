// SPDX-License-Identifier: Apache-2.0

package statemachine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/statemachine"
	"github.com/stablekernel/crucible/state"
)

func TestDriveFunc_StatelessAckAndEmit(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()

	var emitted []any
	sink := statemachine.SinkFunc(func(_ context.Context, eff any) error {
		emitted = append(emitted, eff)
		return nil
	})
	// The host owns the instance; here a fresh funded one per call.
	fire := func(ctx context.Context, _ turnstileState, ev turnstileEvent) (state.FireResult[turnstileState], error) {
		inst := m.Cast(&turnstile{Funded: true}, state.WithInitialState[turnstileState](locked))
		return inst.Fire(ctx, ev), nil
	}
	h := statemachine.DriveFunc[turnstileState, turnstileEvent](fire, routeFunded, statemachine.WithSink(sink))

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionAck {
		t.Fatalf("action = %v, want ack (err=%v)", res.Action, res.Err)
	}
	if len(emitted) != 1 {
		t.Fatalf("emitted %d effects, want 1", len(emitted))
	}
}

func TestDriveFunc_StateAwareReject(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	fire := func(ctx context.Context, _ turnstileState, ev turnstileEvent) (state.FireResult[turnstileState], error) {
		inst := m.Cast(&turnstile{Funded: false}, state.WithInitialState[turnstileState](locked))
		return inst.Fire(ctx, ev), nil
	}
	h := statemachine.DriveFunc[turnstileState, turnstileEvent](fire, routeFunded)

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionTerm || res.Class != source.InvalidForState {
		t.Fatalf("got %v/%v, want term/invalid_for_state", res.Action, res.Class)
	}
	if !errors.Is(res.Err, source.ErrInvalidForState) {
		t.Fatalf("err %v does not match ErrInvalidForState", res.Err)
	}
}

func TestDriveFunc_ResolutionError_IsNak(t *testing.T) {
	t.Parallel()
	boom := errors.New("instance store down")
	fire := func(context.Context, turnstileState, turnstileEvent) (state.FireResult[turnstileState], error) {
		return state.FireResult[turnstileState]{}, boom
	}
	h := statemachine.DriveFunc[turnstileState, turnstileEvent](fire, routeFunded)

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionNak || res.Class != source.Retryable {
		t.Fatalf("got %v/%v, want nak/retryable", res.Action, res.Class)
	}
}

func TestDriveFunc_RouteFailure_IsTerm(t *testing.T) {
	t.Parallel()
	fire := func(ctx context.Context, _ turnstileState, ev turnstileEvent) (state.FireResult[turnstileState], error) {
		return state.FireResult[turnstileState]{}, nil
	}
	router := func(source.Message) (turnstileState, turnstileEvent, error) {
		return 0, 0, errors.New("bad")
	}
	h := statemachine.DriveFunc[turnstileState, turnstileEvent](fire, router)

	if res := h(context.Background(), msg("e", "c")); res.Action != source.ActionTerm {
		t.Fatalf("action = %v, want term", res.Action)
	}
}
