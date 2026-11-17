package state_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestWithResultTransform_RewritesTheOutcomeTickSettles proves Tick settles a bound
// service with the transform's outcome, a result or an error, and that LastResult and
// LastError report the rewritten form.
func TestWithResultTransform_RewritesTheOutcomeTickSettles(t *testing.T) {
	ctx := context.Background()
	upper := func(result any, err error) (any, error) {
		if err != nil {
			return nil, errors.New("wrapped: " + err.Error())
		}
		return strings.ToUpper(result.(string)), nil
	}
	cases := []struct {
		name      string
		fn        state.ServiceFn[*trec]
		wantState string
		wantNotes string
	}{
		{"result", func(context.Context, state.ServiceCtx[*trec]) (any, error) { return "payload", nil }, "ready", "result:PAYLOAD"},
		{"error", func(context.Context, state.ServiceCtx[*trec]) (any, error) { return nil, errors.New("boom") }, "errored", "error:wrapped: boom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var run *state.ServiceRunner[string, string, *trec]
			m := invokeMachine(&run)
			entity := &trec{}
			inst := m.Cast(entity, state.WithInitialState("idle"))
			run = state.NewServiceRunner(inst, state.NewRegistry[*trec]().Service("fetch", tc.fn)).WithResultTransform(upper)
			run.Absorb(ctx, inst.Fire(ctx, "start").Effects)
			res := run.Tick(ctx, state.InvokeID("loader", "loading", 0))
			if len(res) != 1 || res[0].NewState != tc.wantState {
				t.Fatalf("Tick settled to %+v, want %q", res, tc.wantState)
			}
			if got := join(entity.notes); got != tc.wantNotes {
				t.Fatalf("notes = %q, want %q", got, tc.wantNotes)
			}
		})
	}
}

// TestWithResultTransform_UnboundServiceStillRoutesOnError proves a transform can't
// turn an unbound service into a success: a transform that returns a result and no
// error for the UnboundRefError still settles the service through onError, with the
// UnboundRefError.
func TestWithResultTransform_UnboundServiceStillRoutesOnError(t *testing.T) {
	ctx := context.Background()
	var run *state.ServiceRunner[string, string, *trec]
	m := invokeMachine(&run)
	entity := &trec{}
	inst := m.Cast(entity, state.WithInitialState("idle"))
	// A nil registry leaves "fetch" unbound.
	run = state.NewServiceRunner(inst, nil).WithResultTransform(func(any, error) (any, error) {
		return "fallback", nil
	})
	run.Absorb(ctx, inst.Fire(ctx, "start").Effects)
	res := run.Tick(ctx, state.InvokeID("loader", "loading", 0))
	if len(res) != 1 || res[0].NewState != "errored" {
		t.Fatalf("an unbound service settled to %+v, want onError's errored", res)
	}
	var unbound *state.UnboundRefError
	if !errors.As(run.LastError(), &unbound) {
		t.Fatalf("LastError = %v, want the UnboundRefError", run.LastError())
	}
}
