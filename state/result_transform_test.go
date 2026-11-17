package state_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// upperTransform uppercases a string result and wraps an error's message, and notes
// the event it was given for each outcome in *events.
func upperTransform(events *[]string) func(string, any, error) (any, error, error) {
	return func(event string, result any, err error) (any, error, error) {
		*events = append(*events, event)
		if err != nil {
			return nil, errors.New("wrapped: " + err.Error()), nil
		}
		return strings.ToUpper(result.(string)), nil, nil
	}
}

// TestWithResultTransform_RewritesTheOutcomeTickSettles proves Tick settles a bound
// service with the transform's outcome, a result or an error, that LastResult and
// LastError report the rewritten form, and that the transform gets onDone's event for
// a result and onError's for an error.
func TestWithResultTransform_RewritesTheOutcomeTickSettles(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		fn        state.ServiceFn[*trec]
		wantState string
		wantNotes string
		wantEvent string
	}{
		{"result", func(context.Context, state.ServiceCtx[*trec]) (any, error) { return "payload", nil }, "ready", "result:PAYLOAD", "ok"},
		{"error", func(context.Context, state.ServiceCtx[*trec]) (any, error) { return nil, errors.New("boom") }, "errored", "error:wrapped: boom", "fail"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var run *state.ServiceRunner[string, string, *trec]
			var events []string
			m := invokeMachine(&run)
			entity := &trec{}
			inst := m.Cast(entity, state.WithInitialState("idle"))
			run = state.NewServiceRunner(inst, state.NewRegistry[*trec]().Service("fetch", tc.fn)).WithResultTransform(upperTransform(&events))
			run.Absorb(ctx, inst.Fire(ctx, "start").Effects)
			res := run.Tick(ctx, state.InvokeID("loader", "loading", 0))
			if len(res) != 1 || res[0].NewState != tc.wantState {
				t.Fatalf("Tick settled to %+v, want %q", res, tc.wantState)
			}
			if got := join(entity.notes); got != tc.wantNotes {
				t.Fatalf("notes = %q, want %q", got, tc.wantNotes)
			}
			if len(events) != 1 || events[0] != tc.wantEvent {
				t.Fatalf("the transform got events %q, want [%q]", events, tc.wantEvent)
			}
		})
	}
}

// TestWithResultTransform_AppliesToSettleDoneAndSettleError proves the transform runs
// on an outcome a host settles directly, as a replay does, with the event that carries
// it.
func TestWithResultTransform_AppliesToSettleDoneAndSettleError(t *testing.T) {
	ctx := context.Background()
	id := state.InvokeID("loader", "loading", 0)
	cases := []struct {
		name      string
		settle    func(*state.ServiceRunner[string, string, *trec]) (state.FireResult[string], bool)
		wantState string
		wantNotes string
		wantEvent string
	}{
		{"SettleDone", func(r *state.ServiceRunner[string, string, *trec]) (state.FireResult[string], bool) {
			return r.SettleDone(ctx, id, "payload")
		}, "ready", "result:PAYLOAD", "ok"},
		{"SettleError", func(r *state.ServiceRunner[string, string, *trec]) (state.FireResult[string], bool) {
			return r.SettleError(ctx, id, errors.New("boom"))
		}, "errored", "error:wrapped: boom", "fail"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var run *state.ServiceRunner[string, string, *trec]
			var events []string
			m := invokeMachine(&run)
			entity := &trec{}
			inst := m.Cast(entity, state.WithInitialState("idle"))
			run = state.NewServiceRunner(inst, nil).WithResultTransform(upperTransform(&events))
			run.Absorb(ctx, inst.Fire(ctx, "start").Effects)
			res, ok := tc.settle(run)
			if !ok || res.NewState != tc.wantState {
				t.Fatalf("settled to %+v (%v), want %q", res, ok, tc.wantState)
			}
			if got := join(entity.notes); got != tc.wantNotes {
				t.Fatalf("notes = %q, want %q", got, tc.wantNotes)
			}
			if len(events) != 1 || events[0] != tc.wantEvent {
				t.Fatalf("the transform got events %q, want [%q]", events, tc.wantEvent)
			}
		})
	}
}

// TestWithResultTransform_AbortFiresNoEvent proves a transform that aborts stops the
// settle, whether Tick, SettleDone or SettleError drives it: no event fires, so no
// Assign, inspector or history entry; the caller gets the abort in FireResult.Err, with
// false from SettleDone and SettleError and as Tick's one element; LastResult and
// LastError keep the previous settle; and the service stays in flight, so a later
// settle the transform lets through routes it.
func TestWithResultTransform_AbortFiresNoEvent(t *testing.T) {
	ctx := context.Background()
	id := state.InvokeID("loader", "loading", 0)
	refused := errors.New("refused")
	fetch := func(context.Context, state.ServiceCtx[*trec]) (any, error) { return "payload", nil }
	cases := []struct {
		name   string
		settle func(*state.ServiceRunner[string, string, *trec]) (state.FireResult[string], bool)
	}{
		{"Tick", func(r *state.ServiceRunner[string, string, *trec]) (state.FireResult[string], bool) {
			res := r.Tick(ctx, id)
			if len(res) != 1 {
				t.Fatalf("Tick returned %d results, want 1", len(res))
			}
			return res[0], false
		}},
		{"SettleDone", func(r *state.ServiceRunner[string, string, *trec]) (state.FireResult[string], bool) {
			return r.SettleDone(ctx, id, "payload")
		}},
		{"SettleError", func(r *state.ServiceRunner[string, string, *trec]) (state.FireResult[string], bool) {
			return r.SettleError(ctx, id, errors.New("boom"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var run *state.ServiceRunner[string, string, *trec]
			m := invokeMachine(&run)
			entity := &trec{}
			var inspected int
			inst := m.Cast(entity, state.WithInitialState("idle"), state.WithUnboundedHistory[string](),
				state.WithInspector[string](state.InspectorFunc(func(state.InspectionEvent) { inspected++ })))
			abort := true
			run = state.NewServiceRunner(inst, state.NewRegistry[*trec]().Service("fetch", fetch)).
				WithResultTransform(func(_ string, result any, err error) (any, error, error) {
					if abort {
						return "ignored", nil, refused
					}
					return result, err, nil
				})
			run.Absorb(ctx, inst.Fire(ctx, "start").Effects)
			history, events := len(inst.History()), inspected

			res, ok := tc.settle(run)
			if ok || !errors.Is(res.Err, refused) {
				t.Fatalf("an aborted settle returned %+v, %v; want false and the abort in Err", res, ok)
			}
			if res.NewState != "" || len(res.Effects) != 0 {
				t.Fatalf("an aborted settle returned %+v, want no state or effects", res)
			}
			if got := inst.Current(); got != "loading" || len(entity.notes) != 0 {
				t.Fatalf("after the abort the instance is in %q with notes %q, want loading and none", got, entity.notes)
			}
			if len(inst.History()) != history || inspected != events {
				t.Fatalf("the abort added %d history entries and %d inspection events, want none",
					len(inst.History())-history, inspected-events)
			}
			if _, ok := run.LastResult(); ok || run.LastError() != nil {
				t.Fatalf("the abort set LastResult or LastError (%v)", run.LastError())
			}
			if !run.HasPending(id) {
				t.Fatalf("the aborted service isn't in flight, want it kept")
			}

			// The service is still in flight, so a settle the transform lets through
			// routes it.
			abort = false
			res, _ = tc.settle(run)
			if res.Err != nil || res.NewState == "" || run.HasPending(id) {
				t.Fatalf("settling after the abort gave %+v, pending %v; want it routed", res, run.HasPending(id))
			}
		})
	}
}

// TestWithResultTransform_PanicLeavesServiceInFlight proves a transform that panics
// panics out of the settle, as any host panic does, before any event fires and
// before the service is dropped.
func TestWithResultTransform_PanicLeavesServiceInFlight(t *testing.T) {
	ctx := context.Background()
	var run *state.ServiceRunner[string, string, *trec]
	m := invokeMachine(&run)
	entity := &trec{}
	inst := m.Cast(entity, state.WithInitialState("idle"))
	run = state.NewServiceRunner(inst, nil).WithResultTransform(func(string, any, error) (any, error, error) {
		panic("boom")
	})
	run.Absorb(ctx, inst.Fire(ctx, "start").Effects)
	id := state.InvokeID("loader", "loading", 0)
	func() {
		defer func() {
			if p := recover(); p != "boom" {
				t.Fatalf("recovered %v, want the transform's panic", p)
			}
		}()
		run.SettleDone(ctx, id, "payload")
	}()
	if got := inst.Current(); got != "loading" || len(entity.notes) != 0 {
		t.Fatalf("after the panic the instance is in %q with notes %q, want loading and none", got, entity.notes)
	}
	if !run.HasPending(id) {
		t.Fatalf("the service isn't in flight after the panic, want it kept")
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
	run = state.NewServiceRunner(inst, nil).WithResultTransform(func(string, any, error) (any, error, error) {
		return "fallback", nil, nil
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
