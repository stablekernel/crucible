package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// rearm stops the in-flight "loading" service by leaving the state, then enters it
// again, which arms a new activation under the same InvokeID.
func rearm(ctx context.Context, t *testing.T, inst *state.Instance[string, string, *trec], run *state.ServiceRunner[string, string, *trec], id string) {
	t.Helper()
	run.Absorb(ctx, inst.Fire(ctx, "cancel").Effects)
	if run.HasPending(id) {
		t.Fatalf("leaving loading left %q in flight", id)
	}
	run.Absorb(ctx, inst.Fire(ctx, "start").Effects)
	if !run.HasPending(id) || inst.Current() != "loading" {
		t.Fatalf("re-entering loading didn't arm %q again", id)
	}
}

// TestSettle_StaleTransformDoesNotSettleARearmedActivation proves a settle whose
// transform ran for one activation of a service can't settle a later one. The
// transform blocks while the test stops the service and re-arms it under the same
// InvokeID; once released, the old settle must return false, fire nothing, and leave
// the new activation in flight.
func TestSettle_StaleTransformDoesNotSettleARearmedActivation(t *testing.T) {
	ctx := context.Background()
	id := state.InvokeID("loader", "loading", 0)
	var run *state.ServiceRunner[string, string, *trec]
	m := invokeMachine(&run)
	entity := &trec{}
	inst := m.Cast(entity, state.WithInitialState("idle"))
	entered, release := make(chan struct{}), make(chan struct{})
	block := true
	run = state.NewServiceRunner(inst, nil).WithResultTransform(func(_ string, result any, err error) (any, error, error) {
		if block {
			close(entered)
			<-release
		}
		return result, err, nil
	})
	run.Absorb(ctx, inst.Fire(ctx, "start").Effects)

	type settled struct {
		res state.FireResult[string]
		ok  bool
	}
	done := make(chan settled)
	go func() {
		res, ok := run.SettleDone(ctx, id, "old")
		done <- settled{res, ok}
	}()
	<-entered
	rearm(ctx, t, inst, run, id)
	block = false
	close(release)
	got := <-done

	if got.ok || got.res.NewState != "" {
		t.Fatalf("the stale settle returned %+v, %v; want false and nothing fired", got.res, got.ok)
	}
	if inst.Current() != "loading" || len(entity.notes) != 0 {
		t.Fatalf("the stale settle moved the instance to %q with notes %q, want loading and none", inst.Current(), entity.notes)
	}
	if !run.HasPending(id) {
		t.Fatalf("the stale settle dropped the re-armed activation, want it in flight")
	}
	// The new activation still settles with its own outcome.
	if res, ok := run.SettleDone(ctx, id, "new"); !ok || res.NewState != "ready" || join(entity.notes) != "result:new" {
		t.Fatalf("settling the new activation gave %+v, %v, notes %q; want ready with result:new", res, ok, entity.notes)
	}
}

// TestTick_StaleServiceRunDoesNotSettleARearmedActivation proves the same for the
// ServiceFn's own run: a Tick whose ServiceFn ran for one activation can't settle a
// later one armed under the same InvokeID while it ran. It returns nil, as for an id
// not in flight, so a caller can't mistake it for a settle whose Fire returned a zero
// FireResult.
func TestTick_StaleServiceRunDoesNotSettleARearmedActivation(t *testing.T) {
	ctx := context.Background()
	id := state.InvokeID("loader", "loading", 0)
	var run *state.ServiceRunner[string, string, *trec]
	m := invokeMachine(&run)
	entity := &trec{}
	inst := m.Cast(entity, state.WithInitialState("idle"))
	entered, release := make(chan struct{}), make(chan struct{})
	block := true
	reg := state.NewRegistry[*trec]().Service("fetch", func(context.Context, state.ServiceCtx[*trec]) (any, error) {
		if block {
			close(entered)
			<-release
		}
		return "payload", nil
	})
	run = state.NewServiceRunner(inst, reg)
	run.Absorb(ctx, inst.Fire(ctx, "start").Effects)

	done := make(chan []state.FireResult[string])
	go func() { done <- run.Tick(ctx, id) }()
	<-entered
	rearm(ctx, t, inst, run, id)
	block = false
	close(release)
	got := <-done

	if got != nil {
		t.Fatalf("the stale Tick returned %+v, want nil: it settled nothing", got)
	}
	if inst.Current() != "loading" || len(entity.notes) != 0 {
		t.Fatalf("the stale Tick moved the instance to %q with notes %q, want loading and none", inst.Current(), entity.notes)
	}
	if !run.HasPending(id) {
		t.Fatalf("the stale Tick dropped the re-armed activation, want it in flight")
	}
}

// TestSettle_StaleAbortSettlesNothing proves an aborted settle of an activation stopped,
// and armed again, while its transform ran reports nothing: the activation it ran for
// is gone, so the abort has nothing to leave in flight. SettleDone and SettleError
// return false with no Err, and Tick an empty slice, as for an id not in flight. The
// re-armed activation stays in flight and settles with its own outcome.
func TestSettle_StaleAbortSettlesNothing(t *testing.T) {
	abortErr := errors.New("transform aborted")
	for _, verb := range []string{"SettleDone", "SettleError", "Tick"} {
		t.Run(verb, func(t *testing.T) {
			ctx := context.Background()
			id := state.InvokeID("loader", "loading", 0)
			var run *state.ServiceRunner[string, string, *trec]
			m := invokeMachine(&run)
			entity := &trec{}
			inst := m.Cast(entity, state.WithInitialState("idle"))
			entered, release := make(chan struct{}), make(chan struct{})
			block := true
			reg := state.NewRegistry[*trec]().Service("fetch", func(context.Context, state.ServiceCtx[*trec]) (any, error) {
				return "payload", nil
			})
			run = state.NewServiceRunner(inst, reg).WithResultTransform(func(_ string, result any, err error) (any, error, error) {
				if block {
					close(entered)
					<-release
					return nil, nil, abortErr
				}
				return result, err, nil
			})
			run.Absorb(ctx, inst.Fire(ctx, "start").Effects)

			type settled struct {
				res []state.FireResult[string]
				ok  bool
			}
			done := make(chan settled)
			go func() {
				var res state.FireResult[string]
				var ok bool
				switch verb {
				case "SettleDone":
					res, ok = run.SettleDone(ctx, id, "old")
				case "SettleError":
					res, ok = run.SettleError(ctx, id, errors.New("old"))
				default:
					done <- settled{res: run.Tick(ctx, id)}
					return
				}
				done <- settled{[]state.FireResult[string]{res}, ok}
			}()
			<-entered
			rearm(ctx, t, inst, run, id)
			block = false
			close(release)
			got := <-done

			if verb == "Tick" {
				if len(got.res) != 0 {
					t.Fatalf("the stale aborted Tick returned %+v, want an empty slice: it settled nothing", got.res)
				}
			} else if got.ok || got.res[0].Err != nil || got.res[0].NewState != "" {
				t.Fatalf("the stale aborted %s returned %+v, %v; want false with no Err: it settled nothing", verb, got.res[0], got.ok)
			}
			if inst.Current() != "loading" || len(entity.notes) != 0 {
				t.Fatalf("the stale settle moved the instance to %q with notes %q, want loading and none", inst.Current(), entity.notes)
			}
			if !run.HasPending(id) {
				t.Fatalf("the stale settle dropped the re-armed activation, want it in flight")
			}
			if res, ok := run.SettleDone(ctx, id, "new"); !ok || res.NewState != "ready" || join(entity.notes) != "result:new" {
				t.Fatalf("settling the new activation gave %+v, %v, notes %q; want ready with result:new", res, ok, entity.notes)
			}
		})
	}
}
