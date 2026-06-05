package state_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// invokeMachine builds a flat machine whose "loading" state invokes the "fetch"
// service: on success it fires "ok" and moves to "ready"; on failure it fires
// "fail" and moves to "errored". A plain "cancel" event exits "loading" early (to
// "idle"), so a test can exercise auto-stop-on-exit. The onDone/onError actions
// record the service result/error read from the runner so a test asserts the
// payload routed through.
func invokeMachine(run **state.ServiceRunner[string, string, *trec]) *state.Machine[string, string, *trec] {
	return state.Forge[string, string, *trec]("loader").
		Service("fetch", func(context.Context, state.ServiceCtx[*trec]) (any, error) {
			return "payload", nil
		}).
		Action("captureResult", func(c state.ActionCtx[*trec]) (state.Effect, error) {
			if r, ok := (*run).LastResult(); ok {
				c.Entity.notes = append(c.Entity.notes, "result:"+r.(string))
			}
			return nil, nil
		}).
		Action("captureError", func(c state.ActionCtx[*trec]) (state.Effect, error) {
			if e := (*run).LastError(); e != nil {
				c.Entity.notes = append(c.Entity.notes, "error:"+e.Error())
			}
			return nil, nil
		}).
		State("idle").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("ok"), state.WithInvokeOnError("fail")).
		State("ready").
		State("errored").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("loading").
		Transition("loading").On("ok").GoTo("ready").Do("captureResult").
		Transition("loading").On("fail").GoTo("errored").Do("captureError").
		Transition("loading").On("cancel").GoTo("idle").
		Quench()
}

// TestInvoke_OnDone drives an invoked service to success: entering loading arms
// the service, settling it done fires the onDone event, and the instance lands in
// ready with the service result routed through.
func TestInvoke_OnDone(t *testing.T) {
	var run *state.ServiceRunner[string, string, *trec]
	m := invokeMachine(&run)
	entity := &trec{}
	inst := m.Cast(entity, state.WithInitialState("idle"))
	run = state.NewServiceRunner(inst, nil)
	ctx := context.Background()

	res := inst.Fire(ctx, "start")
	run.Absorb(ctx, res.Effects)

	if inst.Current() != "loading" {
		t.Fatalf("after start, want loading, got %q", inst.Current())
	}
	id := state.InvokeID("loader", "loading", 0)
	if !run.HasPending(id) {
		t.Fatalf("entering loading should start service %q; pending=%d", id, run.Pending())
	}
	assertStartEffect(t, res.Effects, id, "fetch", "ok", "fail")

	fr, ok := run.SettleDone(ctx, id, "payload")
	if !ok {
		t.Fatalf("SettleDone reported no in-flight service %q", id)
	}
	if fr.NewState != "ready" {
		t.Fatalf("after onDone, want ready, got %q", fr.NewState)
	}
	if inst.Current() != "ready" {
		t.Fatalf("instance want ready, got %q", inst.Current())
	}
	if run.Pending() != 0 {
		t.Fatalf("service should be settled; pending=%d", run.Pending())
	}
	if got := join(entity.notes); got != "result:payload" {
		t.Fatalf("onDone action notes = %q, want result:payload", got)
	}
}

// TestInvoke_OnError drives an invoked service to failure: settling it with an
// error fires the onError event, the instance lands in errored, and the error is
// routed through.
func TestInvoke_OnError(t *testing.T) {
	var run *state.ServiceRunner[string, string, *trec]
	m := invokeMachine(&run)
	entity := &trec{}
	inst := m.Cast(entity, state.WithInitialState("idle"))
	run = state.NewServiceRunner(inst, nil)
	ctx := context.Background()

	run.Absorb(ctx, inst.Fire(ctx, "start").Effects)
	id := state.InvokeID("loader", "loading", 0)

	fr, ok := run.SettleError(ctx, id, errors.New("boom"))
	if !ok {
		t.Fatalf("SettleError reported no in-flight service %q", id)
	}
	if fr.NewState != "errored" {
		t.Fatalf("after onError, want errored, got %q", fr.NewState)
	}
	if run.Pending() != 0 {
		t.Fatalf("service should be settled; pending=%d", run.Pending())
	}
	if got := join(entity.notes); got != "error:boom" {
		t.Fatalf("onError action notes = %q, want error:boom", got)
	}
}

// TestInvoke_StoppedOnExit asserts auto-stop-on-exit: leaving the loading
// state before the service completes emits StopService and the service can no
// longer settle (onDone never fires).
func TestInvoke_StoppedOnExit(t *testing.T) {
	var run *state.ServiceRunner[string, string, *trec]
	m := invokeMachine(&run)
	entity := &trec{}
	inst := m.Cast(entity, state.WithInitialState("idle"))
	run = state.NewServiceRunner(inst, nil)
	ctx := context.Background()

	run.Absorb(ctx, inst.Fire(ctx, "start").Effects)
	id := state.InvokeID("loader", "loading", 0)
	if !run.HasPending(id) {
		t.Fatalf("service %q should be in flight after entering loading", id)
	}

	// Leave loading before the service completes: a StopService effect drops it.
	leave := inst.Fire(ctx, "cancel")
	assertStopEffect(t, leave.Effects, id)
	run.Absorb(ctx, leave.Effects)

	if inst.Current() != "idle" {
		t.Fatalf("after cancel, want idle, got %q", inst.Current())
	}
	if run.HasPending(id) || run.Pending() != 0 {
		t.Fatalf("service should be auto-stopped on exit; pending=%d", run.Pending())
	}

	// A late settlement of the stopped service is a no-op: onDone never fires.
	if _, ok := run.SettleDone(ctx, id, "payload"); ok {
		t.Fatal("stopped service should not settle")
	}
	if inst.Current() != "idle" {
		t.Fatalf("stopped service changed state to %q", inst.Current())
	}
	if len(entity.notes) != 0 {
		t.Fatalf("stopped service ran an onDone action: %q", join(entity.notes))
	}
}

// TestInvoke_UnboundServiceQuench asserts an invoke whose Src is not registered
// fails Quench with the typed *ErrUnboundRef (Kind "service"), exactly like an
// unbound guard or action.
func TestInvoke_UnboundServiceQuench(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Quench with an unbound service ref should panic")
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("panic value is not an error: %T", r)
		}
		var ub *state.ErrUnboundRef
		if !errors.As(err, &ub) {
			t.Fatalf("panic = %v, want *ErrUnboundRef", err)
		}
		if ub.Kind != "service" || ub.Name != "missing" {
			t.Fatalf("unbound ref = {%q, %q}, want {service, missing}", ub.Kind, ub.Name)
		}
	}()

	state.Forge[string, string, *trec]("unbound").
		State("idle").
		State("loading").Invoke("missing", state.WithInvokeOnDone("ok"), state.WithInvokeOnError("fail")).
		State("ready").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("loading").
		Transition("loading").On("ok").GoTo("ready").
		Quench()
}

// TestInvoke_RoundTrip asserts the invoke block round-trips losslessly: the id,
// src ref + params, input, and onDone/onError survive ToJSON -> LoadFromJSON, and
// the rehydrated machine starts the same service.
func TestInvoke_RoundTrip(t *testing.T) {
	m := state.Forge[string, string, *trec]("rt").
		Service("fetch", func(context.Context, state.ServiceCtx[*trec]) (any, error) { return nil, nil }).
		State("idle").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("ok"), state.WithInvokeOnError("fail"),
		state.WithInvokeID("svc-1"),
		state.WithServiceParams(map[string]any{"url": "/x"}),
		state.WithInput(map[string]any{"page": float64(2)})).
		State("ready").
		State("errored").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("loading").
		Transition("loading").On("ok").GoTo("ready").
		Transition("loading").On("fail").GoTo("errored").
		Quench()

	raw, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}

	// The invoke block survives serialization with all of its fields.
	var probe struct {
		States []struct {
			Name   string `json:"name"`
			Invoke []struct {
				ID    string         `json:"id"`
				Src   state.Ref      `json:"src"`
				Input map[string]any `json:"input"`
				Done  string         `json:"onDone"`
				Err   string         `json:"onError"`
			} `json:"invoke"`
		} `json:"states"`
	}
	if uerr := json.Unmarshal(raw, &probe); uerr != nil {
		t.Fatalf("probe unmarshal err = %v", uerr)
	}
	found := false
	for _, s := range probe.States {
		for _, iv := range s.Invoke {
			found = true
			if iv.ID != "svc-1" || iv.Src.Name != "fetch" || iv.Done != "ok" || iv.Err != "fail" {
				t.Fatalf("invoke round-trip = %+v, want id svc-1 / fetch / ok / fail", iv)
			}
			if iv.Src.Params["url"] != "/x" || iv.Input["page"] != float64(2) {
				t.Fatalf("invoke params/input round-trip lost: %+v", iv)
			}
		}
	}
	if !found {
		t.Fatal("serialized IR carried no invoke block")
	}

	ir, err := state.LoadFromJSON[string, string, *trec](raw)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	reg := state.NewRegistry[*trec]().
		Service("fetch", func(context.Context, state.ServiceCtx[*trec]) (any, error) { return "ok", nil })
	m2 := ir.Provide(reg).Quench()

	inst := m2.Cast(&trec{}, state.WithInitialState("idle"))
	run := state.NewServiceRunner(inst, reg)
	ctx := context.Background()

	run.Absorb(ctx, inst.Fire(ctx, "start").Effects)
	if !run.HasPending("svc-1") {
		t.Fatalf("rehydrated machine should start service svc-1; pending=%d", run.Pending())
	}
	if _, ok := run.SettleDone(ctx, "svc-1", "x"); !ok {
		t.Fatal("rehydrated service did not settle")
	}
	if inst.Current() != "ready" {
		t.Fatalf("rehydrated after onDone, want ready, got %q", inst.Current())
	}
}

// TestInvoke_RunResolvesService asserts the Run convenience resolves and executes
// the bound ServiceFn against the registry and routes its result through onDone.
func TestInvoke_RunResolvesService(t *testing.T) {
	var run *state.ServiceRunner[string, string, *trec]
	m := invokeMachine(&run)
	entity := &trec{}
	inst := m.Cast(entity, state.WithInitialState("idle"))
	run = state.NewServiceRunner(inst, registryFromMachine{m}.regValue())
	ctx := context.Background()

	run.Absorb(ctx, inst.Fire(ctx, "start").Effects)
	id := state.InvokeID("loader", "loading", 0)
	fr, ok := run.Run(ctx, id)
	if !ok {
		t.Fatalf("Run reported no in-flight service %q", id)
	}
	if fr.NewState != "ready" {
		t.Fatalf("after Run, want ready, got %q", fr.NewState)
	}
	if got := join(entity.notes); got != "result:payload" {
		t.Fatalf("Run onDone notes = %q, want result:payload", got)
	}
}

// TestInvoke_StartEffectsInitialState asserts StartEffects arms the services of
// the initial active configuration, so an invoke on the very first state runs.
func TestInvoke_StartEffectsInitialState(t *testing.T) {
	var run *state.ServiceRunner[string, string, *trec]
	m := state.Forge[string, string, *trec]("boot").
		Service("fetch", func(context.Context, state.ServiceCtx[*trec]) (any, error) { return "p", nil }).
		State("loading").Invoke("fetch", state.WithInvokeOnDone("ok"), state.WithInvokeOnError("fail")).
		State("ready").
		State("errored").
		Initial("loading").
		CurrentStateFn(func(*trec) string { return "loading" }).
		Transition("loading").On("ok").GoTo("ready").
		Transition("loading").On("fail").GoTo("errored").
		Quench()

	inst := m.Cast(&trec{}, state.WithInitialState("loading"))
	run = state.NewServiceRunner(inst, nil)
	ctx := context.Background()

	run.Absorb(ctx, inst.StartEffects())
	id := state.InvokeID("boot", "loading", 0)
	if !run.HasPending(id) {
		t.Fatalf("StartEffects should arm initial-state service %q; pending=%d", id, run.Pending())
	}
	if _, ok := run.SettleDone(ctx, id, "p"); !ok {
		t.Fatal("initial-state service did not settle")
	}
	if inst.Current() != "ready" {
		t.Fatalf("after onDone, want ready, got %q", inst.Current())
	}
}

// assertStartEffect fails unless effects contains a StartService matching id, src,
// onDone, and onError.
func assertStartEffect(t *testing.T, effects []state.Effect, id, src, onDone, onError string) {
	t.Helper()
	for _, e := range effects {
		s, ok := e.(state.StartService)
		if !ok || s.ID != id {
			continue
		}
		if s.Src.Name != src {
			t.Fatalf("start %q src = %q, want %q", id, s.Src.Name, src)
		}
		if s.OnDone != any(onDone) || s.OnError != any(onError) {
			t.Fatalf("start %q routing = %v/%v, want %q/%q", id, s.OnDone, s.OnError, onDone, onError)
		}
		return
	}
	t.Fatalf("no StartService effect for id %q in %v", id, effects)
}

// assertStopEffect fails unless effects contains a StopService for id.
func assertStopEffect(t *testing.T, effects []state.Effect, id string) {
	t.Helper()
	for _, e := range effects {
		if c, ok := e.(state.StopService); ok && c.ID == id {
			return
		}
	}
	t.Fatalf("no StopService effect for id %q in %v", id, effects)
}

// join concatenates trec notes with commas for assertion.
func join(notes []string) string {
	out := ""
	for i, n := range notes {
		if i > 0 {
			out += ","
		}
		out += n
	}
	return out
}

// registryFromMachine adapts a machine's bound service palette into a Registry
// the ServiceRunner can resolve against, exercising Machine.Services.
type registryFromMachine struct {
	m *state.Machine[string, string, *trec]
}

func (r registryFromMachine) regValue() *state.Registry[*trec] {
	reg := state.NewRegistry[*trec]()
	for name, fn := range r.m.Services() {
		reg.Service(name, fn)
	}
	return reg
}
