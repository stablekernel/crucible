package durable

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// These tests answer review findings on serviceStep's bookkeeping: a settle that finds
// its activation gone records nothing, the JSON a settle decoded isn't held past its
// step, and the settled services and parked results are forgotten together.

type stepCtx struct{}

// stepMachine invokes fetch in loading; onDone "done" goes to ready, onError "fail"
// to failed.
func stepMachine(fn state.ServiceFn[stepCtx]) *state.Machine[string, string, stepCtx] {
	return state.ForgeFor[stepCtx]("step").
		Service("fetch", fn).
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done"), state.WithInvokeOnError("fail")).
		State("ready").Final().
		State("failed").Final().
		Initial("loading").
		Transition("loading").On("done").GoTo("ready").
		Transition("loading").On("fail").GoTo("failed").
		Quench()
}

func stepOptions(fn state.ServiceFn[stepCtx]) []Option[string, string, stepCtx] {
	return []Option[string, string, stepCtx]{WithServiceRegistry[string, string, stepCtx](state.NewRegistry[stepCtx]().Service("fetch", fn))}
}

// TestServiceStep_StaleSettleRecordsNothing: when the activation a ServiceFn runs for is
// stopped, or stopped and armed again, before its settle, ServiceRunner.Tick fires
// nothing and returns nothing. serviceStep must record nothing for it: no
// JournalServiceResult, no settled service, and no JSON left behind, so a Recover still
// finds the service in flight. The step lock keeps durable's own steps from changing the
// activation while a ServiceFn runs (see TestRunService_ServiceFnCantChangeItsActivation),
// so this drives the ServiceRunner directly, as only code inside the package can.
func TestServiceStep_StaleSettleRecordsNothing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rearm bool
	}{{"stopped", false}, {"stopped and armed again", true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			id := state.InvokeID("step", "loading", 0)
			var h *Handle[string, string, stepCtx]
			runs := 0
			fn := func(ctx context.Context, _ state.ServiceCtx[stepCtx]) (any, error) {
				runs++
				if runs == 1 {
					effects := []state.Effect{state.StopService{ID: id}}
					if tc.rearm {
						effects = append(effects, h.inst.StartEffects()...)
					}
					h.svc.Absorb(ctx, effects)
				}
				return "late", nil
			}
			m := stepMachine(fn)
			st := NewMemStore()
			var err error
			if h, err = NewRunner(m, st, stepOptions(fn)...).Start(ctx, "s1", stepCtx{}, state.WithInitialState("loading")); err != nil {
				t.Fatalf("Start: %v", err)
			}
			// RunService reports the service ran, as its doc says: it was in flight and the
			// ServiceFn ran, though nothing settled, so the result is the zero one.
			res, ran, rerr := h.RunService(ctx, id)
			if rerr != nil || !ran || runs != 1 {
				t.Fatalf("RunService = %v, %v with the ServiceFn run %d times; want ran with no error, once", ran, rerr, runs)
			}
			if res.NewState != "" || len(res.Effects) != 0 || res.Err != nil || res.Trace.Event != "" {
				t.Fatalf("RunService returned %+v, want the zero FireResult: nothing fired", res)
			}
			if _, recs, _ := st.Load(ctx, "s1"); len(recs) != 0 {
				t.Fatalf("the journal holds %d Records, want none: the settle fired nothing", len(recs))
			}
			if h.settled[id] {
				t.Fatal("the service is noted settled, so a checkpoint would record it settled")
			}
			if h.svcJSON.raw != nil || h.svcJSON.typ != "" {
				t.Fatalf("the Handle holds the stale settle's JSON %s (%s)", h.svcJSON.raw, h.svcJSON.typ)
			}
			if got := h.inst.Current(); got != "loading" {
				t.Fatalf("in %q, want loading", got)
			}
			if got := h.svc.HasPending(id); got != tc.rearm {
				t.Fatalf("HasPending = %v, want %v", got, tc.rearm)
			}
			rh, err := Recover(ctx, m, st, "s1", stepOptions(fn)...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if !rh.svc.HasPending(id) {
				t.Fatal("the recovered Handle has the service settled; the journal holds no settle")
			}
		})
	}
}

// TestServiceStep_StaleAbortParksNothing: when the activation a ServiceFn runs for is
// stopped and armed again before its settle, and the decoder refuses the stale result,
// ServiceRunner.Tick reports nothing, as for a settle that fired nothing, rather than
// the abort. serviceStep must park nothing: no Record, no parked result on the new
// activation, so the next RunService runs the ServiceFn for that activation. Before
// the fix, the stale abort parked the old run's result onto the new activation, which
// never ran its ServiceFn. It drives the ServiceRunner directly, as only code inside
// the package can (see TestServiceStep_StaleSettleRecordsNothing).
func TestServiceStep_StaleAbortParksNothing(t *testing.T) {
	ctx := context.Background()
	id := state.InvokeID("step", "loading", 0)
	var h *Handle[string, string, stepCtx]
	runs := 0
	fn := func(ctx context.Context, _ state.ServiceCtx[stepCtx]) (any, error) {
		runs++
		if runs == 1 {
			h.svc.Absorb(ctx, append([]state.Effect{state.StopService{ID: id}}, h.inst.StartEffects()...))
		}
		return "late", nil
	}
	refuse := WithEventDataDecoder[string, string, stepCtx](func(string, []byte) (any, error) {
		return nil, errors.New("decoder refused")
	})
	opts := append(stepOptions(fn), refuse)
	m := stepMachine(fn)
	st := NewMemStore()
	var err error
	if h, err = NewRunner(m, st, opts...).Start(ctx, "s1", stepCtx{}, state.WithInitialState("loading")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	res, ran, rerr := h.RunService(ctx, id)
	if rerr != nil || !ran || runs != 1 {
		t.Fatalf("RunService = %v, %v with the ServiceFn run %d times; want ran with no error, once: the stale settle parks nothing", ran, rerr, runs)
	}
	if res.NewState != "" || res.Err != nil {
		t.Fatalf("RunService returned %+v, want the zero FireResult", res)
	}
	if _, recs, _ := st.Load(ctx, "s1"); len(recs) != 0 {
		t.Fatalf("the journal holds %d Records, want none: the stale run parked nothing", len(recs))
	}
	if _, ok := h.parked[id]; ok {
		t.Fatal("the stale run's result is parked on the new activation")
	}
	if !h.svc.HasPending(id) {
		t.Fatal("the new activation isn't in flight")
	}
	// The new activation runs its own ServiceFn; its result, refused too, parks.
	if _, ran, rerr = h.RunService(ctx, id); !ran || !errors.Is(rerr, ErrEventDataEncoding) || runs != 2 {
		t.Fatalf("RunService of the new activation = %v, %v with the ServiceFn run %d times; want it run again and parked", ran, rerr, runs)
	}
	if _, err = Recover(ctx, m, st, "s1", opts...); err != nil {
		t.Fatalf("Recover: %v", err)
	}
}

// TestServiceStep_ZeroFireResultSettleIsRecorded: a middleware may return a zero
// FireResult for a Fire it ran. When that Fire is the onDone settle, the settle still
// happened: the runner dropped the service and the instance moved. serviceStep must
// record it as any settle, so the live Handle and the journal agree: the step is
// recorded, the service is noted settled, and Recover replays the result to the same
// state without running the ServiceFn again.
func TestServiceStep_ZeroFireResultSettleIsRecorded(t *testing.T) {
	ctx := context.Background()
	id := state.InvokeID("step", "loading", 0)
	runs := 0
	fn := func(context.Context, state.ServiceCtx[stepCtx]) (any, error) {
		runs++
		return "ok", nil
	}
	hide := func(next state.FireFunc[string, string, stepCtx]) state.FireFunc[string, string, stepCtx] {
		return func(ctx context.Context, event string) state.FireResult[string] {
			res := next(ctx, event)
			if event == "done" {
				return state.FireResult[string]{}
			}
			return res
		}
	}
	m := state.ForgeFor[stepCtx]("step").
		Use(hide).
		Service("fetch", fn).
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done"), state.WithInvokeOnError("fail")).
		State("ready").Final().
		State("failed").Final().
		Initial("loading").
		Transition("loading").On("done").GoTo("ready").
		Transition("loading").On("fail").GoTo("failed").
		Quench()
	st := NewMemStore()
	h, err := NewRunner(m, st, stepOptions(fn)...).Start(ctx, "s1", stepCtx{}, state.WithInitialState("loading"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, ran, rerr := h.RunService(ctx, id); rerr != nil || !ran {
		t.Fatalf("RunService = %v, %v; want ran with no error", ran, rerr)
	}
	if got := h.inst.Current(); got != "ready" || h.svc.HasPending(id) {
		t.Fatalf("live: in %q with the service pending %v; want ready and settled", got, h.svc.HasPending(id))
	}
	if !h.settled[id] {
		t.Fatal("the live Handle doesn't note the service settled")
	}
	_, recs, err := st.Load(ctx, "s1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("the journal holds %d Records, want the settle's one", len(recs))
	}
	rh, err := Recover(ctx, m, st, "s1", stepOptions(fn)...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := rh.inst.Current(); got != "ready" || rh.svc.HasPending(id) {
		t.Fatalf("recovered: in %q with the service pending %v; want ready and settled, as live", got, rh.svc.HasPending(id))
	}
	if _, ran, rerr := rh.RunService(ctx, id); ran || rerr != nil {
		t.Fatalf("the recovered RunService = %v, %v; want nothing in flight", ran, rerr)
	}
	if runs != 1 {
		t.Fatalf("the ServiceFn ran %d times, want once", runs)
	}
}

// TestServiceStep_ClearsTheSettledJSON: the JSON a settle decoded is the step's alone.
// Once RunService returns, live or for a parked result, and once Recover has replayed
// the journal, the Handle holds none of it.
func TestServiceStep_ClearsTheSettledJSON(t *testing.T) {
	ctx := context.Background()
	id := state.InvokeID("step", "loading", 0)
	fn := func(context.Context, state.ServiceCtx[stepCtx]) (any, error) { return "ok", nil }
	m := stepMachine(fn)
	st := NewMemStore()
	h, err := NewRunner(m, st, stepOptions(fn)...).Start(ctx, "s1", stepCtx{}, state.WithInitialState("loading"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, _, err = h.RunService(ctx, id); err != nil {
		t.Fatalf("RunService: %v", err)
	}
	if h.svcJSON.raw != nil {
		t.Fatalf("after RunService the Handle holds %s", h.svcJSON.raw)
	}
	rh, err := Recover(ctx, m, st, "s1", stepOptions(fn)...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rh.svcJSON.raw != nil {
		t.Fatalf("after Recover the Handle holds %s", rh.svcJSON.raw)
	}

	// A parked result's settle.
	refuse := WithEventDataDecoder[string, string, stepCtx](func(e string, data []byte) (any, error) {
		if e == "done" {
			return nil, ErrEventDataEncoding
		}
		return DecodeAs[any](data)
	})
	st = NewMemStore()
	if h, err = NewRunner(m, st, append(stepOptions(fn), refuse)...).Start(ctx, "s1", stepCtx{}, state.WithInitialState("loading")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, _, err = h.RunService(ctx, id); err == nil {
		t.Fatal("RunService settled a result the decoder refuses")
	}
	if rh, err = Recover(ctx, m, st, "s1", stepOptions(fn)...); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, _, err = rh.RunService(ctx, id); err != nil {
		t.Fatalf("RunService of the parked result: %v", err)
	}
	if rh.svcJSON.raw != nil {
		t.Fatalf("after a parked result's settle the Handle holds %s", rh.svcJSON.raw)
	}
}

// TestServiceBookkeeping_SettledAndParkedMoveTogether pins that the Handle and the
// replay mirror keep a service out of the parked results once it settles, and forget
// both when a state that declares it is entered or exited, through one helper each.
func TestServiceBookkeeping_SettledAndParkedMoveTogether(t *testing.T) {
	start := []state.Effect{state.StartService{ID: "a"}}
	stop := []state.Effect{state.StopService{ID: "b"}}
	h := &Handle[string, string, stepCtx]{parked: map[string]recordedResult{"a": {}, "b": {}}}
	h.markSettled("a")
	if _, ok := h.parked["a"]; ok || !h.settled["a"] {
		t.Fatalf("after markSettled: settled %v, parked %v; want a settled and not parked", h.settled, h.parked)
	}
	h.settled["b"] = true // b settled in an earlier activation, then parked in this one
	h.forgetServices(start)
	h.forgetServices(stop)
	if len(h.settled) != 0 || len(h.parked) != 0 {
		t.Fatalf("after start and stop effects: settled %v, parked %v; want both empty", h.settled, h.parked)
	}

	m := &armMirror{}
	m.park("a", recordedResult{})
	m.park("b", recordedResult{})
	m.settle("a")
	if _, ok := m.parked["a"]; ok || !m.settled["a"] {
		t.Fatalf("after settle: settled %v, parked %v; want a settled and not parked", m.settled, m.parked)
	}
	m.settled["b"] = true
	m.forget(start)
	m.forget(stop)
	if len(m.settled) != 0 || len(m.parked) != 0 {
		t.Fatalf("after start and stop effects: settled %v, parked %v; want both empty", m.settled, m.parked)
	}
}
