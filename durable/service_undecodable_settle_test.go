package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// outOfRangeResult is a service result whose MarshalJSON emits a number beyond
// float64, so its JSON form encodes but won't decode.
type outOfRangeResult struct{}

func (outOfRangeResult) MarshalJSON() ([]byte, error) { return []byte("1e400"), nil }

// undecodableProbe counts what a settle with a result that won't decode drives: the
// onDone and onError Assigns, with the type of the event each saw, the actor the
// onDone state spawns, and the domain effects dispatched.
type undecodableProbe struct {
	doneAssigns, errorAssigns int
	doneEventType             string
	errorMessage              string
	spawns                    int
	dispatched                int
}

// undecodableMachine invokes fetch in loading. Its onDone transition runs an Assign,
// emits a notify effect, and enters ready, which spawns child; its onError runs an
// Assign and enters failed. With assertString, the onDone Assign asserts its event is
// a string unchecked, as a reducer that trusts its service's result type does.
func undecodableMachine(fn state.ServiceFn[svcCtx], p *undecodableProbe, assertString bool) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("undecodable").
		Service("fetch", fn).
		Actor("child").
		Action("notify", func(state.ActionCtx[svcCtx]) (state.Effect, error) {
			return notifyEffect{To: "ops@example.com"}, nil
		}).
		Reducer("noteDone", func(in state.AssignCtx[svcCtx]) svcCtx {
			p.doneAssigns++
			p.doneEventType = fmt.Sprintf("%T", in.Event)
			c := in.Entity
			if assertString {
				c.Notes = append(c.Notes, in.Event.(string))
			}
			return c
		}).
		Reducer("noteError", func(in state.AssignCtx[svcCtx]) svcCtx {
			p.errorAssigns++
			if e, ok := in.Event.(error); ok {
				p.errorMessage = e.Error()
			}
			return in.Entity
		}).
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done"), state.WithInvokeOnError("fail")).
		State("ready").InvokeActor("child").
		State("failed").Final().
		Initial("loading").
		Transition("loading").On("done").GoTo("ready").Assign("noteDone").Do("notify").
		Transition("loading").On("fail").GoTo("failed").Assign("noteError").
		Quench()
}

func undecodableOptions(fn state.ServiceFn[svcCtx], p *undecodableProbe) []durable.Option[string, string, svcCtx] {
	return []durable.Option[string, string, svcCtx]{
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
		durable.WithActorPalette[string, string, svcCtx](map[string]state.ActorBehavior{
			"child": func(map[string]any) (state.ActorInstance, error) {
				p.spawns++
				return &reentryGreeter{sent: true}, nil
			},
		}),
		durable.WithEffectHandler[string, string, svcCtx](func(context.Context, string, state.Effect) error {
			p.dispatched++
			return nil
		}),
	}
}

// TestRunService_UndecodableResult_SettlesOnDoneBeforeFailing pins what a ServiceFn
// result whose JSON form won't decode does today, and one with no JSON form at all,
// which takes the same path on the raw result. The live settle runs the onDone
// transition on a stand-in for the result: its Assign runs once and sees that
// stand-in, an unexported durable type, and the state it enters spawns its actor.
// Only then does recording refuse the result, so RunService fails the Handle with
// ErrHandleFailed, records nothing and dispatches nothing. The onError Assign never
// runs. A recovered Handle has the service in flight again and doesn't spawn the
// actor, and running the service again fails the same way. A reducer that asserts
// its event's type unchecked panics on the stand-in; the kernel turns that panic into
// the settle's Fire error, so the Fire doesn't apply, nothing spawns, and RunService
// still returns ErrHandleFailed rather than panicking.
func TestRunService_UndecodableResult_SettlesOnDoneBeforeFailing(t *testing.T) {
	ctx := context.Background()
	results := map[string]struct {
		result   any
		seenType string
	}{
		"json.Number":        {json.Number("1e400"), "durable.undecodableResult"},
		"custom MarshalJSON": {outOfRangeResult{}, "durable.undecodableResult"},
		"json.RawMessage":    {json.RawMessage("1e400"), "durable.undecodableResult"},
		// A result with no JSON form at all takes the same path, on the raw result.
		"won't encode": {make(chan int), "chan int"},
	}
	id := state.InvokeID("undecodable", "loading", 0)
	for name, tc := range results {
		t.Run(name, func(t *testing.T) {
			fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) { return tc.result, nil }
			p := &undecodableProbe{}
			m := undecodableMachine(fn, p, false)
			st := durable.NewMemStore()
			opts := undecodableOptions(fn, p)
			h, err := durable.NewRunner(m, st, opts...).Start(ctx, "i1", svcCtx{}, state.WithInitialState("loading"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, _, err = h.RunService(ctx, id); !errors.Is(err, durable.ErrHandleFailed) {
				t.Fatalf("RunService = %v, want ErrHandleFailed", err)
			}
			if p.doneAssigns != 1 || p.doneEventType != tc.seenType || p.errorAssigns != 0 {
				t.Fatalf("onDone Assign ran %d times on %s, onError Assign %d times; want onDone once on %s, onError never",
					p.doneAssigns, p.doneEventType, p.errorAssigns, tc.seenType)
			}
			if p.spawns != 1 || p.dispatched != 0 {
				t.Fatalf("child spawned %d times, %d effects dispatched; want the onDone state's actor spawned once and nothing dispatched", p.spawns, p.dispatched)
			}
			if _, recs, lerr := st.Load(ctx, "i1"); lerr != nil || len(recs) != 0 {
				t.Fatalf("journal holds %d Records (err %v), want none", len(recs), lerr)
			}
			recovered, err := durable.Recover(ctx, m, st, "i1", opts...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if got := recovered.Instance().Current(); got != "loading" || p.spawns != 1 {
				t.Fatalf("recovered in %q, child spawned %d times; want loading, with no new spawn", got, p.spawns)
			}
			if _, _, err = recovered.RunService(ctx, id); !errors.Is(err, durable.ErrHandleFailed) {
				t.Fatalf("RunService after Recover = %v, want ErrHandleFailed again", err)
			}
			if p.doneAssigns != 2 || p.spawns != 2 {
				t.Fatalf("after the retry: onDone Assign ran %d times, child spawned %d; want 2 and 2", p.doneAssigns, p.spawns)
			}
		})
	}
	t.Run("unchecked assertion refuses the Fire", func(t *testing.T) {
		fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) { return json.Number("1e400"), nil }
		p := &undecodableProbe{}
		m := undecodableMachine(fn, p, true)
		st := durable.NewMemStore()
		opts := undecodableOptions(fn, p)
		h, err := durable.NewRunner(m, st, opts...).Start(ctx, "i1", svcCtx{}, state.WithInitialState("loading"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		var panicked any
		var res state.FireResult[string]
		func() {
			defer func() { panicked = recover() }()
			res, _, err = h.RunService(ctx, id)
		}()
		if panicked != nil || !errors.Is(err, durable.ErrHandleFailed) {
			t.Fatalf("RunService panicked %v, returned %v; want ErrHandleFailed and no panic", panicked, err)
		}
		if res.Err == nil || p.doneAssigns != 1 || p.spawns != 0 || p.dispatched != 0 {
			t.Fatalf("settle's Fire error %v, onDone Assign ran %d times, child spawned %d, %d dispatched; want the Fire refused for the panic after one Assign, and no spawn or dispatch",
				res.Err, p.doneAssigns, p.spawns, p.dispatched)
		}
		if got := h.Instance().Current(); got != "loading" {
			t.Fatalf("live instance in %q, want loading: the refused Fire didn't apply", got)
		}
		if _, recs, lerr := st.Load(ctx, "i1"); lerr != nil || len(recs) != 0 {
			t.Fatalf("journal holds %d Records (err %v), want none", len(recs), lerr)
		}
	})
}
