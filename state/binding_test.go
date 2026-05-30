package state

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// errTest is a sentinel used by the binding error-propagation tests.
var errTest = errors.New("binding test error")

// bindOrder is the small context type the binding tests project across the seam.
type bindOrder struct {
	Amount int    `json:"amount"`
	Status string `json:"status"`
}

// TestContextView_PassThrough_RawIsLiveEntity asserts the in-process contextView
// is a pass-through to the live entity: Raw returns the exact value handed in,
// without copying or marshaling.
func TestContextView_PassThrough_RawIsLiveEntity(t *testing.T) {
	ent := bindOrder{Amount: 42, Status: "open"}
	view := newInProcessView(ent)

	got, ok := view.Raw().(bindOrder)
	if !ok {
		t.Fatalf("Raw() type = %T, want bindOrder", view.Raw())
	}
	if got != ent {
		t.Fatalf("Raw() = %+v, want %+v", got, ent)
	}
}

// TestContextView_JSON_MatchesCodec asserts the serialized wire form of the
// in-process contextView equals the ContextCodec (default JSON) encoding of the
// entity — the projection an out-of-process binding would consume.
func TestContextView_JSON_MatchesCodec(t *testing.T) {
	ent := bindOrder{Amount: 7, Status: "paid"}
	view := newInProcessView(ent)

	got, err := view.JSON()
	if err != nil {
		t.Fatalf("JSON() error: %v", err)
	}
	want, err := json.Marshal(ent)
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("JSON() = %s, want %s", got, want)
	}
}

// TestGuardBinding_Shadow_JSONMatchesDirect is the polyglot-envelope shadow gate
// for guards: the in-process binding, driven through the serialized contextView
// JSON path, yields the same boolean as invoking the underlying GuardFn directly
// against the live entity. This proves the L3/L4 seam carries enough data before
// any out-of-process transport exists.
func TestGuardBinding_Shadow_JSONMatchesDirect(t *testing.T) {
	fn := func(c GuardCtx[bindOrder]) bool { return c.Entity.Amount >= 10 }
	binding := inProcessGuard(fn)

	cases := []bindOrder{{Amount: 5}, {Amount: 10}, {Amount: 100}}
	for _, ent := range cases {
		direct := fn(GuardCtx[bindOrder]{Entity: ent})

		// Drive the binding through the SERIALIZED view, not the live shortcut:
		// decode the entity back from JSON and feed that into the request.
		raw, err := newInProcessView(ent).JSON()
		if err != nil {
			t.Fatalf("project: %v", err)
		}
		var decoded bindOrder
		if err = json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("decode: %v", err)
		}
		res, err := binding.EvalGuard(context.Background(), GuardRequest[bindOrder]{
			Name:    "minAmount",
			Context: newInProcessView(decoded),
		})
		if err != nil {
			t.Fatalf("EvalGuard: %v", err)
		}
		if res.OK != direct {
			t.Fatalf("amount=%d: binding OK=%v, direct=%v", ent.Amount, res.OK, direct)
		}
	}
}

// TestActionBinding_Shadow_JSONMatchesDirect is the shadow gate for actions: the
// in-process binding driven through the serialized contextView yields the same
// effect as the underlying ActionFn invoked directly.
func TestActionBinding_Shadow_JSONMatchesDirect(t *testing.T) {
	fn := func(c ActionCtx[bindOrder]) (Effect, error) {
		return "charged:" + c.Entity.Status, nil
	}
	binding := inProcessAction(fn)

	ent := bindOrder{Amount: 30, Status: "ready"}
	direct, err := fn(ActionCtx[bindOrder]{Entity: ent})
	if err != nil {
		t.Fatalf("direct: %v", err)
	}

	raw, err := newInProcessView(ent).JSON()
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	var decoded bindOrder
	if err = json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	res, err := binding.EvalAction(context.Background(), ActionRequest[bindOrder]{
		Name:    "charge",
		Context: newInProcessView(decoded),
	})
	if err != nil {
		t.Fatalf("EvalAction: %v", err)
	}
	if len(res.Effects) != 1 || res.Effects[0] != direct {
		t.Fatalf("binding effects=%v, want [%v]", res.Effects, direct)
	}
	// ContextDelta is the reserved channel: in-process actions never populate it.
	if res.ContextDelta != nil {
		t.Fatalf("ContextDelta = %v, want nil (reserved, unused at v1)", res.ContextDelta)
	}
}

// TestActionBinding_Shadow_PropagatesError asserts the binding surfaces the
// underlying ActionFn error rather than swallowing it.
func TestActionBinding_Shadow_PropagatesError(t *testing.T) {
	want := errTest
	binding := inProcessAction(func(ActionCtx[bindOrder]) (Effect, error) { return nil, want })
	_, err := binding.EvalAction(context.Background(), ActionRequest[bindOrder]{Name: "x", Context: newInProcessView(bindOrder{})})
	if err != want {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

// TestServiceBinding_Shadow_MatchesDirect asserts the in-process service binding
// matches the underlying ServiceFn for the same input.
func TestServiceBinding_Shadow_MatchesDirect(t *testing.T) {
	fn := func(_ context.Context, in ServiceCtx[bindOrder]) (any, error) {
		return in.Input["k"], nil
	}
	binding := inProcessService(fn)
	res, err := binding.RunService(context.Background(), ServiceRequest[bindOrder]{
		Name:  "svc",
		Input: map[string]any{"k": "v"},
	})
	if err != nil {
		t.Fatalf("RunService: %v", err)
	}
	if res != "v" {
		t.Fatalf("result = %v, want v", res)
	}
}

// TestRegistry_Sugar_RecordsBindings asserts the public registration sugar
// (Guard/Action/Service) records a parallel binding for every registered name,
// without changing the bare-func fast-path maps.
func TestRegistry_Sugar_RecordsBindings(t *testing.T) {
	reg := NewRegistry[bindOrder]()
	reg.Guard("g", func(GuardCtx[bindOrder]) bool { return true })
	reg.Action("a", func(ActionCtx[bindOrder]) (Effect, error) { return nil, nil })
	reg.Service("s", func(context.Context, ServiceCtx[bindOrder]) (any, error) { return nil, nil })

	// Bare-func fast path still populated (unchanged behavior).
	if _, ok := reg.guards["g"]; !ok {
		t.Fatal("guard func map not populated")
	}
	if _, ok := reg.actions["a"]; !ok {
		t.Fatal("action func map not populated")
	}
	if _, ok := reg.services["s"]; !ok {
		t.Fatal("service func map not populated")
	}

	// Parallel binding map populated in lockstep.
	if reg.guardBinding("g") == nil {
		t.Fatal("guard binding not recorded")
	}
	if reg.actionBinding("a") == nil {
		t.Fatal("action binding not recorded")
	}
	if reg.serviceBinding("s") == nil {
		t.Fatal("service binding not recorded")
	}
	// Unknown names resolve to nil, not a panic.
	if reg.guardBinding("missing") != nil {
		t.Fatal("unknown guard binding should be nil")
	}
}

// TestRegistry_BindingsSurviveProvideQuench asserts a Provide'd / Quench'd machine
// carries its bindings, mirroring the func-map adoption, so the seam is available
// off a rehydrated machine.
func TestRegistry_BindingsSurviveProvideQuench(t *testing.T) {
	reg := NewRegistry[bindOrder]()
	reg.Guard("g", func(GuardCtx[bindOrder]) bool { return true })
	if reg.guardBinding("g") == nil {
		t.Fatal("precondition: guard binding missing")
	}
	// Copy into a fresh registry the way ir.go adopts a host registry, then assert
	// the binding came along.
	dst := NewRegistry[bindOrder]()
	dst.adoptBindings(reg)
	if dst.guardBinding("g") == nil {
		t.Fatal("guard binding not adopted")
	}
}
