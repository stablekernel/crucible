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
}

// TestAssignBinding_Shadow_MatchesDirectReducer asserts the in-process assign
// binding, evaluated through the serialized ContextView path, returns the same
// next context as calling the AssignFn directly. It is the write-side gate
// symmetric to the read-side guard/action shadow tests: AssignResult.Context is
// the realized channel the action binding formerly reserved.
func TestAssignBinding_Shadow_MatchesDirectReducer(t *testing.T) {
	reducer := func(in AssignCtx[bindOrder]) bindOrder {
		c := in.Entity
		c.Amount += 10
		return c
	}
	in := bindOrder{Amount: 5}
	direct := reducer(AssignCtx[bindOrder]{Entity: in})

	binding := inProcessAssign(reducer)
	raw, err := newInProcessView(in).JSON()
	if err != nil {
		t.Fatalf("view JSON: %v", err)
	}
	var decoded bindOrder
	if err = json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	res, err := binding.EvalAssign(context.Background(), AssignRequest[bindOrder]{
		Name:    "credit",
		Context: newInProcessView(decoded),
	})
	if err != nil {
		t.Fatalf("EvalAssign: %v", err)
	}
	if res.Context != direct {
		t.Fatalf("binding context = %+v, want %+v (direct reducer)", res.Context, direct)
	}
}

// TestRegistry_AssignBindingRecorded asserts Assign registers its in-process
// AssignBinding alongside the bare reducer, namespaced under KindAssign so it
// never collides with a same-named action.
func TestRegistry_AssignBindingRecorded(t *testing.T) {
	reg := NewRegistry[bindOrder]().
		Assign("credit", func(in AssignCtx[bindOrder]) bindOrder { return in.Entity })
	if reg.assignBinding("credit") == nil {
		t.Fatal("Assign did not record an in-process AssignBinding")
	}
	if reg.assignBinding("missing") != nil {
		t.Fatal("assignBinding returned a binding for an unregistered name")
	}
}

// TestEvalAssign_UnboundRefFailsClosed asserts the defensive fire-time guard:
// an assign ref with no bound reducer surfaces as a typed *ErrAssignPanic and
// leaves the context unchanged, rather than silently dropping the fold.
func TestEvalAssign_UnboundRefFailsClosed(t *testing.T) {
	m := Forge[string, string, bindOrder]("x").State("a").Initial("a").Quench()
	next, err := m.evalAssign(Ref{Name: "ghost"}, bindOrder{Amount: 3}, nil)
	if err == nil {
		t.Fatal("unbound assign ref should fail")
	}
	var ap *ErrAssignPanic
	if !errors.As(err, &ap) || ap.AssignName != "ghost" {
		t.Fatalf("error = %v, want *ErrAssignPanic{ghost}", err)
	}
	if next.Amount != 3 {
		t.Fatalf("context changed on unbound assign: %+v", next)
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

// stubGuardBinding is a test GuardBinding that reads the live entity off the
// in-process view and applies a Go predicate, optionally returning an error to
// exercise the error-to-false adaptation BindGuard performs.
type stubGuardBinding struct {
	pred func(bindOrder) bool
	err  error
}

// EvalGuard satisfies GuardBinding[bindOrder] for the stub.
func (b stubGuardBinding) EvalGuard(_ context.Context, req GuardRequest[bindOrder]) (GuardResult, error) {
	if b.err != nil {
		return GuardResult{}, b.err
	}
	ent, _ := req.Context.Raw().(bindOrder)
	return GuardResult{OK: b.pred(ent)}, nil
}

// TestBindGuard_RegistersFireResolvableGuard asserts BindGuard registers a guard
// that the fire-time path resolves and evaluates from the bare-func map, with the
// binding's verdict carried through, AND records the binding on the parallel seam.
func TestBindGuard_RegistersFireResolvableGuard(t *testing.T) {
	reg := NewRegistry[bindOrder]()
	reg.BindGuard("rich", stubGuardBinding{pred: func(o bindOrder) bool { return o.Amount >= 10 }})

	fn, found := reg.guards["rich"]
	if !found {
		t.Fatal("BindGuard did not register a fire-resolvable GuardFn")
	}
	if reg.guardBinding("rich") == nil {
		t.Fatal("BindGuard did not record the binding on the parallel seam")
	}
	if !fn(GuardCtx[bindOrder]{Entity: bindOrder{Amount: 10}}) {
		t.Fatal("guard should pass for amount=10")
	}
	if fn(GuardCtx[bindOrder]{Entity: bindOrder{Amount: 9}}) {
		t.Fatal("guard should fail for amount=9")
	}
}

// TestBindGuard_BindingErrorYieldsFalse asserts a binding that returns an error is
// adapted to a non-transitioning false verdict, matching how a Go guard that cannot
// decide does not enable a transition.
func TestBindGuard_BindingErrorYieldsFalse(t *testing.T) {
	reg := NewRegistry[bindOrder]()
	reg.BindGuard("boom", stubGuardBinding{err: errTest})
	if reg.guards["boom"](GuardCtx[bindOrder]{Entity: bindOrder{Amount: 100}}) {
		t.Fatal("a binding error must yield false, not pass")
	}
}

// TestBindGuard_DrivesTransitionThroughFire asserts a guard registered via
// BindGuard enables (or blocks) a transition when fired, proving the binding flows
// through the same fire-time guard path as a Go-func guard.
func TestBindGuard_DrivesTransitionThroughFire(t *testing.T) {
	build := func(amount int) *Instance[string, string, bindOrder] {
		b := Forge[string, string, bindOrder]("bg")
		b.reg.BindGuard("highAmount", stubGuardBinding{pred: func(o bindOrder) bool { return o.Amount >= 50 }})
		m := b.
			State("from").
			Transition("from").On("go").GoTo("to").When("highAmount").
			State("to").
			Initial("from").
			Quench()
		return m.Cast(bindOrder{Amount: amount}, WithInitialState("from"))
	}

	pass := build(50)
	pass.Fire(context.Background(), "go")
	if pass.Current() != "to" {
		t.Fatalf("amount=50 should transition; current=%v", pass.Current())
	}

	block := build(49)
	block.Fire(context.Background(), "go")
	if block.Current() != "from" {
		t.Fatalf("amount=49 should block; current=%v", block.Current())
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
