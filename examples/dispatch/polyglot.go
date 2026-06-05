package dispatch

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/examples/fooddelivery"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/wasm"
)

// This file is the showcase's polyglot-guard capability: it proves the order saga's
// "generous order" admission guard can be evaluated in a foreign engine — a
// WebAssembly module — and remain behaviorally identical to the in-tree CEL guard the
// machine ships with. The same predicate (subtotal + tip ≥ 6000) is authored twice:
// once as the default CEL expression [fooddelivery.GenerousGuardSource], and once as a
// wasip1 guest compiled to WebAssembly and run through wazero. The machine is otherwise
// untouched; only the engine that decides "generous" differs, swapped in through the
// engine-agnostic [fooddelivery.WithGenerousGuard] seam.
//
// [RunPolyglotEquivalence] drives both models through the Authorized decision across a
// set of orders chosen to isolate the generous branch — every order is non-fast-lane
// and below the expedite threshold, so the Core big-basket branch can never admit and
// only the generous guard can. If the CEL and WASM models reach identical outcomes for
// every order — and they include at least one admit and one reject — the two engines
// are proven equivalent on the predicate the saga depends on.

// orderOutcome is whether an order was admitted into the Active fulfillment
// configuration by the Authorized guard, or blocked and left resting in Authorizing.
type orderOutcome int

const (
	// outcomeBlocked is the resting Authorizing stage: the generous guard reported false
	// and the order was not admitted.
	outcomeBlocked orderOutcome = iota
	// outcomeAdmitted is the Cooking stage of the Active fulfillment configuration: the
	// generous guard reported true and admitted the order.
	outcomeAdmitted
)

// String renders an outcome symbolically for the report and Example output.
func (o orderOutcome) String() string {
	if o == outcomeAdmitted {
		return "admitted"
	}
	return "blocked"
}

// PolyglotCase is one order's equivalence result: the order driven, the outcome each
// engine produced, and whether the two engines agreed.
type PolyglotCase struct {
	// Name labels the case for the report and Example output.
	Name string
	// Order is the order both engines decided on.
	Order fooddelivery.Order
	// CEL is the outcome the default CEL-guarded model produced.
	CEL orderOutcome
	// WASM is the outcome the WebAssembly-guarded model produced.
	WASM orderOutcome
	// Agree reports whether CEL and WASM reached the same outcome for this order.
	Agree bool
}

// PolyglotReport is the outcome of [RunPolyglotEquivalence]: the per-order equivalence
// results and the suite-wide verdict. A report whose [PolyglotReport.Equivalent] is true
// is proof that the WebAssembly guard and the CEL guard decide the generous predicate
// identically.
type PolyglotReport struct {
	// Cases lists each order's CEL-vs-WASM outcome, in the order driven.
	Cases []PolyglotCase
	// Equivalent reports whether every case agreed AND the suite exercised both verdicts
	// (at least one admit and one reject), so agreement is meaningful rather than vacuous.
	Equivalent bool
}

// generousIsolatingOrders are the orders the equivalence drives. Each is non-fast-lane
// (priority "standard") and below the expedite threshold (subtotal < 5000), so the Core
// big-basket admission branch can never fire and ONLY the generous guard can admit:
//
//   - "generous" has subtotal+tip = 6500 ≥ 6000, so the generous predicate is true and
//     both engines must admit it.
//   - "frugal" has subtotal+tip = 4500 < 6000, so the predicate is false and both
//     engines must reject it (the order rests in Authorizing).
//
// Driving both branches makes the agreement non-vacuous: the engines are proven to
// agree on a true verdict and a false one, not merely to never admit.
var generousIsolatingOrders = []PolyglotCase{
	{Name: "generous", Order: fooddelivery.Order{Subtotal: 3000, Tip: 3500, Priority: "standard"}},
	{Name: "frugal", Order: fooddelivery.Order{Subtotal: 3000, Tip: 1500, Priority: "standard"}},
}

// polyglotDeps are the model-construction and order-driving seams the
// equivalence harness routes through, so a test can inject a failure to exercise
// the error paths that the real CEL and WASM models — which build and drive
// cleanly — never take. They are carried on a value rather than package globals
// so each test supplies its own, and no two runs share mutable state; that keeps
// the harness safe to drive from parallel sub-tests. Production always uses
// [productionDeps] (the real [fooddelivery.NewModel] and [driveAuthorized]).
type polyglotDeps struct {
	newModel       func(...fooddelivery.Option) (*state.Machine[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order], error)
	driveAuthorize func(context.Context, *state.Machine[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order], fooddelivery.Order) (orderOutcome, error)
}

// productionDeps returns the real seams the exported harness runs with.
func productionDeps() polyglotDeps {
	return polyglotDeps{
		newModel:       fooddelivery.NewModel,
		driveAuthorize: driveAuthorized,
	}
}

// RunPolyglotEquivalence builds two order models that differ only in the engine
// computing the generous-order guard — the default CEL model and a WebAssembly model
// whose guard is the compiled wasmBytes guest — and drives both through the Authorized
// decision across the generous-branch-isolating orders. It returns a [PolyglotReport]
// recording each order's CEL and WASM outcome and whether the two engines agreed
// throughout. It errors if the WASM module fails to compile or build, or if either
// model fails to build or drive — nothing is swallowed.
func RunPolyglotEquivalence(ctx context.Context, wasmBytes []byte) (PolyglotReport, error) {
	return runPolyglotEquivalence(ctx, wasmBytes, productionDeps())
}

// runPolyglotEquivalence is the harness core, parameterized over its seams so a
// test injects fakes through deps without mutating any shared state.
func runPolyglotEquivalence(ctx context.Context, wasmBytes []byte, deps polyglotDeps) (PolyglotReport, error) {
	celModel, err := deps.newModel()
	if err != nil {
		return PolyglotReport{}, fmt.Errorf("dispatch: build CEL model: %w", err)
	}

	mod, err := wasm.Compile(ctx, wasmBytes)
	if err != nil {
		return PolyglotReport{}, fmt.Errorf("dispatch: compile wasm guard: %w", err)
	}
	defer func() { _ = mod.Close(ctx) }()

	wasmModel, err := deps.newModel(fooddelivery.WithGenerousGuard(wasmGenerousGuard(mod)))
	if err != nil {
		return PolyglotReport{}, fmt.Errorf("dispatch: build wasm model: %w", err)
	}

	report := PolyglotReport{Cases: make([]PolyglotCase, 0, len(generousIsolatingOrders))}
	sawAdmit, sawReject, allAgree := false, false, true
	for _, tc := range generousIsolatingOrders {
		celOutcome, err := deps.driveAuthorize(ctx, celModel, tc.Order)
		if err != nil {
			return PolyglotReport{}, fmt.Errorf("dispatch: drive CEL model for %q: %w", tc.Name, err)
		}
		wasmOutcome, err := deps.driveAuthorize(ctx, wasmModel, tc.Order)
		if err != nil {
			return PolyglotReport{}, fmt.Errorf("dispatch: drive wasm model for %q: %w", tc.Name, err)
		}

		tc.CEL, tc.WASM = celOutcome, wasmOutcome
		tc.Agree = celOutcome == wasmOutcome
		report.Cases = append(report.Cases, tc)

		allAgree = allAgree && tc.Agree
		if celOutcome == outcomeAdmitted {
			sawAdmit = true
		} else {
			sawReject = true
		}
	}
	report.Equivalent = allAgree && sawAdmit && sawReject
	return report, nil
}

// wasmGenerousGuard returns a [fooddelivery.GenerousGuardBuilder] that binds the
// compiled WebAssembly module mod as the generous-order guard. It reproduces the
// machine's named, registry-bound guard ([fooddelivery.GenerousGuardName]) with the
// WASM evaluator standing in for the CEL engine, so the order machine resolves and
// composes it exactly as it does the CEL node.
func wasmGenerousGuard(mod *wasm.Module) fooddelivery.GenerousGuardBuilder {
	return func(reg *state.Registry[fooddelivery.Order], _ state.ContextSchema) (state.GuardNode[fooddelivery.Stage], error) {
		return wasm.Guard[fooddelivery.Stage, fooddelivery.Order](reg, fooddelivery.GenerousGuardName, mod), nil
	}
}

// driveAuthorized casts an order instance on model at Placed, fires Submit to arm the
// payment authorization, settles the authorize service with a fixed hold token so the
// Authorized transition's guard is evaluated, and reports whether the order was admitted
// into the Active fulfillment configuration (Cooking) or blocked (resting in
// Authorizing). It drives only the instance and a ServiceRunner — no actors, scheduler,
// or clock — because the Authorized decision is the only behavior under test, so the
// driver isolates the generous guard from the rest of the saga. It errors if no
// authorize service was in flight to settle, so a mis-wired drive fails loudly.
func driveAuthorized(ctx context.Context, model *state.Machine[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order], order fooddelivery.Order) (orderOutcome, error) {
	inst := model.Cast(order, state.WithInitialState(fooddelivery.Placed))
	run := state.NewServiceRunner(inst, fooddelivery.ServiceRegistry())
	run.Absorb(ctx, inst.StartEffects())
	run.Absorb(ctx, inst.Fire(ctx, fooddelivery.Submit).Effects)

	authorizeID := state.InvokeID("order", fooddelivery.Authorizing, 0)
	if _, ok := run.SettleDone(ctx, authorizeID, "polyglot-hold"); !ok {
		return outcomeBlocked, fmt.Errorf("dispatch: no authorize service in flight to settle")
	}

	if inst.Configuration()[0] == fooddelivery.Cooking {
		return outcomeAdmitted, nil
	}
	return outcomeBlocked, nil
}
