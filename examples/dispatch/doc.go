// Package dispatch is the flagship showcase for the Crucible suite: it takes the
// food-delivery order saga — a rich statechart with hierarchy, parallel regions,
// actors, invoked services, timed deadlines, and a compensation saga — and runs it
// under the full Crucible runtime, growing capability by capability.
//
// This first capability proves the machine. Before a single order is dispatched,
// [Prove] establishes the saga is well-formed: it verifies that the key lifecycle
// stages are reachable, that the Watchdog region's on-time and overdue leaves are
// mutually exclusive, that no transition guard is a contradictory dead branch, and
// that the analyzer finds no nondeterministic competing transitions it can rule out.
// The result is a [ProofReport] a host can assert on at startup, in a test, or in a
// release gate.
//
// The next capability runs the proven saga durably. [RunCrashRecovery] drives an
// order to its live Active fulfillment configuration under the durable runtime backed
// by an on-disk store, simulates a process crash, and reconstructs the order from the
// store alone — its state, payment hold, and folded log intact — then drives it on to
// Delivered. [RunTimeTravel] records the same happy path through a history-retaining
// store and reconstructs the order's state at an earlier point in its lifecycle,
// read-only. Both reuse the saga wholesale — its model, payment services, and
// kitchen/courier actor behaviors — driven through the durable Handle API.
//
// The next capability runs the proven fulfillment actors across a cluster, over real
// gRPC. [RunDistributedFulfillment] hosts the same kitchen and courier behaviors the
// durable runtime runs in-process — [fooddelivery.KitchenBehavior] and
// [fooddelivery.CourierBehavior] — as remote cluster actors on separate worker nodes,
// dispatched from a coordinator node: the coordinator spawns the kitchen on one worker
// and the courier on another over the wire, a worker-side supervisor restarts a crashed
// kitchen actor within budget, and the coordinator then delivers the
// [fooddelivery.KitchenCook] / [fooddelivery.CourierDrive] signals across the wire to
// drive each remote actor to completion — proving the fulfillment actors are
// location-transparent and survive a worker-side failure. The gRPC transport is carried
// in-memory by a bufconn listener so the whole cluster runs inside one process.
//
// The next capability proves the saga's admission guard is polyglot. The order
// machine's "generous order" guard — the predicate subtotal + tip >= 6000 — is named,
// registry-bound, and engine-agnostic ([fooddelivery.GenerousGuardName]), so the engine
// that computes it can be swapped through [fooddelivery.WithGenerousGuard] without
// touching the machine. [RunPolyglotEquivalence] builds two models that differ only in
// that engine — the default CEL model and a model whose guard is a WebAssembly guest
// compiled to wasip1/wasm and run through wazero — and drives both through the Authorized
// decision across orders chosen to isolate the generous branch (non-fast-lane and below
// the expedite threshold, so only the generous guard can admit). One generous order is
// admitted by both engines and one frugal order is blocked by both; because the run
// exercises both verdicts, the resulting [PolyglotReport.Equivalent] is meaningful proof
// the WebAssembly guard and the CEL guard decide the predicate identically.
//
// The final capability observes the proven, durable saga through Crucible's
// vendor-neutral telemetry seam. [RunObservedSaga] drives the order to Delivered under
// the durable runtime and, for every transition, opens an "order.transition" span and
// increments an "order.transitions" counter — each tagged with the from/to stage — so
// the emitted telemetry narrates the order's path. There is no kernel hook into the
// state machine; the host wraps its own drive calls. Telemetry arrives as an injected
// [telemetry.Provider], so a host wires an slog, otel, or datadog adapter while the
// silent [telemetry.Nop] default runs the saga allocation-free; the function returns an
// [ObservedReport] of the observed facts so the run is verifiable from its return value.
//
// The capstone test ties the whole story together: it runs the same order machine
// through all five capabilities in sequence — proven, durable, distributed, polyglot,
// observed — asserting each stage's headline result, so the showcase reads as a single
// narrative proving one machine runs proven, durable, distributed, polyglot, and
// observed.
package dispatch
