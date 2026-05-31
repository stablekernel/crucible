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
// Later capabilities build on this proven, durable, distributed core — adding
// observation — each layered on as an additive addition without disturbing the proof.
package dispatch
