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
// Later capabilities build on this proven core — running the saga durably, across a
// cluster, over a transport, and under observation — each layered on as an additive
// addition without disturbing the proof.
package dispatch
