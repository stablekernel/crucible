// Package verify checks behavioral properties of a Quenched Crucible state
// machine and returns, for every property it decides, a witness: the concrete
// event sequence that proves or refutes the claim.
//
// # Stability
//
// This package ships in v1.0 but is ADVISORY. Its API surface and finding shapes
// (the [Finding] struct, [FindingKind] set, and option constructors) are NOT
// covered by the frozen-contract guarantee and may change in a minor release.
// Treat its verdicts as diagnostics that inform review, not as a stable contract
// to build automation against without pinning a version.
//
// # Overview
//
// Where [github.com/stablekernel/crucible/state/analysis] reports structural
// defects (unreachable states, dead transitions, nondeterminism) as a flat
// catalog, verify answers questions a caller poses about reachability, safety,
// liveness, configuration invariants, bounded simulation, scenario coverage, and
// covering-suite generation. It is the property-checking layer that sits on top
// of the analysis graph primitives rather than a second copy of them.
//
// # Structural model and fidelity guarantee
//
// Every check in this package reasons over a guard-agnostic structural model of
// the machine's state space. The model is built from the machine's public IR —
// the same representation a JSON-loaded machine produces — so no instance is cast
// and no guard or action is evaluated at check time.
//
// Guard-agnostic means: the model assumes guards pass. A guard can only ever
// prune a transition at run time, never add one, so the structural model is a
// superset of any real machine's execution space. As a result:
//
//   - A state the model calls reachable is reachable in some run (the witness
//     fires events that drive an instance there when guards cooperate).
//   - An unreachable verdict means the model finds no structural path, which means
//     no guard assignment can make it reachable in any run.
//
// The model's reachability verdict is cross-checked against the [analysis]
// package's authoritative [analysis.KindUnreachableState] pass: verify consumes
// that proven set rather than re-deriving it, so both packages agree on which
// states are reachable and the two layers are consistent by construction.
//
// # The property checks
//
// [Verify] takes a [state.Machine] and a tail of functional [Option] values that
// select which properties to check. Each decided property becomes a [Finding].
// With no options, Verify checks reachability of every declared state.
//
// [Reachable] restricts the reachability check to named target states. With no
// [Reachable] option, Verify checks every declared state.
//
// [ReachAvoiding] adds a conditional-reachability check — "reach X along some
// run that never passes through Y". The search prunes every configuration whose
// active states intersect the avoid-set, honoring hierarchy: a configuration is
// "in Y" when any active leaf, enclosing ancestor, or initial-descent member is
// Y, so avoiding a region leaf, a superstate, or a sibling initial-descent state
// each forbids the whole configuration it belongs to. A satisfiable finding
// carries the witnessing event sequence; an unsatisfiable one carries the zero
// Witness.
//
// [AlwaysEventually] adds a liveness check — from every reachable configuration,
// the target is always eventually reachable (the CTL eventuality AG EF target).
// The check is answered by reverse reachability from the target: a reachable
// configuration from which the target can never be reached is a counterexample.
// A holding verdict (Reachable true) carries the zero Witness; a failing one
// carries the route to the nearest stuck configuration — a target-free terminal
// or a node in a target-free cycle — from which the target can never be reached.
//
// [CheckInvariant] adds configuration invariants — predicates over the
// active-state configuration that must hold in every reachable configuration.
// Build invariants with [MutualExclusion], [Implies], or [NeverActive]. The
// exploration models the full set of co-active leaves over the
// configuration-product space, so orthogonal parallel regions advancing
// independently are modeled faithfully. A violation carries the shortest route
// to the nearest configuration that breaks the predicate; its Witness.Target
// names that configuration as a '|'-joined list of active leaves.
//
// [SimulateBounded] adds bounded exhaustive simulation — it enumerates the
// configurations reachable within a depth bound over the same
// configuration-product space (the distinct configurations, not every distinct
// event trace), evaluates a caller-supplied [Oracle] once per configuration — on
// the BFS-shortest path that first reaches it — and reports the shortest such path
// to a configuration the oracle rejects. Because each configuration is evaluated
// once on its shortest discovery path, a longer trace that revisits the same
// configuration is not separately oracle-checked; the guarantee is over reachable
// configurations within the bound, not over every trace. A violation is real and
// replayable; a clean run is a bounded guarantee only (see Caveats below).
//
// [Coverage] adds structural-coverage analysis — it replays a set of scenarios
// (each an ordered event sequence) over the configuration-product explorer and
// reports which reachable states and transitions they exercise against the
// reachable universe. The full breakdown is read with [Result.Coverage]; the
// accompanying [Finding]'s Reachable field is true exactly when nothing is left
// uncovered. An event that names no enabled transition from the current
// configuration is a clean no-op, mirroring a kernel Fire of an unhandled event.
//
// # Covering-suite generation
//
// Alongside the property checkers, [CoveringSuite] is a producer: it generates a
// set of typed event sequences that together exercise every reachable state and
// transition, walking the same configuration-product explorer greedily until
// nothing reachable is left uncovered. Feeding its output back into [Coverage]
// reports 100% coverage of the reachable universe — that round-trip is the suite's
// guarantee. Note this is a STRUCTURAL guarantee, scoped to the same guard-agnostic
// configuration model both the generator and [Coverage] walk: it says the suite
// covers every reachable state and transition in that model, NOT that replaying the
// suite end-to-end through a live instance exercises them, since a real guard may
// block a transition the structural model assumes passes. The suite is
// deterministic and stable across runs; it is a
// covering suite, not a provably minimal one (the generator favors a small,
// deterministic result over minimum cardinality). [MaxScenarioLength] caps each
// scenario's event count, splitting coverage across more, shorter sequences.
//
// # Witnesses and conformance cross-check
//
// A witness is an [analysis.Path] whose Events are the event sequence a driver
// fires to drive an instance from the initial configuration to the target. For
// reachability and conditional reachability, the witness proves the property: a
// test suite can replay it through the conformance harness and confirm the
// instance lands in the target state. For liveness and invariant violations, the
// witness is a counterexample: replaying it drives an instance into the stuck or
// violating configuration, making the defect tangible and drivable.
//
// This cross-check discipline is the package's reproducibility contract: every
// non-trivial witness produced by verify has been confirmed replayable by the
// package's own conformance cross-check tests, tying the static claim to the
// kernel's executable semantics.
//
// # Caveats
//
// Guard-agnostic analysis is a structural over-approximation. The model is
// deliberately wider than any guarded execution, so it can only prove that a
// property holds structurally or that a structural path exists — it cannot
// account for guard predicates narrowing the real run space. In practice:
//
//   - A reachable verdict means "reachable if the guards cooperate". If a guard
//     in the real machine always blocks the path, the state is structurally but
//     not practically reachable; that is a modeling concern, not a verify defect.
//   - An unreachable verdict is exact: no run can reach the state regardless of
//     guard values.
//
// Bounded simulation is NOT a proof of absence. A holding [SimulateBounded]
// verdict ("no violation within the bound") guarantees only that the oracle held
// across every configuration reachable in at most the given number of events. A
// violation may still exist in a longer trace. A violation it does report is
// structural — it reaches the rejected configuration assuming guards cooperate, so
// it may be infeasible in the real machine if a guard always blocks the route. For
// an exact, unbounded HOLDING verdict over fixed structural predicates, use
// [CheckInvariant] instead.
//
// The holds/violation asymmetry applies to liveness and invariant checks too. Only
// the HOLDING verdict is exact: [CheckInvariant] reporting the predicate holds, and
// [AlwaysEventually] reporting the target is always eventually reachable, are both
// exact over the guard-agnostic model — no guard assignment can break them. A
// reported VIOLATION (an invariant counterexample or a liveness stuck route),
// however, is structural and may be guard-infeasible: it identifies a configuration
// the model can reach assuming guards pass, which a real guard may render
// unreachable. So treat a holding verdict as a proof and a violation as a candidate
// to confirm by replaying its witness through the conformance harness.
//
// Configuration invariants and all other checks are configuration-level —
// predicates over the set of active states. Context-value or symbolic reasoning
// (predicates over the runtime context type C, guard expressions, or data-flow
// properties) is a separate capability not provided by this package.
//
// # Usage
//
//	result := verify.Verify(machine,
//	    verify.Reachable("shipped"),
//	    verify.AlwaysEventually("delivered"),
//	    verify.CheckInvariant(verify.MutualExclusion("held", "paid")),
//	    verify.SimulateBounded("never-shipped", 5, myOracle),
//	    verify.Coverage([]string{"pay", "ship"}),
//	)
//	for _, f := range result.Findings {
//	    fmt.Println(f.Kind, f.State, f.Reachable)
//	}
//
// [Verify] never returns nil and never panics: a machine whose IR cannot be read
// yields an empty result rather than an error, honoring the kernel's no-panic
// contract for read-only inspection.
//
// The package imports only [state], the analysis package, and the standard
// library, preserving the kernel's stdlib-only dependency stance. The API is
// designed to grow additively: new property checks arrive as new [Option]
// constructors and new [FindingKind] values without changing the signatures
// callers already depend on.
package verify
