// Package verify checks behavioral properties of a Quenched Crucible state
// machine and returns, for every property it decides, a witness: the concrete
// event sequence that proves or refutes the claim.
//
// Where [github.com/stablekernel/crucible/state/analysis] reports structural
// defects (unreachable states, dead transitions, nondeterminism) as a flat
// catalog, verify answers questions a caller poses about reachability and,
// in later additions, safety and liveness. It is the property-checking layer
// that sits on top of the analysis graph primitives rather than a second copy
// of them: the reachable-state space is explored with the same guard-agnostic
// breadth-first walk the analysis package already exposes through
// [github.com/stablekernel/crucible/state/analysis.ShortestPaths], so a state
// verify calls reachable is reachable in some run of the machine, and the
// witness verify hands back is the minimal event sequence that gets there.
//
// # Usage
//
//	result := verify.Verify(machine, verify.Reachable("shipped"))
//	for _, f := range result.Findings {
//		fmt.Printf("%s %s: %s\n", f.Kind, f.State, f.Witness.Events())
//	}
//
// [Verify] takes a [state.Machine] built either by the Forge DSL or loaded from
// JSON, and a tail of functional [Option] values that select which properties
// to check. With no options it checks reachability of every declared state,
// the foundational property the rest build on. [ReachAvoiding] adds a
// conditional-reachability (safety) check — "reach X along some run that never
// passes through Y" — answered by a witness-carrying constrained search that
// prunes any configuration whose active states intersect the avoid-set.
// [AlwaysEventually] adds a liveness check — "from every reachable
// configuration, Z is always eventually reachable" (the CTL eventuality
// AG EF Z) — answered by reverse reachability from Z: a reachable configuration
// from which Z can never be reached is a counterexample, a configuration parked
// in a Z-free terminal or cycle, and the finding carries the route to it.
// [CheckInvariant] adds configuration invariants — predicates over the
// active-state configuration that must hold in every reachable configuration,
// built with [MutualExclusion], [Implies], or [NeverActive]. Invariants are
// decided by a configuration-product exploration that tracks the full set of
// co-active leaves (so orthogonal parallel regions advancing independently are
// modeled faithfully); a violation carries the shortest route to the nearest
// configuration that breaks the predicate, whose Target names that configuration.
// [SimulateBounded] adds bounded exhaustive simulation — it enumerates the
// machine's event sequences up to a depth bound over that same
// configuration-product space, evaluates a caller-supplied [Oracle] at every
// reached configuration, and reports the shortest trace whose configuration the
// oracle rejects. Unlike the other checks it is bounded, not exact: "no violation
// within the bound" guarantees only that the oracle held across the configurations
// reachable in at most the given number of events, never that the property holds
// in every run — a violation may still exist in a longer trace. A violation it
// does report, by contrast, is real and replayable.
// [Coverage] adds structural-coverage analysis — it replays a set of scenarios
// (each an ordered event sequence) over that same configuration-product explorer
// and reports which reachable states and transitions they exercise against the
// reachable universe, with the concrete uncovered remainder and the coverage
// fractions. Read the breakdown with [Result.Coverage]. The metric is consistent
// with the other checks: each scenario drives the configuration along the same
// structural edges the explorer follows, and an event that names no enabled
// transition from the current configuration is a clean no-op — so an uncovered
// state or transition is a real gap a scenario set leaves unexercised, the input a
// CI gate uses to fail an under-tested suite.
// Each decided property becomes a [Finding]; a finding that holds carries a
// [Witness] — an [github.com/stablekernel/crucible/state/analysis.Path] whose
// Events are the sequence a driver fires to drive an instance from the initial
// state to the target.
//
// Alongside the property checkers, [CoveringSuite] is a producer: it generates a
// set of typed event sequences that together exercise every reachable state and
// transition, walking the same configuration-product explorer greedily until
// nothing reachable is left uncovered. Feeding its output back into [Coverage]
// reports full coverage of the reachable universe — that round-trip is its
// guarantee. It is a covering suite, not a provably minimal one: the generator
// favors a small, deterministic suite over a minimum-cardinality one, which is a
// harder optimization it does not attempt. [MaxScenarioLength] caps each
// scenario's length, splitting coverage across more, shorter sequences.
//
// The checks are purely static in the same sense as the analysis package: no
// instance is cast, no event is fired, no guard or action is evaluated. A guard
// can only ever prune an edge at run time, never add one, so a state proven
// reachable here is reachable in some run, and an unreachable verdict is exact.
//
// The package imports only [state], the analysis package, and the standard
// library, preserving the kernel's stdlib-only dependency stance. The API is
// designed to grow additively: new property checks arrive as new [Option]
// constructors and new [FindingKind] values without changing the signatures
// callers already depend on.
package verify
