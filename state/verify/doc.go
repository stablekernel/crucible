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
// the foundational property the rest build on. Each decided property becomes a
// [Finding]; a finding about a reachable state carries a [Witness] — an
// [github.com/stablekernel/crucible/state/analysis.Path] whose Events are the
// sequence a driver fires to drive an instance from the initial state to the
// target.
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
