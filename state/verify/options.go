package verify

import "fmt"

// Option configures a [Verify] pass. Options compose left to right; with no
// options Verify checks reachability of every declared state. The option set is
// designed to grow additively: new property checks arrive as new Option
// constructors without changing the [Verify] signature.
type Option func(*config)

// config is the accumulated configuration of a Verify pass.
type config struct {
	// coverageRequested records that a [Coverage] option was passed, so an empty
	// scenario set still produces a coverage finding (0% covered) rather than none.
	coverageRequested bool
	// coverageScenarios holds the requested coverage scenarios: each is an event
	// sequence replayed over the configuration-product explorer to record the states
	// and transitions it exercises.
	coverageScenarios [][]string
	// targets restricts the reachability check to these state names; nil or empty
	// means check every declared state.
	targets map[string]bool
	// reachAvoiding holds the requested conditional-reachability queries: for each
	// target, the set of states a witnessing run must never pass through.
	reachAvoiding []avoidQuery
	// alwaysEventually holds the requested liveness targets: for each, check that
	// from every reachable configuration the target is still eventually reachable.
	alwaysEventually []string
	// invariants holds the requested configuration invariants: for each, check that
	// the predicate holds in every reachable configuration.
	invariants []Invariant
	// boundedSims holds the requested bounded-simulation queries: for each, explore
	// every trace up to a depth bound and report the shortest one whose reached
	// configuration the oracle rejects.
	boundedSims []simQuery
}

// avoidQuery is one conditional-reachability request: reach target along a run
// that never enters any state in avoid.
type avoidQuery struct {
	// target is the state to reach.
	target string
	// avoid is the set of states a witnessing run must never pass through.
	avoid map[string]bool
}

// Reachable restricts the reachability check to the named target states. Repeated
// Reachable calls union their targets. A requested name that is not a declared
// state simply yields no finding (verify reports on declared states only). With
// no Reachable option, Verify checks every declared state.
func Reachable(states ...string) Option {
	return func(c *config) {
		if c.targets == nil {
			c.targets = map[string]bool{}
		}
		for _, s := range states {
			c.targets[s] = true
		}
	}
}

// ReachAvoiding adds a conditional-reachability check: is target reachable along
// some run that never passes through any state in avoid? The pass adds one
// [Finding] of [KindConditionalReachability] for target, carrying the witnessing
// event sequence when a clean route exists, or marked unsatisfiable (no witness)
// when every route to target must cross an avoided state.
//
// Avoid membership honors hierarchy: a run "passes through" an avoid state when
// that state is active in any configuration the run visits — as the entered
// state itself, as an enclosing ancestor, or as an initial-descent member of a
// composite or parallel configuration. Avoiding a region leaf, a superstate, or
// a sibling initial-descent state therefore each forbids the whole configuration
// it belongs to.
//
// An empty avoid set makes this plain reachability. A target that is not a
// declared state yields no finding. Repeated ReachAvoiding calls each add their
// own check; the avoid set of a single call is unioned across that call's
// arguments.
func ReachAvoiding(target string, avoid ...string) Option {
	return func(c *config) {
		q := avoidQuery{target: target, avoid: map[string]bool{}}
		for _, a := range avoid {
			q.avoid[a] = true
		}
		c.reachAvoiding = append(c.reachAvoiding, q)
	}
}

// AlwaysEventually adds a liveness check: from every reachable configuration, is
// the target state still eventually reachable? This is the CTL eventuality
// AG EF target — the property "no matter where a run has gone, it can always
// still make progress to target". The pass adds one [Finding] of [KindLiveness]
// for target.
//
// The finding holds (its Reachable field is true, with the zero [Witness]) when
// every reachable configuration retains some run that reaches target. It fails
// (Reachable false) when some reachable configuration can never reach target —
// a configuration parked in a target-free terminal or a target-free cycle. A
// failing finding carries a counterexample witness: the shortest event sequence
// from the initial state to that stuck configuration, whose Target names the
// stuck state. Replaying the witness drives an instance into the trap.
//
// Liveness is exact in the same guard-agnostic sense as reachability: a guard can
// only ever prune an edge at run time, never add one, so a configuration from
// which the structural graph offers no route to target has no run to target in
// any instance, and a holding verdict means every reachable configuration keeps a
// structural route to target. A target that is not a declared state yields no
// finding. Repeated AlwaysEventually calls each add their own check.
func AlwaysEventually(target string) Option {
	return func(c *config) {
		c.alwaysEventually = append(c.alwaysEventually, target)
	}
}

// CheckInvariant adds one or more configuration invariants to the pass: predicates
// over the active-state configuration that must hold in every reachable
// configuration. Build invariants with [MutualExclusion], [Implies], or
// [NeverActive]. The pass adds one [Finding] of [KindInvariant] per invariant,
// keyed by the invariant's [Invariant.Label].
//
// A finding holds (its Reachable field is true, with the zero [Witness]) when the
// predicate is satisfied by every reachable configuration. It fails (Reachable
// false) when some reachable configuration violates the predicate; the finding
// then carries a counterexample witness: the shortest event sequence from the
// initial state to the nearest violating configuration, whose Target names that
// configuration (a '|'-joined list of its active leaves). Replaying the witness
// drives an instance into the violating configuration.
//
// Predicates are over the active configuration of states only — pure structural
// IR, with no runtime context or guard values consulted. Invariant checking is
// exact in the same guard-agnostic sense as reachability: a guard can only ever
// prune an edge at run time, never add one, so a configuration reachable
// structurally is reachable in some run, a structural violation is a real
// reachable violation, and a holding verdict means every reachable configuration
// satisfies the predicate. Repeated CheckInvariant calls each add their
// invariants.
func CheckInvariant(inv Invariant, more ...Invariant) Option {
	return func(c *config) {
		c.invariants = append(c.invariants, inv)
		c.invariants = append(c.invariants, more...)
	}
}

// Coverage adds a structural-coverage analysis: replay a set of scenarios — each
// an ordered event sequence — over the machine and report which reachable states
// and transitions they exercise, against the universe of reachable states and
// transitions the explorer enumerates. The pass adds a single [Finding] of
// [KindCoverage]; read the breakdown with [Result.Coverage].
//
// Each scenario is replayed over the same guard-agnostic configuration-product
// explorer the other property checks reason over, so the coverage metric is
// consistent with verify's model: an event drives the configuration along the
// same structural edge the explorer follows, the states entered are the active
// configurations the run visits, and the transitions fired are the structural
// edges it traverses. An event that names no enabled transition from the current
// configuration is a clean no-op — it advances nothing and is neither an error nor
// counted — exactly as a kernel Fire of an unhandled event is ignored.
//
// The universe a scenario set is measured against is the reachable universe: the
// reachable states (every state active in some reachable configuration) and the
// reachable transitions (every structural edge fired between reachable
// configurations). An empty scenario set is valid and yields 0% coverage with the
// full universe reported uncovered.
//
// Coverage is exact in the same guard-agnostic sense as the other checks: a guard
// can only ever prune an edge at run time, never add one, so a state or transition
// the explorer marks reachable is reachable in some run, and the uncovered set is a
// real structural gap a scenario set leaves unexercised. Repeated Coverage calls
// union their scenarios.
func Coverage[E comparable](scenarios ...[]E) Option {
	return func(c *config) {
		c.coverageRequested = true
		for _, sc := range scenarios {
			seq := make([]string, len(sc))
			for i, e := range sc {
				seq[i] = fmt.Sprint(e)
			}
			c.coverageScenarios = append(c.coverageScenarios, seq)
		}
	}
}

// SimulateBounded adds a bounded exhaustive simulation: enumerate the machine's
// event sequences (traces) up to depth events deep, evaluate the [Oracle] at
// every reached configuration, and report the shortest trace whose configuration
// the oracle rejects. The pass adds one [Finding] of [KindBoundedViolation] keyed
// by label.
//
// The oracle receives the full active configuration — active leaves plus enclosing
// ancestors — and returns whether the property HOLDS there (true acceptable,
// false a violation). The search reuses the configuration-product explorer, so a
// parallel machine's orthogonal regions advance independently and the oracle sees
// true co-active leaf sets. The initial configuration is evaluated at depth 0; a
// depth of 0 therefore checks only the initial configuration, and a negative depth
// is treated as 0.
//
// The finding holds (its Reachable field is true, with the zero [Witness]) when
// the oracle accepts every configuration reachable within the bound. It fails
// (Reachable false) when some reachable configuration is rejected; the finding
// then carries the shortest violating trace — the breadth-first-minimal event
// sequence to that configuration, whose Target names it (a '|'-joined list of its
// active leaves). Replaying the trace drives an instance into the rejected
// configuration, where the oracle genuinely fails.
//
// Bounded simulation is a bounded guarantee, not a proof: "no violation within the
// bound" means only that the oracle held across every configuration reachable in
// at most depth events. A violation may still exist in a longer trace. For an
// exact, unbounded verdict over the fixed structural predicates, use
// [CheckInvariant]. A violation reported here, however, is real and replayable.
// Repeated SimulateBounded calls each add their own check.
func SimulateBounded(label string, depth int, oracle Oracle) Option {
	return func(c *config) {
		c.boundedSims = append(c.boundedSims, simQuery{label: label, depth: depth, oracle: oracle})
	}
}
