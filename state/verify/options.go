package verify

// Option configures a [Verify] pass. Options compose left to right; with no
// options Verify checks reachability of every declared state. The option set is
// designed to grow additively: new property checks arrive as new Option
// constructors without changing the [Verify] signature.
type Option func(*config)

// config is the accumulated configuration of a Verify pass.
type config struct {
	// targets restricts the reachability check to these state names; nil or empty
	// means check every declared state.
	targets map[string]bool
	// reachAvoiding holds the requested conditional-reachability queries: for each
	// target, the set of states a witnessing run must never pass through.
	reachAvoiding []avoidQuery
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
