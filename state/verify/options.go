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
