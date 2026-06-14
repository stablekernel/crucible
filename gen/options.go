package gen

// config holds the resolved settings for an Eject call. It is unexported; callers
// shape it through the functional Option tail so new knobs arrive additively
// without breaking the Eject signature.
type config struct {
	packageName     string
	contextTypeName string
}

// defaultConfig returns the baseline configuration applied before any Option.
func defaultConfig() config {
	return config{
		packageName:     "machine",
		contextTypeName: "Context",
	}
}

// Option configures an Eject call. Options form an additive variadic tail on
// Eject; required inputs stay positional and new capabilities arrive as new
// Options, never as changes to existing signatures.
type Option func(*config)

// WithPackageName sets the package clause of the generated file. The default is
// "machine". An empty name is ignored, leaving the default in place.
func WithPackageName(name string) Option {
	return func(c *config) {
		if name != "" {
			c.packageName = name
		}
	}
}

// WithContextTypeName sets the name of the generated context type — the type
// substituted wherever the engine signatures reference the machine's context type
// parameter. The default is "Context". An empty name is ignored.
func WithContextTypeName(name string) Option {
	return func(c *config) {
		if name != "" {
			c.contextTypeName = name
		}
	}
}
