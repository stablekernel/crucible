package state

// This file declares the functional-option types for every public constructor
// and operation, per the kernel's functional-options convention. Each is an
// opaque type so new knobs arrive as new options, never as signature changes.
//
// The option constructors here are part of the v1 surface; their internal
// effect is applied by the (not-yet-implemented) kernel.

// ForgeOption configures Forge.
type ForgeOption func(*forgeConfig)

type forgeConfig struct{}

// QuenchOption configures Quench.
type QuenchOption func(*quenchConfig)

type quenchConfig struct{ strict bool }

// Strict makes Quench reject any lint warning, not just hard errors.
func Strict() QuenchOption { return func(c *quenchConfig) { c.strict = true } }

// CastOption configures Cast.
type CastOption[S comparable] func(*castConfig[S])

type castConfig[S comparable] struct {
	initial    S
	hasInitial bool
	clock      Clock
}

// WithClock injects the time seam an instance's delayed-transition driver uses.
// It is consumed only by a Scheduler / host driver wired to the instance — never
// by the pure Fire step, which neither reads a clock nor sleeps. Supply
// SystemClock() in production or a fake clock in a test to drive `after`
// transitions deterministically. When omitted, an instance defaults to
// SystemClock().
func WithClock[S comparable](c Clock) CastOption[S] {
	return func(cfg *castConfig[S]) { cfg.clock = c }
}

// WithInitialState supplies the instance's starting state explicitly. Use it
// when the machine declares no CurrentStateFn (i.e. the current state cannot be
// derived from the entity). When both are present, the explicit initial state
// takes precedence over CurrentStateFn.
func WithInitialState[S comparable](s S) CastOption[S] {
	return func(c *castConfig[S]) {
		c.initial = s
		c.hasInitial = true
	}
}

// FireOption configures Fire / FireSeq / FireEach.
type FireOption func(*fireConfig)

type fireConfig struct{ collectAll bool }

// CollectAll makes a batch fire run every step and gather all errors instead of
// stopping at the first.
func CollectAll() FireOption { return func(c *fireConfig) { c.collectAll = true } }

// AssayOption configures Assay.
type AssayOption func(*assayConfig)

type assayConfig struct{ aggregate bool }

// WithAggregate makes Assay collect all failing requirements in one pass.
func WithAggregate() AssayOption { return func(c *assayConfig) { c.aggregate = true } }

// PlanOption configures PlanPath.
type PlanOption func(*planConfig)

type planConfig struct{}

// LoadOption configures LoadFromJSON.
type LoadOption func(*loadConfig)

type loadConfig struct{}

// ProvideOption configures Provide.
type ProvideOption func(*provideConfig)

type provideConfig struct{}

// ToJSONOption configures ToJSON.
type ToJSONOption func(*toJSONConfig)

type toJSONConfig struct{ withoutSrcPos bool }

// WithoutSrcPos omits the diagnostic source-position fields (srcFile/srcLine)
// from the serialized IR. Source positions are captured from the builder via
// runtime.Caller, so they carry the absolute filesystem path of the worktree
// that authored the machine — which makes them non-portable across checkouts.
// They are diagnostic-only metadata ("defined at machine.go:84" tooltips) and
// have no effect on loading or behavior, so stripping them yields a stable,
// position-independent serialization. Use it for committed goldens and any
// interchange that must be byte-identical regardless of where it was generated.
func WithoutSrcPos() ToJSONOption {
	return func(c *toJSONConfig) { c.withoutSrcPos = true }
}

// TemperOption configures Temper.
type TemperOption func(*temperConfig)

type temperConfig struct{}

// VizOption configures the ToMermaid and ToDOT renderers.
type VizOption func(*vizConfig)

// vizConfig holds the resolved rendering knobs. Defaults render guard
// annotations and owner color-coding; direction defaults differ per format and
// are applied by the renderer when dirSet is false.
type vizConfig struct {
	hideGuards bool
	hideOwners bool
	leftRight  bool
	dirSet     bool
}

// WithoutGuards omits the bracketed guard annotations from transition labels.
func WithoutGuards() VizOption { return func(c *vizConfig) { c.hideGuards = true } }

// WithoutOwners omits owner color-coding (Mermaid classDef / DOT fillcolor).
func WithoutOwners() VizOption { return func(c *vizConfig) { c.hideOwners = true } }

// LeftToRight lays the diagram out left-to-right (Mermaid direction LR, DOT
// rankdir=LR).
func LeftToRight() VizOption {
	return func(c *vizConfig) {
		c.leftRight = true
		c.dirSet = true
	}
}

// TopToBottom lays the diagram out top-to-bottom (Mermaid default, DOT
// rankdir=TB).
func TopToBottom() VizOption {
	return func(c *vizConfig) {
		c.leftRight = false
		c.dirSet = true
	}
}
