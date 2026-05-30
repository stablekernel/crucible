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

// InvokeOption configures a Builder.Invoke declaration.
type InvokeOption func(*invokeConfig)

type invokeConfig struct {
	id       string
	params   map[string]any
	input    map[string]any
	systemID string
}

// WithInput sets the serializable input passed to an invoked service when it
// starts, surfaced as input on the StartService effect.
func WithInput(input map[string]any) InvokeOption {
	return func(c *invokeConfig) { c.input = input }
}

// WithServiceParams sets the serializable params on an invoked service's Src ref,
// available to the bound ServiceFn as ServiceCtx.Params — the per-ref
// configuration knob, distinct from the per-start Input.
func WithServiceParams(params map[string]any) InvokeOption {
	return func(c *invokeConfig) { c.params = params }
}

// WithInvokeID sets an explicit, stable id for an invoked service instead of the
// derived InvokeID. Use it when a host or a Cancel-style coordination needs a
// known id independent of the invocation's declaration order.
func WithInvokeID(id string) InvokeOption {
	return func(c *invokeConfig) { c.id = id }
}

// WithSystemID sets the system-scoped name a child-machine actor (InvokeActor)
// registers under in the ActorSystem (its systemId), so a sibling can
// address it by a well-known name rather than by ref. It is meaningful only for
// InvokeActor; on a plain service Invoke it is ignored.
func WithSystemID(id string) InvokeOption {
	return func(c *invokeConfig) { c.systemID = id }
}

// SpawnOption configures a Builder.Spawn declaration (the dynamic spawn built-in).
type SpawnOption func(*spawnConfig)

type spawnConfig struct {
	input    map[string]any
	systemID string
	onDone   any
	onError  any
}

// WithSpawnInput sets the serializable input passed to a dynamically spawned
// actor when it is created, surfaced as input on the SpawnActor effect.
func WithSpawnInput(input map[string]any) SpawnOption {
	return func(c *spawnConfig) { c.input = input }
}

// WithSpawnSystemID sets the system-scoped name a dynamically spawned actor
// registers under in the ActorSystem (its systemId).
func WithSpawnSystemID(id string) SpawnOption {
	return func(c *spawnConfig) { c.systemID = id }
}

// WithSpawnOnDone sets the event the host re-fires through the parent's Fire when
// a dynamically spawned actor reaches its final state, routing the child's output
// through an ordinary transition from the spawning state. Omit it for a
// fire-and-forget spawn whose completion the parent does not observe.
func WithSpawnOnDone[E comparable](onDone E) SpawnOption {
	return func(c *spawnConfig) { c.onDone = onDone }
}

// WithSpawnOnError sets the event the host re-fires through the parent's Fire when
// a dynamically spawned actor fails, routing the error through an ordinary
// transition from the spawning state.
func WithSpawnOnError[E comparable](onError E) SpawnOption {
	return func(c *spawnConfig) { c.onError = onError }
}

// SendOption configures a Builder.SendTo / Builder.ForwardTo declaration (the
// actor-communication send built-ins).
type SendOption func(*sendConfig)

type sendConfig struct {
	systemID string
}

// WithSendToSystemID addresses the send target by its system-scoped id (the
// `systemId`) instead of its registry id, so a sibling actor is addressed by a
// well-known name. When set it takes precedence over the positional target id.
func WithSendToSystemID(id string) SendOption {
	return func(c *sendConfig) { c.systemID = id }
}

// CastOption configures Cast.
type CastOption[S comparable] func(*castConfig[S])

type castConfig[S comparable] struct {
	initial    S
	hasInitial bool
	clock      Clock
	inspector  Inspector
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

// WithInspector registers a live observer sink fed inspection events as the
// instance advances — event received, transition taken, snapshot update — mirroring
// the live inspection stream. It is off by default: with no
// inspector the instance never calls one, so inspection adds zero overhead and the
// pure Fire step performs no IO. The same inspector can be wired to an ActorSystem
// (WithActorInspector) so actor lifecycle and inter-actor messages are observed on
// the same sink. The inspector is notified synchronously and must not block or
// mutate the instance.
func WithInspector[S comparable](insp Inspector) CastOption[S] {
	return func(c *castConfig[S]) { c.inspector = insp }
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

// RestoreOption configures Machine.Restore.
type RestoreOption[S comparable] func(*restoreConfig[S])

type restoreConfig[S comparable] struct {
	clock Clock
}

// WithRestoreClock wires the time seam a restored instance's delayed-transition
// driver reads, mirroring WithClock at Cast. It is consumed only by a Scheduler /
// host driver, never by the pure Fire step. When omitted, a restored instance
// defaults to SystemClock().
func WithRestoreClock[S comparable](c Clock) RestoreOption[S] {
	return func(cfg *restoreConfig[S]) { cfg.clock = c }
}

// SnapshotCodecOption configures MarshalSnapshot / UnmarshalSnapshot.
type SnapshotCodecOption[C any] func(*snapshotCodecConfig[C])

type snapshotCodecConfig[C any] struct {
	codec ContextCodec[C]
}

// WithContextCodec supplies a custom ContextCodec for a snapshot context that is
// not directly JSON-marshalable (or needs a bespoke wire form). When omitted, the
// default codec marshals the context with encoding/json, so the context type must
// be JSON-marshalable by default. Pass it to MarshalSnapshot / UnmarshalSnapshot
// to override the default.
func WithContextCodec[C any](codec ContextCodec[C]) SnapshotCodecOption[C] {
	return func(cfg *snapshotCodecConfig[C]) { cfg.codec = codec }
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
