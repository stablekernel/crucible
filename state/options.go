package state

import "log/slog"

// This file declares the functional-option types for every public constructor
// and operation, per the kernel's functional-options convention. Each is an
// opaque type so new knobs arrive as new options, never as signature changes.
//
// The option constructors here are part of the v1 surface; their internal
// effect is applied by the (not-yet-implemented) kernel.

// ForgeOption configures Forge.
type ForgeOption func(*forgeConfig)

type forgeConfig struct {
	version string
	id      string
}

// WithMachineVersion stamps the machine DEFINITION version (the IR Version, a
// semver label) onto a Forge-built machine, so a Snapshot taken from it carries the
// version a restored instance self-identifies by — the precondition for live
// migration. It mirrors the version a machine rehydrated from a versioned IR
// already carries. When omitted, a Forge-built machine has no definition version.
func WithMachineVersion(version string) ForgeOption {
	return func(c *forgeConfig) { c.version = version }
}

// WithMachineID stamps the machine DEFINITION id (the IR ID) onto a Forge-built
// machine, carried alongside the version so a migrator can resolve the source
// definition unambiguously. When omitted, a Forge-built machine has no definition
// id.
func WithMachineID(id string) ForgeOption {
	return func(c *forgeConfig) { c.id = id }
}

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
	initial       S
	hasInitial    bool
	clock         Clock
	inspector     Inspector
	logger        *slog.Logger
	traceFull     bool // set by WithFullTrace
	histLimit     int  // set by WithHistory (positive value enables ring buffer)
	histUnbounded bool // set by WithUnboundedHistory
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

// WithLogger wires a structured-logging seam an instance writes a terse,
// fixed-shape record to as each Fire settles — distinct from the event-shaped
// Inspector. Where an Inspector receives the full, typed InspectionEvent stream
// for live observation and tooling, the logger is the conventional *slog.Logger a
// host already threads through its services, so a Fire's outcome shows up in the
// host's ordinary logs (machine, event, from, to, outcome) without the host
// adapting an Inspector. It is no-op by default: with no WithLogger the instance
// holds a nil logger and never logs, so the pure Fire step performs no IO and adds
// zero overhead. The logger is written to synchronously on the Fire path at
// slog.LevelDebug and must not block; it observes only and never mutates the
// instance. Wire both seams when you want host logs AND structured inspection —
// they are independent.
func WithLogger[S comparable](l *slog.Logger) CastOption[S] {
	return func(c *castConfig[S]) { c.logger = l }
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

// WithFullTrace opts the instance into full trace mode, populating all rich
// diagnostic fields on every FireResult.Trace: GuardsEvaluated, EffectsEmitted,
// ExitedStates, EnteredStates, AssignsApplied, Microsteps, EventPayload, and
// SelectedTransition. Without this option (or WithInspector / WithHistory /
// WithUnboundedHistory), those fields are empty to minimize allocations on
// instances whose traces are not observed. Logger-only instances stay lite.
func WithFullTrace[S comparable]() CastOption[S] {
	return func(c *castConfig[S]) { c.traceFull = true }
}

// WithHistory enables a bounded ring-buffer trace history of capacity limit.
// The last limit settled traces are retained; older entries are overwritten in
// declaration order. limit <= 0 is a no-op (no retention). Implies full trace.
// Use History() to retrieve the stored traces in chronological order.
func WithHistory[S comparable](limit int) CastOption[S] {
	return func(c *castConfig[S]) {
		if limit > 0 {
			c.histLimit = limit
		}
	}
}

// WithUnboundedHistory enables append-only trace history: every settled trace is
// retained. This is the previous default behavior, now opt-in. Suitable for
// short-lived or test instances; long-lived production instances should prefer
// WithHistory to bound memory growth. Implies full trace.
func WithUnboundedHistory[S comparable]() CastOption[S] {
	return func(c *castConfig[S]) { c.histUnbounded = true }
}

// FireOption configures Fire / FireSeq / FireEach.
type FireOption func(*fireConfig)

type fireConfig struct {
	collectAll bool
	eventData  any
	hasData    bool
}

// CollectAll makes a batch fire run every step and gather all errors instead of
// stopping at the first.
func CollectAll() FireOption { return func(c *fireConfig) { c.collectAll = true } }

// WithEventData attaches a payload to a single Fire so the triggering transition's
// Assign reads it from AssignCtx.Event. It is the channel by which a host delivers
// a service result, an actor's done-data, or an error to the onDone/onError
// transition's reducer: the ServiceRunner and ActorSystem re-fire the routing
// event with the result as the payload, so the reducer consumes it through
// AssignCtx.Event with no side channel. When omitted, AssignCtx.Event carries the
// boxed triggering event itself.
func WithEventData(data any) FireOption {
	return func(c *fireConfig) {
		c.eventData = data
		c.hasData = true
	}
}

// AssayOption configures Assay.
type AssayOption func(*assayConfig)

type assayConfig struct{ aggregate bool }

// Aggregate makes Assay collect all failing requirements in one pass instead of
// failing fast at the first. It is a pure directive option (it carries no value),
// so it drops the With prefix that value-carrying options keep — matching Strict
// and CollectAll.
func Aggregate() AssayOption { return func(c *assayConfig) { c.aggregate = true } }

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
	clock                Clock
	rejectMachineVersion bool
}

// RejectMachineVersionMismatch makes Restore enforce the machine DEFINITION
// version strictly: a snapshot whose MachineVersion differs from the target
// machine's version is rejected with a typed *SnapshotVersionError instead of the
// default advisory (accept) posture. Use it when an instance must only resume
// against the exact machine version it was snapshotted from. The snapshot-format
// schema version is always validated regardless of this option.
func RejectMachineVersionMismatch[S comparable]() RestoreOption[S] {
	return func(cfg *restoreConfig[S]) { cfg.rejectMachineVersion = true }
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
