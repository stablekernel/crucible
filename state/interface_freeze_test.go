package state

import "context"

// This file pins the v1.0 frozen interface surface with compile-time
// assertions. If a freeze is violated — a sealed interface gains a method its
// sole crucible implementer does not satisfy, or a host-implementable interface
// loses its locked method set — the package stops compiling here, turning a
// silent surface drift into a build break.

// ContextView is SEALED: crucible owns it via the unexported isContextView
// marker, so inProcessView is the only legal implementer. Hosts must not
// implement it.
var _ ContextView = inProcessView[int]{}

// The following interfaces are host-implementable with a LOCKED method set.
// Each assertion pins crucible's own concrete implementer so a method-set drift
// breaks the build. actorAdapter's type params satisfy its
// (S comparable, E comparable, C any) constraints with int/int/int.
var (
	_ Clock             = systemClock{}
	_ ContextCodec[int] = jsonCodec[int]{}
	_ Snapshotter       = (*actorAdapter[int, int, int])(nil)
	_ ActorInstance     = (*actorAdapter[int, int, int])(nil)
)

// ---------------------------------------------------------------------------
// v1.0 API signature freeze.
//
// The following compile-time assignments pin the EXACT signatures of the
// load-bearing public constructors and methods that the v1 promise covers. If
// any signature drifts — a parameter type changes, an option tail is dropped, a
// return type changes — the package stops compiling here. This is deliberate: a
// public signature change is a breaking change and must be a conscious decision,
// not an accident caught only downstream.
//
// HOW TO UPDATE (deliberately, additive only): these constructors and methods
// already end in a variadic functional-option tail, so a new capability arrives
// as a new option WITHOUT changing any signature below — no edit needed. Edit a
// pinned signature here only when intentionally making a breaking API change,
// which requires a major version bump.
//
// Only genuinely load-bearing surface is pinned; internal helpers are left free
// to evolve. Signatures are instantiated at concrete int/int/int (or string)
// type params, which does not change the shape being frozen.
var (
	// Forge / ForgeFor — the two builder entry points.
	_ func(string, ...ForgeOption) *Builder[int, int, int]       = Forge[int, int, int]
	_ func(string, ...ForgeOption) *Builder[string, string, int] = ForgeFor[int]

	// Quench / Cast — builder -> machine -> instance.
	_ func(*Builder[int, int, int], ...QuenchOption) *Machine[int, int, int]          = (*Builder[int, int, int]).Quench
	_ func(*Machine[int, int, int], int, ...CastOption[int]) *Instance[int, int, int] = (*Machine[int, int, int]).Cast

	// Fire — the pure step, on Instance.
	_ func(*Instance[int, int, int], context.Context, int, ...FireOption) FireResult[int] = (*Instance[int, int, int]).Fire

	// IR serialization round-trip.
	_ func([]byte, ...LoadOption) (*IR[int, int, int], error)        = LoadFromJSON[int, int, int]
	_ func(*Machine[int, int, int], ...ToJSONOption) ([]byte, error) = (*Machine[int, int, int]).ToJSON

	// Palette — registry introspection.
	_ func(*Registry[int]) []Descriptor = (*Registry[int]).Palette

	// Visualization exporters.
	_ func(*Machine[int, int, int], ...VizOption) string = (*Machine[int, int, int]).ToMermaid
	_ func(*Machine[int, int, int], ...VizOption) string = (*Machine[int, int, int]).ToDOT
)
