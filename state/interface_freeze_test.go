package state

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
