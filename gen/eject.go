package gen

import (
	"errors"

	"github.com/stablekernel/crucible/state"
)

// errNotImplemented is the scaffold placeholder returned until the codegen lands.
var errNotImplemented = errors.New("gen: Eject not implemented")

// Eject renders the given IR to typed Go stub source.
//
// Signature choice (a): the IR is walked directly. The type parameters S, E, and
// C are unused by the codegen — the IR's behavior references (state.Ref) and its
// context schema (state.ContextSchema) are non-generic, so the walk and the
// emitted source are driven entirely by those concrete shapes. The parameters
// remain on the signature so callers can pass an *state.IR (or its value) without
// reflection or an interface wrapper, keeping Eject a drop-in over a typed
// machine's loaded IR.
func Eject[S comparable, E comparable, C any](ir state.IR[S, E, C], opts ...Option) ([]byte, error) {
	_ = ir
	_ = opts
	return nil, errNotImplemented
}
