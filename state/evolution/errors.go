package evolution

import "fmt"

// DecodeError reports that one side of a JSON diff could not be loaded into an
// IR. Side is "old" or "new"; the wrapped Err is the underlying decode failure.
type DecodeError struct {
	Side string
	Err  error
}

func (e *DecodeError) Error() string {
	return fmt.Sprintf("evolution: decode %s machine: %v", e.Side, e.Err)
}

// Unwrap exposes the underlying decode error for errors.Is / errors.As.
func (e *DecodeError) Unwrap() error { return e.Err }

// SerializeError reports that a machine could not be serialized to its IR before
// diffing. Side is "old" or "new".
type SerializeError struct {
	Side string
	Err  error
}

func (e *SerializeError) Error() string {
	return fmt.Sprintf("evolution: serialize %s machine: %v", e.Side, e.Err)
}

// Unwrap exposes the underlying serialize error for errors.Is / errors.As.
func (e *SerializeError) Unwrap() error { return e.Err }
