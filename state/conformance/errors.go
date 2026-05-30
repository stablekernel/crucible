package conformance

import "fmt"

// ErrSchemaVersion is returned when a serialized artifact carries a schema
// version this package does not understand.
type ErrSchemaVersion struct {
	Got  int
	Want int
}

func (e *ErrSchemaVersion) Error() string {
	return fmt.Sprintf("conformance: unsupported schema version %d (want %d)", e.Got, e.Want)
}

// ErrUnknownEvent is returned when a scenario names an event the codec cannot
// resolve to a typed value.
type ErrUnknownEvent struct {
	Name string
}

func (e *ErrUnknownEvent) Error() string {
	return fmt.Sprintf("conformance: scenario references unknown event %q", e.Name)
}

// Mismatch is one field-level divergence found by an oracle comparison.
type Mismatch struct {
	// Scenario is the name of the scenario whose run diverged.
	Scenario string
	// Field names what diverged (e.g. "finalState", "effects", "trace.len").
	Field string
	// Reference and Subject are the diverging values from each side.
	Reference string
	Subject   string
}

func (m Mismatch) String() string {
	return fmt.Sprintf("[%s] %s: reference=%q subject=%q", m.Scenario, m.Field, m.Reference, m.Subject)
}

// ErrConformance aggregates the mismatches found across an oracle comparison or
// a round-trip identity check. A nil error means the two sides agreed.
type ErrConformance struct {
	Mismatches []Mismatch
}

func (e *ErrConformance) Error() string {
	if len(e.Mismatches) == 0 {
		return "conformance: no mismatches"
	}
	msg := fmt.Sprintf("conformance: %d mismatch(es):", len(e.Mismatches))
	for _, m := range e.Mismatches {
		msg += "\n  " + m.String()
	}
	return msg
}
