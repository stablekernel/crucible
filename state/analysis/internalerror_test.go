package analysis_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
)

// brokenKey is a state-name type whose JSON round-trip fails: it marshals to a
// plain string but always refuses to unmarshal, so buildGraph's LoadFromJSON step
// errors. It models a corrupt/incompatible IR encountered when reading a machine
// back from its serialized form, exercising the KindInternalError path (A1).
type brokenKey string

func (brokenKey) MarshalJSON() ([]byte, error) { return []byte(`"x"`), nil }

func (*brokenKey) UnmarshalJSON([]byte) error {
	return errors.New("brokenKey: refusing to unmarshal")
}

func (b brokenKey) String() string { return string(b) }

// TestInternalError_OnIRLoadFailure proves an IR-load/internal failure surfaces a
// KindInternalError finding rather than being mis-reported as an unreachable
// state (A1).
func TestInternalError_OnIRLoadFailure(t *testing.T) {
	m := state.Forge[brokenKey, string, any]("broken").
		State("open").
		Transition("open").On("go").GoTo("closed").
		State("closed").Final().
		Initial("open").
		Quench()

	r := analysis.Analyze(m)

	if got := r.OfKind(analysis.KindInternalError); len(got) == 0 {
		t.Fatalf("expected an internal_error finding when the IR cannot be read; report:\n%s", r)
	}
	if got := r.OfKind(analysis.KindUnreachableState); len(got) != 0 {
		t.Fatalf("an IR-load failure must NOT be mis-reported as unreachable; report:\n%s", r)
	}
	for _, f := range r.OfKind(analysis.KindInternalError) {
		if !strings.Contains(f.Message, "could not be read") && !strings.Contains(f.Message, "IR") {
			t.Fatalf("internal_error message should explain the IR could not be read; got %q", f.Message)
		}
	}
}
