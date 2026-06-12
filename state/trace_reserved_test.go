package state_test

// This file locks the reserved status of frozen Trace fields that have no writer
// in v1.0. A reserved field is kept on the frozen Trace shape so a future feature
// can populate it additively without a breaking change; these tests fail if an
// accidental writer starts populating one, so the reservation stays intentional.

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestTrace_PoliciesEvaluated_ReservedEmpty pins PoliciesEvaluated as reserved:
// no kernel step writes it in v1.0, so it must be empty after a representative
// full-trace Fire. If a future policy-tracing feature populates it, this test is
// updated deliberately alongside that feature.
func TestTrace_PoliciesEvaluated_ReservedEmpty(t *testing.T) {
	m := buildToggleMachine()
	ctx := context.Background()

	inst := m.Cast(nil,
		state.WithInitialState[toggleState](toggleA),
		state.WithFullTrace[toggleState](),
	)
	res := inst.Fire(ctx, toggleGo)
	if res.Err != nil {
		t.Fatalf("Fire err = %v", res.Err)
	}
	if len(res.Trace.PoliciesEvaluated) != 0 {
		t.Errorf("PoliciesEvaluated = %v, want empty (reserved, no writer in v1.0)",
			res.Trace.PoliciesEvaluated)
	}
}
