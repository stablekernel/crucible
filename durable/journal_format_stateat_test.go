package durable_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests answer two review findings. StateAt checked the journal formats only of
// the Records up to its target step, while Recover checks its whole tail, so a journal
// Recover refuses as mixed-version still reconstructed through StateAt before the older
// Record. And undecodableResult's doc said its error always wraps ErrEventDataEncoding,
// though a result with no JSON form fails the Handle with an error that doesn't.

// loopMachine stays in spin on every "go".
func loopMachine() *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("loop").
		State("spin").
		Initial("spin").
		Transition("spin").On("go").GoTo("spin").
		Quench()
}

// TestStateAt_RefusesAMixedJournalAtEveryStep: this version records steps 0 to 6 in
// format 2, and an older replica appends step 7 in format 1. Recover refuses the
// journal, so StateAt must refuse it at every step, before step 7 too, and Steps
// must refuse it, as each refuses a journal a newer version wrote.
func TestStateAt_RefusesAMixedJournalAtEveryStep(t *testing.T) {
	for _, history := range []bool{false, true} {
		name := "Load"
		if history {
			name = "History"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			m := loopMachine()
			var st *durable.MemStore
			if history {
				st = durable.NewMemStore(durable.WithHistory())
			} else {
				st = durable.NewMemStore()
			}
			h, err := durable.NewRunner(m, st).Start(ctx, "l1", svcCtx{}, state.WithInitialState("spin"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			for i := range 7 {
				if _, err = h.Fire(ctx, "go"); err != nil {
					t.Fatalf("Fire %d: %v", i, err)
				}
			}
			if _, err = st.Append(ctx, "l1", durable.Record{Step: 7, Event: []byte(`"go"`), Format: 1}); err != nil {
				t.Fatalf("Append: %v", err)
			}
			mixed := func(what string, err error) {
				t.Helper()
				if err == nil || !strings.Contains(err.Error(), "older version") {
					t.Errorf("%s = %v, want the mixed-version error", what, err)
				}
			}
			_, err = durable.Recover(ctx, m, st, "l1")
			mixed("Recover", err)
			for _, step := range []int{durable.BaselineStep, 0, 5, 6, 7} {
				_, err = durable.StateAt(ctx, m, st, "l1", step)
				mixed("StateAt("+strconv.Itoa(step)+")", err)
			}
			_, err = durable.Steps(ctx, st, "l1")
			mixed("Steps", err)
		})
	}
}

// TestRunService_ResultEncodingErrorsMatchTheirSentinels pins which sentinel each
// failed settle wraps. A result that won't decode is parked: RunService returns
// ErrEventDataEncoding with the Handle healthy. A result with no JSON form fails the
// Handle: RunService returns ErrHandleFailed, not ErrEventDataEncoding, whose Handle
// stays usable.
func TestRunService_ResultEncodingErrorsMatchTheirSentinels(t *testing.T) {
	t.Run("won't decode", func(t *testing.T) {
		_, runErr, _ := scnRun(t, returning(scnUser{ID: 7, Name: "ann"}, nil), failingDecoder())
		if !errors.Is(runErr, durable.ErrEventDataEncoding) || errors.Is(runErr, durable.ErrHandleFailed) {
			t.Errorf("RunService = %v, want ErrEventDataEncoding without ErrHandleFailed", runErr)
		}
	})
	t.Run("won't encode", func(t *testing.T) {
		_, runErr, _ := scnRun(t, returning(make(chan int), nil))
		if !errors.Is(runErr, durable.ErrHandleFailed) || errors.Is(runErr, durable.ErrEventDataEncoding) {
			t.Errorf("RunService = %v, want ErrHandleFailed without ErrEventDataEncoding", runErr)
		}
	})
}
