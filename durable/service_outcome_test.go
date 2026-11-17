package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// outcomeMachine invokes fn on entering "loading" and records what the onDone or
// onError Assign sees in *seen, live and on replay.
func outcomeMachine(fn state.ServiceFn[svcCtx], seen *[]string) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("outcome").
		Service("fetch", fn).
		Reducer("noteDone", func(in state.AssignCtx[svcCtx]) svcCtx {
			*seen = append(*seen, "done")
			return in.Entity
		}).
		Reducer("noteError", func(in state.AssignCtx[svcCtx]) svcCtx {
			if e, ok := in.Event.(error); ok {
				*seen = append(*seen, e.Error())
			}
			return in.Entity
		}).
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done"), state.WithInvokeOnError("fail")).
		State("ready").Final().
		State("failed").Final().
		Initial("loading").
		Transition("loading").On("done").GoTo("ready").Assign("noteDone").
		Transition("loading").On("fail").GoTo("failed").Assign("noteError").
		Quench()
}

// TestRunService_ResultThatEncodesButWontDecode_FailsTheStep proves a result whose
// JSON form a replay can't decode, such as a number beyond float64, fails RunService
// with ErrHandleFailed and records nothing, as a result that won't encode does, so the
// instance stays recoverable with the service in flight again.
func TestRunService_ResultThatEncodesButWontDecode_FailsTheStep(t *testing.T) {
	ctx := context.Background()
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) { return json.RawMessage("1e400"), nil }
	var seen []string
	m := outcomeMachine(fn, &seen)
	st := durable.NewMemStore()
	opts := []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))}
	h, err := durable.NewRunner(m, st, opts...).Start(ctx, "i1", svcCtx{}, state.WithInitialState("loading"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	id := state.InvokeID("outcome", "loading", 0)
	if _, _, err = h.RunService(ctx, id); !errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("RunService: %v, want ErrHandleFailed", err)
	}
	if _, recs, lerr := st.Load(ctx, "i1"); lerr != nil || len(recs) != 0 {
		t.Fatalf("journal holds %d Records (err %v), want none", len(recs), lerr)
	}
	recovered, err := durable.Recover(ctx, m, st, "i1", opts...)
	if err != nil {
		t.Fatalf("Recover: %v, want the instance recoverable", err)
	}
	if got := recovered.Instance().Current(); got != "loading" {
		t.Fatalf("recovered in %q, want loading with the service in flight again", got)
	}
}

// panickyError is a service error whose Error method panics with a message holding
// invalid UTF-8.
type panickyError struct{}

func (panickyError) Error() string { panic("bad \xff byte") }

// TestRunService_PanickingErrorMessage_LiveMatchesReplay proves an error whose Error
// method panics settles onError with the same message live and on replay, though the
// panic's text holds invalid UTF-8, and that the message doesn't call it an error
// payload.
func TestRunService_PanickingErrorMessage_LiveMatchesReplay(t *testing.T) {
	ctx := context.Background()
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) { return nil, panickyError{} }
	var seen []string
	m := outcomeMachine(fn, &seen)
	st := durable.NewMemStore()
	opts := []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))}
	h, err := durable.NewRunner(m, st, opts...).Start(ctx, "i1", svcCtx{}, state.WithInitialState("loading"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	mustRunService(t, h, state.InvokeID("outcome", "loading", 0))
	if _, err = durable.Recover(ctx, m, st, "i1", opts...); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(seen) != 2 || seen[0] != seen[1] {
		t.Fatalf("live and replay saw %q, want the same message", seen)
	}
	if strings.Contains(seen[0], "error payload") || !strings.Contains(seen[0], "panicked") {
		t.Fatalf("onError saw %q, want a message saying the service error panicked", seen[0])
	}
}
