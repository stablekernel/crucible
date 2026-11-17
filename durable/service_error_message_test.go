package durable_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestRunService_AnUnboundServiceErrorIsMessageOnly pins a documented limit: errors are
// message-only, so onError's Assign sees a plain error with the message of an unbound
// service's *state.UnboundRefError, which errors.As doesn't match, live or on replay.
func TestRunService_AnUnboundServiceErrorIsMessageOnly(t *testing.T) {
	ctx := context.Background()
	var seen []any
	m := state.ForgeFor[svcCtx]("unbound").
		Service("fetch", func(context.Context, state.ServiceCtx[svcCtx]) (any, error) { return nil, nil }).
		Reducer("noteFail", func(in state.AssignCtx[svcCtx]) svcCtx {
			seen = append(seen, in.Event)
			return in.Entity
		}).
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done"), state.WithInvokeOnError("fail")).
		State("ready").Final().
		State("failed").Final().
		Initial("loading").
		Transition("loading").On("done").GoTo("ready").
		Transition("loading").On("fail").GoTo("failed").Assign("noteFail").
		Quench()
	store := durable.NewMemStore()
	// The registry binds no "fetch", so the service settles as unbound.
	opt := durable.WithServiceRegistry[string, string, svcCtx](state.NewRegistry[svcCtx]())
	h, err := durable.NewRunner(m, store, opt).Start(ctx, "unb-1", svcCtx{}, state.WithInitialState("loading"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, _, err = h.RunService(ctx, state.InvokeID("unbound", "loading", 0)); err != nil {
		t.Fatalf("RunService: %v", err)
	}
	if _, err = durable.Recover(ctx, m, store, "unb-1", opt); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("onError's Assign ran %d times (%v), want 2: live and on replay", len(seen), seen)
	}
	want := (&state.UnboundRefError{Kind: "service", Name: "fetch"}).Error()
	for i, when := range []string{"live", "on replay"} {
		err, ok := seen[i].(error)
		if !ok || err.Error() != want {
			t.Fatalf("%s, onError's Assign saw %T %v, want an error saying %q", when, seen[i], seen[i], want)
		}
		var unbound *state.UnboundRefError
		if errors.As(err, &unbound) {
			t.Fatalf("%s, errors.As matched an *UnboundRefError, though errors are message-only", when)
		}
	}
}
