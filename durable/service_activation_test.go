package durable_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestRunService_ServiceFnCantChangeItsActivation pins why a settle can't find its
// activation gone through durable's API: a ServiceFn runs under RunService's step lock,
// so a Fire it makes to leave the state is refused, with ErrReentrantStep on the step's
// context and with its own context's error on any other, and the settle routes as
// usual. Only code that drives the ServiceRunner past the Handle can stop the activation
// (see TestServiceStep_StaleSettleRecordsNothing).
func TestRunService_ServiceFnCantChangeItsActivation(t *testing.T) {
	ctx := context.Background()
	var h *durable.Handle[string, string, svcCtx]
	var reentrant, other error
	fn := func(sctx context.Context, _ state.ServiceCtx[svcCtx]) (any, error) {
		_, reentrant = h.Fire(sctx, "next")
		octx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, other = h.Fire(octx, "next")
		return scnUser{ID: 7, Name: "ann"}, nil
	}
	m := forkMachine(fn)
	st := durable.NewMemStore()
	var err error
	if h, err = durable.NewRunner(m, st, durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))).
		Start(ctx, "o1", svcCtx{}, state.WithInitialState("loading")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, ran, rerr := h.RunService(ctx, state.InvokeID("fork", "loading", 0)); !ran || rerr != nil {
		t.Fatalf("RunService = %v, %v", ran, rerr)
	}
	if !errors.Is(reentrant, durable.ErrReentrantStep) {
		t.Fatalf("a Fire on the step's context: %v, want ErrReentrantStep", reentrant)
	}
	if !errors.Is(other, context.DeadlineExceeded) {
		t.Fatalf("a Fire on another context: %v, want its deadline", other)
	}
	if got := h.Instance().Current(); got != "ready" {
		t.Fatalf("in %q, want ready: the settle routed", got)
	}
	if got := tailLen(t, st, "o1"); got != 1 {
		t.Fatalf("the journal holds %d Records, want the settle's", got)
	}
}

// forkMachine invokes fetch in loading. "next" is declared in loading and in ready,
// with different targets, so a journal replayed from the wrong state still replays,
// to the wrong place.
func forkMachine(fn state.ServiceFn[svcCtx]) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("fork").
		Service("fetch", fn).
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done"), state.WithInvokeOnError("fail")).
		State("ready").
		State("canceled").Final().
		State("shipped").Final().
		State("failed").Final().
		Initial("loading").
		Transition("loading").On("done").GoTo("ready").
		Transition("loading").On("fail").GoTo("failed").
		Transition("loading").On("next").GoTo("canceled").
		Transition("ready").On("next").GoTo("shipped").
		Quench()
}
