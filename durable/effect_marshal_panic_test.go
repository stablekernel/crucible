package durable_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests pin a KindedEffect whose MarshalJSON panics: it is recorded by id and
// kind, as one whose marshal fails, rather than panicking out of a step the kernel
// applied. A Kind that panics still fails the Handle.

// panicky is a KindedEffect whose MarshalJSON panics.
type panicky struct{}

func (panicky) Kind() string                 { return "demo.panicky" }
func (panicky) MarshalJSON() ([]byte, error) { panic("marshal blew up") }

// panickyKind is a KindedEffect whose Kind panics.
type panickyKind struct{}

func (panickyKind) Kind() string { panic("kind blew up") }

func emitting(eff state.Effect) func(state.ActionCtx[svcCtx]) (state.Effect, error) {
	return func(state.ActionCtx[svcCtx]) (state.Effect, error) { return eff, nil }
}

func panickyMachine(eff state.Effect) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("pnk").
		Action("emit", emitting(eff)).
		State("idle").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("done").Do("emit").
		Quench()
}

// fireCatching fires event on h and returns the panic that escaped, or the error.
func fireCatching(h *durable.Handle[string, string, svcCtx], event string) (panicked any, err error) {
	defer func() { panicked = recover() }()
	_, err = h.Fire(context.Background(), event)
	return nil, err
}

// A KindedEffect whose MarshalJSON panics is recorded as one whose marshal fails: by id
// and kind, with the panic's message in Meta under crucible.marshalError. The step
// doesn't panic or fail, and Recover replays it.
func TestUnmarshalableEffect_PanickingMarshal_RecordsByIDAndKind(t *testing.T) {
	ctx := context.Background()
	m := panickyMachine(panicky{})
	store := durable.NewMemStore()
	h, err := durable.NewRunner(m, store).Start(ctx, "pnk-1", svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	p, ferr := fireCatching(h, "go")
	if p != nil || ferr != nil {
		t.Fatalf("Fire = %v, panic %v; want neither", ferr, p)
	}
	_, recs, err := store.Load(ctx, "pnk-1")
	if err != nil || len(recs) != 1 || len(recs[0].Effects) != 1 {
		t.Fatalf("Load = %d Records (err %v), want one with one effect", len(recs), err)
	}
	env := recs[0].Effects[0]
	if env.Kind != "demo.panicky" || env.EffectID != "0#0#demo.panicky" || len(env.Payload) != 0 {
		t.Fatalf("recorded %+v, want kind demo.panicky, id 0#0#demo.panicky and no payload", env)
	}
	if msg, _ := env.Meta[metaMarshalError].(string); !strings.Contains(msg, "marshal blew up") {
		t.Fatalf("Meta = %v, want the panic's message under %q", env.Meta, metaMarshalError)
	}
	rh, err := durable.Recover(ctx, m, store, "pnk-1")
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := rh.Instance().Current(); got != "done" {
		t.Fatalf("recovered in %q, want done", got)
	}
}

// Pins current behavior: a KindedEffect whose Kind panics has no kind to record it by,
// so it can't take the id-and-kind fallback. The panic leaves the Fire, and the Handle
// is failed first, as for any panic in host code during a step, so the caller can
// Recover. The kernel applied the transition, but nothing was journaled.
func TestUnmarshalableEffect_PanickingKind_FailsTheHandle(t *testing.T) {
	ctx := context.Background()
	m := panickyMachine(panickyKind{})
	store := durable.NewMemStore()
	h, err := durable.NewRunner(m, store).Start(ctx, "pnk-1", svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if p, _ := fireCatching(h, "go"); p == nil || !strings.Contains(fmt.Sprint(p), "kind blew up") {
		t.Fatalf("Fire panicked with %v, want the Kind panic", p)
	}
	if _, err := h.Fire(ctx, "go"); !errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("Fire after the panic = %v, want ErrHandleFailed", err)
	}
	if got := tailLen(t, store, "pnk-1"); got != 0 {
		t.Fatalf("the journal holds %d Records, want none", got)
	}
}
