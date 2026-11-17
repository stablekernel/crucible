package durable

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

type plainStoreCtx struct{}

// plainStore hides MemStore's DispatchStore methods, so it tracks no dispatched effects.
type plainStore struct{ Store }

// TestDispatch_WithoutADispatchStore_RefusesPermanently pins that dispatch itself
// refuses a handler wired with a Store that can't track dispatched effects, with the
// permanent ErrNoDispatchStore, should a path ever reach it without Start's or
// recover's refusal. It must not surface as a retryable ErrEffectDispatch.
func TestDispatch_WithoutADispatchStore_RefusesPermanently(t *testing.T) {
	m := state.ForgeFor[plainStoreCtx]("plain").
		State("a").
		Initial("a").
		Quench()
	applied := 0
	r := NewRunner(m, plainStore{NewMemStore()},
		WithEffectHandler[string, string, plainStoreCtx](func(context.Context, string, state.Effect) error {
			applied++
			return nil
		}))
	des := []dispatchableEffect{{id: "0#0#demo.notify", effect: state.UnknownEffect{EffectKind: "demo.notify"}}}

	err := r.dispatch(context.Background(), "i1", des)
	if !errors.Is(err, ErrNoDispatchStore) {
		t.Fatalf("dispatch = %v, want ErrNoDispatchStore", err)
	}
	if errors.Is(err, ErrEffectDispatch) {
		t.Fatalf("dispatch = %v, which matches the retryable ErrEffectDispatch", err)
	}
	if applied != 0 {
		t.Fatalf("the handler ran %d times, want none", applied)
	}
}
