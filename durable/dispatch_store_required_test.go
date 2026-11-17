package durable_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// noDispatchStore wraps a Store and hides its DispatchStore methods, so the Runner
// can't track which effects it applied. It counts the writes that reach the Store.
type noDispatchStore struct {
	durable.Store
	writes int
}

func (s *noDispatchStore) Append(ctx context.Context, id durable.InstanceID, rec durable.Record, opts ...durable.AppendOption) (int64, error) {
	s.writes++
	return s.Store.Append(ctx, id, rec, opts...)
}

func (s *noDispatchStore) Checkpoint(ctx context.Context, id durable.InstanceID, snapshot []byte, throughStep int, opts ...durable.CheckpointOption) error {
	s.writes++
	return s.Store.Checkpoint(ctx, id, snapshot, throughStep, opts...)
}

// mustRefuseNoDispatchStore fails unless err matches ErrNoDispatchStore and not
// ErrEffectDispatch or ErrHandleFailed.
func mustRefuseNoDispatchStore(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, durable.ErrNoDispatchStore) {
		t.Fatalf("%s = %v, want ErrNoDispatchStore", what, err)
	}
	if errors.Is(err, durable.ErrEffectDispatch) || errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("%s = %v matches ErrEffectDispatch or ErrHandleFailed, want only ErrNoDispatchStore: retrying doesn't help", what, err)
	}
}

// TestEffectHandler_RefusesAStoreWithoutDispatch proves an effect handler wired with
// a Store that isn't a DispatchStore is refused with ErrNoDispatchStore, before any
// Store write or handler call, by Start, Recover and the stateless Fire. On main the
// handler was silently skipped. The refusal doesn't match ErrEffectDispatch: retrying
// doesn't help, so a caller that retries Recover on ErrEffectDispatch mustn't loop on
// it.
func TestEffectHandler_RefusesAStoreWithoutDispatch(t *testing.T) {
	ctx := context.Background()

	t.Run("Start", func(t *testing.T) {
		mem := durable.NewMemStore()
		st := &noDispatchStore{Store: mem}
		rec := newEffectRecorder()
		h, err := durable.NewRunner(entryTwoEffectsMachine(), st,
			durable.WithEffectHandler[string, string, *runCtx](rec.handler())).
			Start(ctx, "i1", &runCtx{}, state.WithInitialState("ready"))
		mustRefuseNoDispatchStore(t, "Start", err)
		if h != nil {
			t.Fatal("Start returned a Handle, want none")
		}
		if st.writes != 0 || rec.totalApplies() != 0 {
			t.Fatalf("Start wrote %d times and applied %d effects, want none", st.writes, rec.totalApplies())
		}
		if _, _, lerr := mem.Load(ctx, "i1"); !errors.Is(lerr, durable.ErrInstanceNotFound) {
			t.Fatalf("Load after the refused Start = %v, want ErrInstanceNotFound", lerr)
		}
	})

	// started returns a Store holding an instance started with no handler, after fired
	// Fires, so with "go" it holds an effect not yet sent.
	started := func(t *testing.T, fired ...string) *noDispatchStore {
		t.Helper()
		st := &noDispatchStore{Store: durable.NewMemStore()}
		r := durable.NewRunner(effectMachine(), st)
		if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
			t.Fatalf("Start with no handler: %v", err)
		}
		for _, ev := range fired {
			if _, err := r.Fire(ctx, "i1", ev); err != nil {
				t.Fatalf("Fire %s with no handler: %v", ev, err)
			}
		}
		st.writes = 0
		return st
	}

	for _, tc := range []struct {
		name  string
		fired []string
	}{{"no effect pending", nil}, {"an effect pending", []string{"go"}}} {
		t.Run("Recover/"+tc.name, func(t *testing.T) {
			st := started(t, tc.fired...)
			rec := newEffectRecorder()
			h, err := durable.Recover(ctx, effectMachine(), st, "i1",
				durable.WithEffectHandler[string, string, *runCtx](rec.handler()))
			mustRefuseNoDispatchStore(t, "Recover", err)
			if h != nil {
				t.Fatal("Recover returned a Handle, want none")
			}
			if st.writes != 0 || rec.totalApplies() != 0 {
				t.Fatalf("Recover wrote %d times and applied %d effects, want none", st.writes, rec.totalApplies())
			}
		})

		t.Run("stateless Fire/"+tc.name, func(t *testing.T) {
			st := started(t, tc.fired...)
			_, before, _ := st.Load(ctx, "i1")
			rec := newEffectRecorder()
			ev := "go"
			if len(tc.fired) > 0 {
				ev = "bill"
			}
			_, err := durable.NewRunner(effectMachine(), st,
				durable.WithEffectHandler[string, string, *runCtx](rec.handler())).
				Fire(ctx, "i1", ev)
			mustRefuseNoDispatchStore(t, "Fire", err)
			if st.writes != 0 || rec.totalApplies() != 0 {
				t.Fatalf("Fire wrote %d times and applied %d effects, want none", st.writes, rec.totalApplies())
			}
			if _, tail, _ := st.Load(ctx, "i1"); len(tail) != len(before) {
				t.Fatalf("the journal holds %d Records after the refused Fire, want %d", len(tail), len(before))
			}
		})
	}

	t.Run("no handler", func(t *testing.T) {
		st := started(t, "go")
		h, err := durable.Recover(ctx, effectMachine(), st, "i1")
		if err != nil {
			t.Fatalf("Recover with no handler: %v", err)
		}
		if _, err := h.Fire(ctx, "bill"); err != nil {
			t.Fatalf("Fire with no handler: %v", err)
		}
		if got := h.Instance().Current(); got != "billed" {
			t.Fatalf("in %q, want billed", got)
		}
	})
}

// TestEffectHandler_DispatchStoresUnaffected proves MemStore and FileStore, which
// implement DispatchStore, take an effect handler: Start, the stateless Fire and
// Recover all run, and each effect applies once.
func TestEffectHandler_DispatchStoresUnaffected(t *testing.T) {
	ctx := context.Background()
	fs, err := durable.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for _, tc := range []struct {
		name string
		st   durable.Store
	}{{"MemStore", durable.NewMemStore()}, {"FileStore", fs}} {
		t.Run(tc.name, func(t *testing.T) {
			rec := newEffectRecorder()
			r := durable.NewRunner(effectMachine(), tc.st,
				durable.WithEffectHandler[string, string, *runCtx](rec.handler()))
			if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err := r.Fire(ctx, "i1", "go"); err != nil {
				t.Fatalf("Fire: %v", err)
			}
			if _, err := durable.Recover(ctx, effectMachine(), tc.st, "i1",
				durable.WithEffectHandler[string, string, *runCtx](rec.handler())); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if n := rec.totalApplies(); n != 1 {
				t.Fatalf("applied %d effects, want the go step's one, once", n)
			}
		})
	}
}
