package durable_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// dispatchProbeStore wraps a MemStore's dispatch tracking: it counts each call to
// Dispatched and MarkDispatched, and panics in either, or returns an error, when told
// to.
type dispatchProbeStore struct {
	*durable.MemStore
	mu                       sync.Mutex
	dispatched, marks        int
	panicDispatched, panicMk bool
	errDispatched, errMk     error
}

func newDispatchProbeStore(st *durable.MemStore) *dispatchProbeStore {
	return &dispatchProbeStore{MemStore: st}
}

func (s *dispatchProbeStore) Dispatched(ctx context.Context, id durable.InstanceID) (map[string]bool, error) {
	s.mu.Lock()
	s.dispatched++
	p, e := s.panicDispatched, s.errDispatched
	s.mu.Unlock()
	if p {
		panic("Dispatched exploded")
	}
	if e != nil {
		return nil, e
	}
	return s.MemStore.Dispatched(ctx, id)
}

func (s *dispatchProbeStore) MarkDispatched(ctx context.Context, id durable.InstanceID, ids ...string) error {
	s.mu.Lock()
	s.marks++
	p, e := s.panicMk, s.errMk
	s.mu.Unlock()
	if p {
		panic("MarkDispatched exploded")
	}
	if e != nil {
		return e
	}
	return s.MemStore.MarkDispatched(ctx, id, ids...)
}

func (s *dispatchProbeStore) counts() (dispatched, marks int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dispatched, s.marks
}

// unmarkedFireEffect starts i1 on effectMachine and fires "go" with a handler that
// fails, so the journal holds step 0's effect unmarked for the next Recover to send.
func unmarkedFireEffect(t *testing.T) *durable.MemStore {
	t.Helper()
	ctx := context.Background()
	st := durable.NewMemStore()
	live := &stepIDHandler{failStep: "0"}
	h, err := durable.NewRunner(effectMachine(), st, durable.WithEffectHandler[string, string, *runCtx](live.handle)).
		Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); !errors.Is(err, durable.ErrEffectDispatch) {
		t.Fatalf("Fire go: %v, want ErrEffectDispatch", err)
	}
	return st
}

// TestDispatchStorePanic_ReturnsEffectDispatch proves a DispatchStore whose Dispatched
// or MarkDispatched panics fails Recover and the stateless Runner.Fire with
// ErrEffectDispatch, as a live step's dispatch and a panicking handler already do,
// rather than letting the panic out.
func TestDispatchStorePanic_ReturnsEffectDispatch(t *testing.T) {
	ctx := context.Background()
	healthy := durable.WithEffectHandler[string, string, *runCtx](newEffectRecorder().handler())
	cases := []struct {
		name string
		arm  func(*dispatchProbeStore)
	}{
		{"MarkDispatched", func(s *dispatchProbeStore) { s.panicMk = true }},
		{"Dispatched", func(s *dispatchProbeStore) { s.panicDispatched = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/Recover", func(t *testing.T) {
			st := newDispatchProbeStore(unmarkedFireEffect(t))
			tc.arm(st)
			err := noPanic(t, "Recover", func() error {
				_, rerr := durable.Recover(ctx, effectMachine(), st, "i1", healthy)
				return rerr
			})
			if !errors.Is(err, durable.ErrEffectDispatch) {
				t.Fatalf("Recover: %v, want ErrEffectDispatch", err)
			}
			if !strings.Contains(err.Error(), "step 0 for") {
				t.Fatalf("Recover: %v, want the message to name step 0", err)
			}
		})
		t.Run(tc.name+"/Runner.Fire", func(t *testing.T) {
			st := newDispatchProbeStore(unmarkedFireEffect(t))
			tc.arm(st)
			err := noPanic(t, "Runner.Fire", func() error {
				_, ferr := durable.NewRunner(effectMachine(), st, healthy).Fire(ctx, "i1", "bill")
				return ferr
			})
			if !errors.Is(err, durable.ErrEffectDispatch) {
				t.Fatalf("Runner.Fire: %v, want ErrEffectDispatch", err)
			}
		})
	}
	// Start applies its effects without the dispatched set, so a panicking
	// MarkDispatched can't reach it; a step's live dispatch already turns it into
	// ErrEffectDispatch. Pin both, so the three paths stay alike.
	t.Run("MarkDispatched/live step", func(t *testing.T) {
		st := newDispatchProbeStore(durable.NewMemStore())
		h, err := durable.NewRunner(effectMachine(), st, healthy).Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		st.panicMk = true
		err = noPanic(t, "Fire", func() error {
			_, ferr := h.Fire(ctx, "go")
			return ferr
		})
		if !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, durable.ErrEffectDispatch) {
			t.Fatalf("Fire: %v, want ErrHandleFailed wrapping ErrEffectDispatch", err)
		}
	})
}

// pingMachine emits a notify on every "ping", so each Fire records one effect.
func pingMachine() *state.Machine[string, string, *runCtx] {
	return state.ForgeFor[*runCtx]("ping").
		Action("notify", func(state.ActionCtx[*runCtx]) (state.Effect, error) {
			return notifyEffect{To: "ops@example.com"}, nil
		}).
		State("idle").
		Initial("idle").
		Transition("idle").On("ping").GoTo("idle").Do("notify").
		Quench()
}

// TestRecover_LoadsTheDispatchedSetOncePerEffectRecord pins what Recover costs the
// Store: it loads the instance's whole dispatched set once for each tail Record that
// names effects, and marks nothing when every effect already landed. main did the same
// for a Fire's Records; this change adds a tick's and a service settle's at the same
// rate. Loading the set once per Recover would make this 1.
func TestRecover_LoadsTheDispatchedSetOncePerEffectRecord(t *testing.T) {
	ctx := context.Background()
	const n = 25
	st := newDispatchProbeStore(durable.NewMemStore())
	handler := durable.WithEffectHandler[string, string, *runCtx](newEffectRecorder().handler())
	h, err := durable.NewRunner(pingMachine(), st, handler).Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err = h.Fire(ctx, "ping"); err != nil {
			t.Fatalf("Fire ping %d: %v", i, err)
		}
	}
	liveLoads, liveMarks := st.counts()
	if _, err = durable.Recover(ctx, pingMachine(), st, "i1", handler); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	loads, marks := st.counts()
	if got := loads - liveLoads; got != n {
		t.Fatalf("Recover over %d effect Records loaded the dispatched set %d times, want %d", n, got, n)
	}
	if got := marks - liveMarks; got != 0 {
		t.Fatalf("Recover marked %d effects, want none: every one landed live", got)
	}
}

var errDispatchStore = errors.New("dispatch table unavailable")

// TestDispatchStoreError_ReturnsEffectDispatch proves a DispatchStore whose Dispatched
// or MarkDispatched returns an error fails a live step, Recover and the stateless
// Runner.Fire with ErrEffectDispatch wrapping the Store's error, as one that panics
// does: either way the effect isn't marked, and Recover sends it again. A live step's
// error is ErrHandleFailed too.
func TestDispatchStoreError_ReturnsEffectDispatch(t *testing.T) {
	ctx := context.Background()
	healthy := durable.WithEffectHandler[string, string, *runCtx](newEffectRecorder().handler())
	cases := []struct {
		name string
		arm  func(*dispatchProbeStore)
	}{
		{"MarkDispatched", func(s *dispatchProbeStore) { s.errMk = errDispatchStore }},
		{"Dispatched", func(s *dispatchProbeStore) { s.errDispatched = errDispatchStore }},
	}
	want := func(t *testing.T, what string, err error) {
		t.Helper()
		if !errors.Is(err, durable.ErrEffectDispatch) || !errors.Is(err, errDispatchStore) {
			t.Fatalf("%s: %v, want ErrEffectDispatch wrapping the Store's error", what, err)
		}
	}
	for _, tc := range cases {
		t.Run(tc.name+"/live step", func(t *testing.T) {
			st := newDispatchProbeStore(durable.NewMemStore())
			h, err := durable.NewRunner(effectMachine(), st, healthy).Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			tc.arm(st)
			_, err = h.Fire(ctx, "go")
			want(t, "Fire", err)
			if !errors.Is(err, durable.ErrHandleFailed) {
				t.Fatalf("Fire: %v, want ErrHandleFailed", err)
			}
		})
		t.Run(tc.name+"/Recover", func(t *testing.T) {
			st := newDispatchProbeStore(unmarkedFireEffect(t))
			tc.arm(st)
			_, err := durable.Recover(ctx, effectMachine(), st, "i1", healthy)
			want(t, "Recover", err)
		})
		t.Run(tc.name+"/Runner.Fire", func(t *testing.T) {
			st := newDispatchProbeStore(unmarkedFireEffect(t))
			tc.arm(st)
			_, err := durable.NewRunner(effectMachine(), st, healthy).Fire(ctx, "i1", "bill")
			want(t, "Runner.Fire", err)
		})
	}
}
