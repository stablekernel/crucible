package durable_test

// These tests pin Start's documented contract for the initial entry's domain effects:
// they are at-least-once. Start applies them before it writes the baseline and never
// marks them, and nothing replays them, so a handler failure, a failed baseline write
// or overlapping Starts re-apply them, and Recover never applies them.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

const (
	startNotifyID = "-1#0#demo.notify"
	startChargeID = "-1#1#demo.charge"
)

// entryTwoEffectsMachine emits two host effects on entering its initial state.
func entryTwoEffectsMachine() *state.Machine[string, string, *runCtx] {
	return state.ForgeFor[*runCtx]("entryTwo").
		Action("notify", func(state.ActionCtx[*runCtx]) (state.Effect, error) {
			return notifyEffect{To: "ops@example.com"}, nil
		}).
		Action("charge", func(state.ActionCtx[*runCtx]) (state.Effect, error) {
			return chargeEffect{Cents: 500}, nil
		}).
		State("ready").OnEntry("notify").OnEntry("charge").
		State("done").Final().
		Initial("ready").
		Transition("ready").On("finish").GoTo("done").
		Quench()
}

// startContractStore wraps a MemStore. It can fail Checkpoint once, and report an
// instance not found on its first lieLoads probes, as two Starts whose probes both ran
// before either wrote see it. It counts every call.
type startContractStore struct {
	*durable.MemStore
	mu             sync.Mutex
	failCheckpoint int
	lieLoads       int
	calls          map[string]int
}

func newStartContractStore() *startContractStore {
	return &startContractStore{MemStore: durable.NewMemStore(), calls: map[string]int{}}
}

func (s *startContractStore) count(op string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[op]++
}

func (s *startContractStore) callCount(op string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[op]
}

func (s *startContractStore) Load(ctx context.Context, id durable.InstanceID) ([]byte, []durable.Record, error) {
	s.count("Load")
	s.mu.Lock()
	lie := s.lieLoads > 0
	if lie {
		s.lieLoads--
	}
	s.mu.Unlock()
	if lie {
		return nil, nil, fmt.Errorf("%w: %q", durable.ErrInstanceNotFound, id)
	}
	return s.MemStore.Load(ctx, id)
}

func (s *startContractStore) Checkpoint(ctx context.Context, id durable.InstanceID, snap []byte, through int, opts ...durable.CheckpointOption) error {
	s.count("Checkpoint")
	s.mu.Lock()
	fail := s.failCheckpoint > 0
	if fail {
		s.failCheckpoint--
	}
	s.mu.Unlock()
	if fail {
		return errors.New("checkpoint unavailable")
	}
	return s.MemStore.Checkpoint(ctx, id, snap, through, opts...)
}

func (s *startContractStore) MarkDispatched(ctx context.Context, id durable.InstanceID, ids ...string) error {
	s.count("MarkDispatched")
	return s.MemStore.MarkDispatched(ctx, id, ids...)
}

func handlerOpt(rec *effectRecorder) durable.Option[string, string, *runCtx] {
	return durable.WithEffectHandler[string, string, *runCtx](rec.handler())
}

// mustNotExist fails unless the Store has no instance id.
func mustNotExist(t *testing.T, st durable.Store, id durable.InstanceID) {
	t.Helper()
	if _, _, err := st.Load(context.Background(), id); !errors.Is(err, durable.ErrInstanceNotFound) {
		t.Fatalf("Load(%q) = %v, want ErrInstanceNotFound: a failed Start leaves no instance", id, err)
	}
}

// TestStart_HandlerFails_LeavesNoInstance_StartAgainReappliesEffects: a handler that
// fails on the second initial effect fails Start with ErrEffectDispatch and leaves no
// instance, so the caller can Start again, which applies the first effect a second
// time. Recover applies neither.
func TestStart_HandlerFails_LeavesNoInstance_StartAgainReappliesEffects(t *testing.T) {
	ctx := context.Background()
	st := newStartContractStore()
	rec := newEffectRecorder()
	rec.failKinds["demo.charge"] = errors.New("downstream unavailable")
	r := durable.NewRunner(entryTwoEffectsMachine(), st, handlerOpt(rec))

	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("ready")); !errors.Is(err, durable.ErrEffectDispatch) {
		t.Fatalf("first Start = %v, want ErrEffectDispatch", err)
	}
	mustNotExist(t, st, "i1")
	if n := rec.count(startNotifyID); n != 1 {
		t.Fatalf("notify applied %d times by the failed Start, want 1", n)
	}

	delete(rec.failKinds, "demo.charge")
	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("ready")); err != nil {
		t.Fatalf("second Start = %v, want it to create the instance", err)
	}
	if _, err := durable.Recover(ctx, entryTwoEffectsMachine(), st, "i1", handlerOpt(rec)); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n := rec.count(startNotifyID); n != 2 {
		t.Errorf("notify applied %d times, want 2: Start again re-applies it", n)
	}
	if n := rec.count(startChargeID); n != 1 {
		t.Errorf("charge applied %d times, want 1: the retry's", n)
	}
}

// TestStart_BaselineCheckpointFails_RetryReappliesEffects: the effects apply before the
// baseline write, so a failed baseline Checkpoint leaves no instance, and the retried
// Start applies them again.
func TestStart_BaselineCheckpointFails_RetryReappliesEffects(t *testing.T) {
	ctx := context.Background()
	st := newStartContractStore()
	st.failCheckpoint = 1
	rec := newEffectRecorder()
	r := durable.NewRunner(entryNotifyMachine(), st, handlerOpt(rec))

	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("ready")); err == nil {
		t.Fatal("first Start = nil, want the Checkpoint failure")
	}
	if n := rec.count(startNotifyID); n != 1 {
		t.Fatalf("notify applied %d times before the failed baseline write, want 1", n)
	}
	mustNotExist(t, st, "i1")
	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("ready")); err != nil {
		t.Fatalf("retried Start: %v", err)
	}
	if n := rec.count(startNotifyID); n != 2 {
		t.Errorf("notify applied %d times across the retry, want 2", n)
	}
}

// TestStart_OverlappingStarts_EachAppliesBeforeTheStoreRefusesTheLoser: two Starts on
// one id whose probes both ran before either wrote each apply the effects; the Store
// then refuses the second baseline.
func TestStart_OverlappingStarts_EachAppliesBeforeTheStoreRefusesTheLoser(t *testing.T) {
	ctx := context.Background()
	st := newStartContractStore()
	st.lieLoads = 2
	rec := newEffectRecorder()
	r := durable.NewRunner(entryNotifyMachine(), st, handlerOpt(rec))

	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("ready")); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("ready")); !errors.Is(err, durable.ErrCheckpointNotAdvancing) {
		t.Fatalf("second Start = %v, want ErrCheckpointNotAdvancing", err)
	}
	if n := rec.count(startNotifyID); n != 2 {
		t.Errorf("notify applied %d times across overlapping Starts, want 2", n)
	}
}

// TestStart_OverlappingFencedOwners_EachAppliesBeforeTheStoreRefusesTheLoser: a stale
// owner (token 1) and a new one (token 2) whose probes both ran before either wrote
// each apply the effects, in either order, before the Store refuses the loser's
// baseline. The new owner's Recover applies none.
func TestStart_OverlappingFencedOwners_EachAppliesBeforeTheStoreRefusesTheLoser(t *testing.T) {
	cases := []struct {
		first, second int64
		wantLoserErr  error
	}{
		{2, 1, durable.ErrFenced},
		{1, 2, durable.ErrCheckpointNotAdvancing},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("token%d_then_token%d", tc.first, tc.second), func(t *testing.T) {
			ctx := context.Background()
			st := newStartContractStore()
			st.lieLoads = 2
			rec := newEffectRecorder()
			runner := func(tok int64) *durable.Runner[string, string, *runCtx] {
				return durable.NewRunner(entryTwoEffectsMachine(), st, handlerOpt(rec),
					durable.WithOwnerFence[string, string, *runCtx](tok))
			}
			if _, err := runner(tc.first).Start(ctx, "i1", &runCtx{}, state.WithInitialState("ready")); err != nil {
				t.Fatalf("Start(token %d): %v", tc.first, err)
			}
			if _, err := runner(tc.second).Start(ctx, "i1", &runCtx{}, state.WithInitialState("ready")); !errors.Is(err, tc.wantLoserErr) {
				t.Fatalf("Start(token %d) = %v, want %v", tc.second, err, tc.wantLoserErr)
			}
			if n := rec.count(startNotifyID); n != 2 {
				t.Errorf("notify applied %d times across both owners, want 2", n)
			}
			if _, err := durable.Recover(ctx, entryTwoEffectsMachine(), st, "i1", handlerOpt(rec),
				durable.WithOwnerFence[string, string, *runCtx](2)); err != nil {
				t.Fatalf("Recover(token 2): %v", err)
			}
			if n := rec.count(startNotifyID); n != 2 {
				t.Errorf("notify applied %d times after the takeover, want still 2: Recover applies none", n)
			}
		})
	}
}

// TestStart_InitialErr_AppliesNoEffects: an initial entry that failed applies no
// effects, at Start or at Recover.
func TestStart_InitialErr_AppliesNoEffects(t *testing.T) {
	ctx := context.Background()
	st := newStartContractStore()
	rec := newEffectRecorder()
	h, err := durable.NewRunner(failingEntryMachine(), st, handlerOpt(rec)).Start(ctx, "i1", &runCtx{}, state.WithInitialState("ready"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if h.Instance().InitialErr() == nil {
		t.Fatal("InitialErr = nil, want the entry's error")
	}
	if _, err = durable.Recover(ctx, failingEntryMachine(), st, "i1", handlerOpt(rec)); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n := rec.totalApplies(); n != 0 {
		t.Errorf("applied %d effects from a failed entry, want 0", n)
	}
}

// TestStart_HandlerWiredOnlyAtRecover_NeverSeesStartEffects: Start's effects are applied
// by Start or not at all. A handler first wired at Recover doesn't see them.
func TestStart_HandlerWiredOnlyAtRecover_NeverSeesStartEffects(t *testing.T) {
	ctx := context.Background()
	st := newStartContractStore()
	if _, err := durable.NewRunner(entryTwoEffectsMachine(), st).Start(ctx, "i1", &runCtx{}, state.WithInitialState("ready")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	rec := newEffectRecorder()
	if _, err := durable.Recover(ctx, entryTwoEffectsMachine(), st, "i1", handlerOpt(rec)); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n := rec.totalApplies(); n != 0 {
		t.Errorf("Recover's handler applied %d of Start's effects, want 0", n)
	}
}

// TestStart_HandlerSeesBaselineEffectIDsAndInstance_MarksNothing: at Start the handler
// gets each initial effect under its baseline id, "-1#<ordinal>#<kind>", with the
// instance on its ctx, and Start marks none of them dispatched.
func TestStart_HandlerSeesBaselineEffectIDsAndInstance_MarksNothing(t *testing.T) {
	ctx := context.Background()
	st := newStartContractStore()
	type call struct {
		inst durable.InstanceID
		ok   bool
		id   string
	}
	var calls []call
	handler := func(ctx context.Context, id string, _ state.Effect) error {
		inst, ok := durable.EffectInstance(ctx)
		calls = append(calls, call{inst, ok, id})
		return nil
	}
	if _, err := durable.NewRunner(entryTwoEffectsMachine(), st,
		durable.WithEffectHandler[string, string, *runCtx](handler)).Start(ctx, "i1", &runCtx{}, state.WithInitialState("ready")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	want := []call{{"i1", true, startNotifyID}, {"i1", true, startChargeID}}
	if len(calls) != len(want) || calls[0] != want[0] || calls[1] != want[1] {
		t.Fatalf("handler saw %+v, want %+v", calls, want)
	}
	if n := st.callCount("MarkDispatched"); n != 0 {
		t.Errorf("Start called MarkDispatched %d times, want none", n)
	}
	dispatched, err := st.Dispatched(ctx, "i1")
	if err != nil {
		t.Fatalf("Dispatched: %v", err)
	}
	if len(dispatched) != 0 {
		t.Errorf("Start marked %v dispatched, want none", dispatched)
	}
}
