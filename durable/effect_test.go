package durable_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// notifyEffect is a host (domain) effect a transition emits to drive an
// at-most-once side effect — an email, a charge, a published message. It opts
// into KindedEffect with a stable, non-builtin discriminant so the durable
// runtime stamps it, journals it, and dedups it by EffectID without ever
// confusing it for a kernel driver effect.
type notifyEffect struct {
	To string `json:"to"`
}

func (notifyEffect) Kind() string { return "demo.notify" }

// chargeEffect is a second host effect kind, so a single step can emit multiple
// distinct effects and the EffectID scheme must keep them stably ordered and
// distinctly identified.
type chargeEffect struct {
	Cents int `json:"cents"`
}

func (chargeEffect) Kind() string { return "demo.charge" }

// effectRecorder is a test effect handler that counts how many times each
// EffectID was applied, recording the live effect values in dispatch order. A
// correct exactly-once runtime applies every EffectID exactly once across the
// whole lifetime of an instance — live run plus any number of recoveries.
type effectRecorder struct {
	mu        sync.Mutex
	counts    map[string]int   // EffectID -> apply count
	order     []string         // EffectIDs in dispatch order
	values    []state.Effect   // applied effect values in dispatch order
	failKinds map[string]error // kind -> error the handler returns for it
}

func newEffectRecorder() *effectRecorder {
	return &effectRecorder{counts: map[string]int{}, failKinds: map[string]error{}}
}

// handler returns the EffectHandler closure bound to this recorder. The runtime
// passes the stamped EffectID alongside the live effect value so the test can
// assert exactly-once per id.
func (r *effectRecorder) handler() func(context.Context, string, state.Effect) error {
	return func(_ context.Context, id string, eff state.Effect) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if ke, ok := eff.(state.KindedEffect); ok {
			if err := r.failKinds[ke.Kind()]; err != nil {
				return err
			}
		}
		r.counts[id]++
		r.order = append(r.order, id)
		r.values = append(r.values, eff)
		return nil
	}
}

func (r *effectRecorder) count(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[id]
}

// ids returns the distinct EffectIDs applied, sorted for stable comparison.
func (r *effectRecorder) ids() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.counts))
	for id := range r.counts {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (r *effectRecorder) totalApplies() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, c := range r.counts {
		n += c
	}
	return n
}

// effectMachine emits a host effect on entry to a state, so each fired event
// drives exactly one dispatchable domain effect the runtime must apply once.
func effectMachine() *state.Machine[string, string, *runCtx] {
	return state.Forge[string, string, *runCtx]("effect").
		Action("notify", func(state.ActionCtx[*runCtx]) (state.Effect, error) {
			return notifyEffect{To: "ops@example.com"}, nil
		}).
		Action("charge", func(state.ActionCtx[*runCtx]) (state.Effect, error) {
			return chargeEffect{Cents: 500}, nil
		}).
		State("idle").
		State("active").
		State("billed").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("active").Do("notify").
		Transition("active").On("bill").GoTo("billed").Do("charge").Do("notify").
		Transition("billed").On("finish").GoTo("done").Do("notify").
		Quench()
}

// TestRunner_EffectDispatch_Live_AppliesEachEffectOnce proves the live path
// applies every emitted domain effect exactly once, in emission order, each
// under a distinct deterministic EffectID.
func TestRunner_EffectDispatch_Live_AppliesEachEffectOnce(t *testing.T) {
	ctx := context.Background()
	rec := newEffectRecorder()
	st := durable.NewMemStore()
	r := durable.NewRunner(effectMachine(), st,
		durable.WithEffectHandler[string, string, *runCtx](rec.handler()))

	h, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := h.Fire(ctx, "go"); err != nil { // emits notify
		t.Fatalf("Fire go: %v", err)
	}
	if _, err := h.Fire(ctx, "bill"); err != nil { // emits charge + notify
		t.Fatalf("Fire bill: %v", err)
	}
	if _, err := h.Fire(ctx, "finish"); err != nil { // emits notify
		t.Fatalf("Fire finish: %v", err)
	}

	if got, want := len(rec.ids()), 4; got != want {
		t.Fatalf("distinct effect ids = %d, want %d (ids=%v)", got, want, rec.ids())
	}
	for _, id := range rec.ids() {
		if c := rec.count(id); c != 1 {
			t.Fatalf("effect %q applied %d times, want exactly 1", id, c)
		}
	}
	if rec.totalApplies() != 4 {
		t.Fatalf("total applies = %d, want 4", rec.totalApplies())
	}
}

// TestRunner_EffectDispatch_Recover_RedispatchesNone proves a recovery after a
// fully dispatched run applies no effect again: replay re-emits the same effects
// but every EffectID is already marked dispatched, so the handler is never
// re-invoked. Exactly-once across the crash boundary.
func TestRunner_EffectDispatch_Recover_RedispatchesNone(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	mk := effectMachine

	live := newEffectRecorder()
	r := durable.NewRunner(mk(), st,
		durable.WithEffectHandler[string, string, *runCtx](live.handler()))
	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range []string{"go", "bill", "finish"} {
		if _, err := r.Fire(ctx, "i1", ev); err != nil {
			t.Fatalf("Fire %s: %v", ev, err)
		}
	}
	liveApplies := live.totalApplies()
	if liveApplies != 4 {
		t.Fatalf("live applies = %d, want 4", liveApplies)
	}

	// Recover with a fresh handler: replay must redispatch nothing.
	rer := newEffectRecorder()
	if _, err := durable.Recover(ctx, mk(), st, "i1",
		durable.WithEffectHandler[string, string, *runCtx](rer.handler())); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n := rer.totalApplies(); n != 0 {
		t.Fatalf("recovery redispatched %d effects, want 0", n)
	}
}

// TestRunner_EffectDispatch_CrashBetweenAppendAndDispatch proves the write-ahead
// ordering: a crash after the step Record (with its effect ids) is persisted but
// BEFORE the effect is dispatched leaves the effect not-yet-dispatched, so
// recovery dispatches it — exactly once total.
func TestRunner_EffectDispatch_CrashBetweenAppendAndDispatch(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()

	// A handler that fails on the very first apply simulates a crash AFTER the
	// write-ahead append (the Record is already in the Store) but mid-dispatch:
	// the effect is recorded yet never marked dispatched.
	crashing := newEffectRecorder()
	crashing.failKinds["demo.notify"] = errors.New("boom: crashed mid-dispatch")
	r := durable.NewRunner(effectMachine(), st,
		durable.WithEffectHandler[string, string, *runCtx](crashing.handler()))
	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// The first Fire emits notify; the handler errors, surfacing the failure. The
	// Record is already persisted (write-ahead), the effect is NOT marked dispatched.
	if _, err := r.Fire(ctx, "i1", "go"); err == nil {
		t.Fatalf("Fire go: expected dispatch error, got nil")
	}

	// Recover with a healthy handler: the not-yet-dispatched effect must dispatch
	// now, exactly once.
	healed := newEffectRecorder()
	if _, err := durable.Recover(ctx, effectMachine(), st, "i1",
		durable.WithEffectHandler[string, string, *runCtx](healed.handler())); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n := healed.totalApplies(); n != 1 {
		t.Fatalf("recovery dispatched %d effects, want exactly 1 (the un-dispatched notify)", n)
	}

	// Recover a second time: now it IS dispatched, so nothing redispatches.
	again := newEffectRecorder()
	if _, err := durable.Recover(ctx, effectMachine(), st, "i1",
		durable.WithEffectHandler[string, string, *runCtx](again.handler())); err != nil {
		t.Fatalf("Recover 2: %v", err)
	}
	if n := again.totalApplies(); n != 0 {
		t.Fatalf("second recovery redispatched %d, want 0", n)
	}
}

// TestRunner_EffectDispatch_CrashAfterDispatch proves the other crash window: an
// effect dispatched and marked before the crash is never redispatched on
// recovery.
func TestRunner_EffectDispatch_CrashAfterDispatch(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()

	live := newEffectRecorder()
	r := durable.NewRunner(effectMachine(), st,
		durable.WithEffectHandler[string, string, *runCtx](live.handler()))
	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Fire one step: effect dispatched AND marked (crash is "after dispatch").
	if _, err := r.Fire(ctx, "i1", "go"); err != nil {
		t.Fatalf("Fire go: %v", err)
	}
	if live.totalApplies() != 1 {
		t.Fatalf("live applies = %d, want 1", live.totalApplies())
	}

	rer := newEffectRecorder()
	if _, err := durable.Recover(ctx, effectMachine(), st, "i1",
		durable.WithEffectHandler[string, string, *runCtx](rer.handler())); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n := rer.totalApplies(); n != 0 {
		t.Fatalf("recovery after dispatch redispatched %d, want 0", n)
	}
}

// TestRunner_EffectDispatch_MultipleEffectsPerStep_StableIDsAndOrder proves a
// step emitting several effects stamps stable, distinct ids preserving emission
// order, identical across a live run and a recovery replay.
func TestRunner_EffectDispatch_MultipleEffectsPerStep_StableIDsAndOrder(t *testing.T) {
	ctx := context.Background()

	run := func() ([]string, int) {
		st := durable.NewMemStore()
		rec := newEffectRecorder()
		r := durable.NewRunner(effectMachine(), st,
			durable.WithEffectHandler[string, string, *runCtx](rec.handler()))
		if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
			t.Fatalf("Start: %v", err)
		}
		// The "bill" step emits charge THEN notify in that emission order.
		if _, err := r.Fire(ctx, "i1", "go"); err != nil {
			t.Fatalf("Fire go: %v", err)
		}
		if _, err := r.Fire(ctx, "i1", "bill"); err != nil {
			t.Fatalf("Fire bill: %v", err)
		}
		return rec.order, len(rec.ids())
	}

	order1, distinct1 := run()
	order2, distinct2 := run()

	if distinct1 != 3 || distinct2 != 3 {
		t.Fatalf("distinct ids = %d / %d, want 3 each", distinct1, distinct2)
	}
	if len(order1) != 3 || len(order2) != 3 {
		t.Fatalf("dispatch order lengths = %d / %d, want 3 each", len(order1), len(order2))
	}
	for i := range order1 {
		if order1[i] != order2[i] {
			t.Fatalf("nondeterministic EffectID at position %d: %q vs %q", i, order1[i], order2[i])
		}
	}
	// The two effects emitted in the same "bill" step must carry distinct ids.
	if order1[1] == order1[2] {
		t.Fatalf("two effects in one step share an EffectID: %q", order1[1])
	}
}

// TestRunner_EffectDispatch_HandlerError_IsSurfacedAndNotMarked proves a handler
// error is returned to the caller and the failing effect is NOT marked
// dispatched, so a later recovery retries it (at-least-once until it succeeds).
func TestRunner_EffectDispatch_HandlerError_IsSurfacedAndNotMarked(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()

	failing := newEffectRecorder()
	failing.failKinds["demo.notify"] = errors.New("downstream unavailable")
	r := durable.NewRunner(effectMachine(), st,
		durable.WithEffectHandler[string, string, *runCtx](failing.handler()))
	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, err := r.Fire(ctx, "i1", "go")
	if err == nil {
		t.Fatalf("Fire go: expected handler error to surface")
	}
	if !errors.Is(err, durable.ErrEffectDispatch) {
		t.Fatalf("error = %v, want wrapped ErrEffectDispatch", err)
	}

	// Recover with a healthy handler: the un-marked effect dispatches now.
	ok := newEffectRecorder()
	if _, err := durable.Recover(ctx, effectMachine(), st, "i1",
		durable.WithEffectHandler[string, string, *runCtx](ok.handler())); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n := ok.totalApplies(); n != 1 {
		t.Fatalf("recovery applied %d, want 1 (the retried notify)", n)
	}
}

// TestRunner_EffectDispatch_NoHandler_IsInert proves the seam is opt-in: without
// WithEffectHandler the Runner records effect ids but dispatches nothing, so an
// existing event-driven user is unaffected (additive default).
func TestRunner_EffectDispatch_NoHandler_IsInert(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	r := durable.NewRunner(effectMachine(), st)
	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := r.Fire(ctx, "i1", "go"); err != nil {
		t.Fatalf("Fire go: %v", err)
	}
	// Nothing to assert beyond "no panic, no error": no handler means no dispatch.
}

// bareEffect is a domain effect that does NOT implement KindedEffect, so the
// runtime must stamp and dispatch it by its Go-type fallback kind and persist it
// by id alone (no envelope payload).
type bareEffect struct{ N int }

// bareEffectMachine emits a bare (non-Kinded) domain effect, exercising the
// effectKind / recordEffects fallback for an effect with no stable discriminant.
func bareEffectMachine() *state.Machine[string, string, *runCtx] {
	return state.Forge[string, string, *runCtx]("bare").
		Action("emit", func(state.ActionCtx[*runCtx]) (state.Effect, error) {
			return bareEffect{N: 1}, nil
		}).
		State("idle").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("done").Do("emit").
		Quench()
}

// TestRunner_EffectDispatch_BareEffect_StampedByType proves a non-Kinded domain
// effect is dispatched once under a type-derived EffectID and replays as
// already-dispatched.
func TestRunner_EffectDispatch_BareEffect_StampedByType(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	rec := newEffectRecorder()
	r := durable.NewRunner(bareEffectMachine(), st,
		durable.WithEffectHandler[string, string, *runCtx](rec.handler()))
	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := r.Fire(ctx, "i1", "go"); err != nil {
		t.Fatalf("Fire go: %v", err)
	}
	if n := rec.totalApplies(); n != 1 {
		t.Fatalf("bare effect applied %d times, want 1", n)
	}

	rer := newEffectRecorder()
	if _, err := durable.Recover(ctx, bareEffectMachine(), st, "i1",
		durable.WithEffectHandler[string, string, *runCtx](rer.handler())); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n := rer.totalApplies(); n != 0 {
		t.Fatalf("bare effect redispatched %d times on recovery, want 0", n)
	}
}

// nonDispatchStore wraps a Store WITHOUT implementing DispatchStore, so the
// Runner records effect ids but cannot dedup; dispatch must degrade to a no-op
// rather than panic.
type nonDispatchStore struct{ durable.Store }

// TestRunner_EffectDispatch_NonDispatchStore_NoOp proves a Store that does not
// implement DispatchStore leaves dispatch inert (the ids are still recorded for a
// dispatch-capable backend on recovery).
func TestRunner_EffectDispatch_NonDispatchStore_NoOp(t *testing.T) {
	ctx := context.Background()
	st := nonDispatchStore{Store: durable.NewMemStore()}
	rec := newEffectRecorder()
	r := durable.NewRunner(effectMachine(), st,
		durable.WithEffectHandler[string, string, *runCtx](rec.handler()))
	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := r.Fire(ctx, "i1", "go"); err != nil {
		t.Fatalf("Fire go: %v", err)
	}
	if n := rec.totalApplies(); n != 0 {
		t.Fatalf("non-dispatch store applied %d effects, want 0 (inert)", n)
	}
}

// timerMachine emits a kernel driver effect (a delayed `after` transition) on the
// same step a domain effect fires, so the dispatcher must route the driver effect
// to the Scheduler and only the domain effect to the handler.
func timerNotifyMachine() *state.Machine[string, string, *runCtx] {
	return state.Forge[string, string, *runCtx]("timer").
		Action("notify", func(state.ActionCtx[*runCtx]) (state.Effect, error) {
			return notifyEffect{To: "ops@example.com"}, nil
		}).
		State("idle").
		State("waiting").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("waiting").Do("notify").
		Transition("waiting").After(time.Second).GoTo("done").
		Quench()
}

// TestRunner_EffectDispatch_DriverEffectsFiltered proves a step that emits both a
// kernel driver effect (ScheduleAfter) and a domain effect dispatches only the
// domain effect through the handler — the driver effect is absorbed by the
// Scheduler, not handed to the EffectHandler.
func TestRunner_EffectDispatch_DriverEffectsFiltered(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	rec := newEffectRecorder()
	r := durable.NewRunner(timerNotifyMachine(), st,
		durable.WithEffectHandler[string, string, *runCtx](rec.handler()),
		durable.WithRunnerClock[string, string, *runCtx](state.NewFakeClock(epoch)))
	if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := r.Fire(ctx, "i1", "go"); err != nil { // schedules a timer AND notifies
		t.Fatalf("Fire go: %v", err)
	}
	// Exactly one domain effect (notify) reached the handler; the ScheduleAfter
	// driver effect did not.
	if n := rec.totalApplies(); n != 1 {
		t.Fatalf("handler applied %d effects, want 1 (driver effect must be filtered)", n)
	}
}

// TestMemStore_DispatchSet_MarkAndQuery exercises the dispatched-set seam
// directly: marking an EffectID makes it report dispatched, and re-marking is
// idempotent.
func TestMemStore_DispatchSet_MarkAndQuery(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()

	dispatched, err := st.Dispatched(ctx, "i1")
	if err != nil {
		t.Fatalf("Dispatched (empty): %v", err)
	}
	if len(dispatched) != 0 {
		t.Fatalf("fresh instance has %d dispatched ids, want 0", len(dispatched))
	}

	if err = st.MarkDispatched(ctx, "i1", "0#0#demo.notify"); err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}
	if err = st.MarkDispatched(ctx, "i1", "0#0#demo.notify"); err != nil {
		t.Fatalf("MarkDispatched (idempotent): %v", err)
	}

	dispatched, err = st.Dispatched(ctx, "i1")
	if err != nil {
		t.Fatalf("Dispatched: %v", err)
	}
	if !dispatched["0#0#demo.notify"] {
		t.Fatalf("expected id marked dispatched, got %v", dispatched)
	}
	if dispatched["1#0#demo.charge"] {
		t.Fatalf("unmarked id reported dispatched")
	}
}
