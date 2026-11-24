package state_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// These tests pin the abort-reporting half of the settle transform's contract,
// which the review of #268 surfaced: an abort is reported once for every settle
// that returns one, on every path that drives one — a host's SettleError, a
// Deliver (and the sendTo / sendParent / forwardTo sugar, which routes through
// the same delivery), and a Tick the host drives itself — through the
// WithSettleAbortObserver, whose report is the only channel a delivery's abort
// reaches. They pin the concurrency rule too: one settle owns an actor at a
// time, so a losing concurrent settle runs no transform, fires nothing and
// reports nothing, and the winner's settle is never rewritten under it.

// abortReports collects the SettleAborts an observer is handed, safe for the
// concurrent settles a test may drive.
type abortReports struct {
	mu      sync.Mutex
	reports []state.SettleAbort[string]
}

func (a *abortReports) add(sa state.SettleAbort[string]) {
	a.mu.Lock()
	a.reports = append(a.reports, sa)
	a.mu.Unlock()
}

func (a *abortReports) take() []state.SettleAbort[string] {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := a.reports
	a.reports = nil
	return out
}

// TestWithSettleAbortObserver_ReportsEachDrivingPath proves every path that
// drives a settle reports its abort to the observer exactly once, with the actor,
// the event that would have carried the outcome, and the abort itself: a host's
// SettleError, a Deliver, the sendTo sugar (which routes through the same
// dispatch the sendParent / forwardTo sugar does), and a Tick the host drives
// with a queued envelope of its own.
func TestWithSettleAbortObserver_ReportsEachDrivingPath(t *testing.T) {
	ctx := context.Background()
	abort := errors.New("no recording")

	t.Run("SettleError", func(t *testing.T) {
		reports := &abortReports{}
		parent, sys, id := settleHarness(t, func(string, any, error) (any, error, error) {
			return nil, nil, abort
		})
		sys.WithSettleAbortObserver(reports.add)
		res, ok := sys.SettleError(ctx, id, errors.New("boom"))
		if ok || !errors.Is(res.Err, abort) {
			t.Fatalf("SettleError = %+v, %v; want false with the abort in FireResult.Err", res, ok)
		}
		got := reports.take()
		if len(got) != 1 || got[0].ActorID != id || got[0].Event != "childErr" || got[0].Abort != abort {
			t.Fatalf("the observer got %+v, want one report for %q on childErr with the abort", got, id)
		}
		if parent.Current() != "supervising" || sys.IsRunning(id) != true {
			t.Fatalf("the aborted settle fired something or finished the actor: %q running=%v", parent.Current(), sys.IsRunning(id))
		}
	})

	t.Run("Deliver", func(t *testing.T) {
		reports := &abortReports{}
		parent, sys, id := settleHarness(t, func(string, any, error) (any, error, error) {
			return nil, nil, abort
		})
		sys.WithSettleAbortObserver(reports.add)
		if !sys.DeliverByID(ctx, id, "finish") {
			t.Fatal("DeliverByID returned false; the actor was running")
		}
		got := reports.take()
		if len(got) != 1 || got[0].ActorID != id || got[0].Event != "childDone" || got[0].Abort != abort {
			t.Fatalf("the observer got %+v, want one report for %q on childDone with the abort", got, id)
		}
		if parent.Current() != "supervising" {
			t.Fatalf("the aborted delivery fired something: parent = %q", parent.Current())
		}
	})

	t.Run("a sendTo another actor drives", func(t *testing.T) {
		reports := &abortReports{}
		// A parent whose supervising invokes two actors: the child, which routes its
		// completion, and a poker whose start sends the child its finishing event, as
		// the sendTo / sendParent / forwardTo sugar does through the same dispatch.
		m := state.ForgeFor[*trec]("abortpoker").
			Reducer("recordDone", func(in state.AssignCtx[*trec]) *trec {
				e := in.Entity
				if o, ok := in.Event.(string); ok {
					e.notes = append(e.notes, "result:"+o)
				}
				return e
			}).
			State("idle").
			State("supervising").
			InvokeActor("child", state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childErr")).
			InvokeActor("poker").
			State("complete").
			Initial("idle").
			Transition("idle").On("start").GoTo("supervising").
			Transition("supervising").On("childDone").GoTo("complete").Assign("recordDone").
			Quench()
		parent := m.Cast(&trec{}, state.WithInitialState[string]("idle"))
		child := settleChildBehavior()
		sys := state.NewActorSystem(parent).
			Register("child", child).
			Register("poker", func(map[string]any) (state.ActorInstance, error) {
				return &abortPoker{target: state.ActorID(m.Name(), "supervising", 0)}, nil
			})
		sys.WithSettleTransform(func(string, any, error) (any, error, error) { return nil, nil, abort })
		sys.WithSettleAbortObserver(reports.add)
		res := parent.Fire(ctx, "start")
		sys.Absorb(ctx, res.Effects)
		got := reports.take()
		if len(got) != 1 || got[0].ActorID != state.ActorID(m.Name(), "supervising", 0) || got[0].Event != "childDone" || got[0].Abort != abort {
			t.Fatalf("the observer got %+v, want one report for the child on childDone with the abort", got)
		}
		if parent.Current() != "supervising" {
			t.Fatalf("the aborted settle fired something: parent = %q", parent.Current())
		}
	})

	t.Run("a Tick the host drives", func(t *testing.T) {
		reports := &abortReports{}
		parent, sys, id := settleHarness(t, nil)
		started, release := make(chan struct{}), make(chan struct{})
		var firstErr error
		sys.WithSettleTransform(func(event string, output any, err error) (any, error, error) {
			if event == "childErr" {
				// The first settle (a SettleError below) blocks here, owning the
				// actor, so the delivery's envelope waits in the mailbox for the
				// host's own Tick.
				close(started)
				<-release
				return nil, nil, firstErr
			}
			return nil, nil, abort
		})
		sys.WithSettleAbortObserver(reports.add)
		firstErr = abort
		settled := make(chan struct{})
		go func() {
			defer close(settled)
			sys.SettleError(ctx, id, errors.New("boom"))
		}()
		<-started
		// The delivery queues its envelope; the settle in flight owns the actor, so
		// the delivery's drain leaves it for the host.
		if !sys.DeliverByID(ctx, id, "finish") {
			t.Fatal("DeliverByID returned false; the actor was running")
		}
		close(release)
		<-settled
		// The host's own Tick drives the queued envelope's settle, which aborts.
		results := sys.Tick(ctx, id)
		if len(results) != 1 || !errors.Is(results[0].Err, abort) {
			t.Fatalf("Tick = %+v, want the one aborted settle's FireResult", results)
		}
		got := reports.take()
		if len(got) != 2 {
			t.Fatalf("the observer got %d reports, want the SettleError's and the Tick's", len(got))
		}
		if got[0].Event != "childErr" || got[1].Event != "childDone" || got[1].Abort != abort || got[0].Abort != abort {
			t.Fatalf("the observer got %+v, want the SettleError's childErr report then the Tick's childDone one", got)
		}
		if parent.Current() != "supervising" {
			t.Fatalf("an aborted settle fired something: parent = %q", parent.Current())
		}
		if n := len(parent.Entity().notes); n != 0 {
			t.Fatalf("the aborted settles ran %d Assigns, want none", n)
		}
	})
}

// abortPoker is an actor that sends its target "finish" from its initial entry,
// as the sendTo action sugar does, then keeps running.
type abortPoker struct {
	target string
	sent   bool
}

func (*abortPoker) DeliverFire(context.Context, any) (bool, any) { return false, nil }
func (p *abortPoker) ChildEffects() []state.Effect {
	if p.sent {
		return nil
	}
	p.sent = true
	return []state.Effect{state.SendTo{TargetID: p.target, Event: "finish"}}
}
func (*abortPoker) Output() any { return nil }

// TestSettle_TwoConcurrentSettlesOnOneActor_OneSettles proves one settle owns an
// actor at a time: of two concurrent settles on one id — a delivery that settles
// the child's success and a SettleError whose transform aborts — exactly one
// settles it, the parent fires its onDone once and reaches complete, and the
// other runs no second transform against the winner's outcome: it either found
// the actor settled and returned nothing, or it owned the actor first, reported
// its abort while the actor was still running, and left it settleable for the
// delivery. Run with -race: the system's own state stays safe, and the loser
// never rewrites or aborts the winner's settle.
func TestSettle_TwoConcurrentSettlesOnOneActor_OneSettles(t *testing.T) {
	ctx := context.Background()
	for round := 0; round < 25; round++ {
		abort := errors.New("no recording")
		reports := &abortReports{}
		parent, sys, id := settleHarness(t, func(event string, output any, err error) (any, error, error) {
			if err != nil {
				return nil, nil, abort
			}
			return output, err, nil
		})
		sys.WithSettleAbortObserver(reports.add)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			sys.DeliverByID(ctx, id, "finish")
		}()
		go func() {
			defer wg.Done()
			sys.SettleError(ctx, id, errors.New("boom"))
		}()
		wg.Wait()
		// A delivery whose drain found the actor owned by the in-flight SettleError
		// left its envelope queued; the host's Tick settles it.
		sys.Tick(ctx, id)

		if parent.Current() != "complete" {
			t.Fatalf("round %d: parent = %q, want complete: the delivery's settle must always complete the actor", round, parent.Current())
		}
		if got := strings.Join(parent.Entity().notes, ","); got != "result:payload" {
			t.Fatalf("round %d: notes = %q, want the one settled onDone's result:payload", round, got)
		}
		if sys.IsRunning(id) || sys.Running() != 0 {
			t.Fatalf("round %d: the actor didn't settle: IsRunning(%v) Running %d", round, sys.IsRunning(id), sys.Running())
		}
		if got := reports.take(); len(got) > 1 {
			t.Fatalf("round %d: the observer got %+v, want at most the one abort the owning settle reported", round, got)
		}
	}
}

// TestSettle_ATransformSettlingItsOwnActorIsRefused proves the settle that runs a
// transform owns the actor for its duration: a transform that settles its own
// actor — reentrant on the same goroutine — gets nothing back and runs no second
// transform, and the abort the owning settle reports is the only one. Without the
// ownership the nested settle would recurse through the transform a second time.
func TestSettle_ATransformSettlingItsOwnActorIsRefused(t *testing.T) {
	ctx := context.Background()
	abort := errors.New("no recording")
	reports := &abortReports{}
	var sys *state.ActorSystem[string, string, *trec]
	var id string
	calls := 0
	parent, sys, id := settleHarness(t, func(event string, output any, err error) (any, error, error) {
		calls++
		if calls == 1 {
			// Reentrant: settle the same actor while its settle runs.
			res, ok := sys.SettleError(ctx, id, errors.New("inner"))
			if ok || res.Err != nil {
				t.Errorf("the nested SettleError = %+v, %v; want nothing: the outer settle owns the actor", res, ok)
			}
		}
		return nil, nil, abort
	})
	sys.WithSettleAbortObserver(reports.add)
	if res, ok := sys.SettleError(ctx, id, errors.New("boom")); ok || !errors.Is(res.Err, abort) {
		t.Fatalf("SettleError = %+v, %v; want false with the abort in FireResult.Err", res, ok)
	}
	if calls != 1 {
		t.Fatalf("the transform ran %d times, want once: the nested settle was refused, not recursed", calls)
	}
	if got := reports.take(); len(got) != 1 || got[0].Abort != abort {
		t.Fatalf("the observer got %+v, want the one report the owning settle made", got)
	}
	if parent.Current() != "supervising" {
		t.Fatalf("the aborted settle fired something: parent = %q", parent.Current())
	}
}

// TestWithSettleTransform_ARewrittenFailureNothingRoutesEscalates proves a
// transform can't make a child's failure vanish: a failure whose settle the
// transform rewrote into a success nothing routes — the invocation wired an
// onError but no onDone — escalates the ORIGINAL error to the parent, as the
// failure does with no transform at all, rather than settling a success nothing
// consumes and nothing observes. LastOutput still reports the rewritten outcome.
func TestWithSettleTransform_ARewrittenFailureNothingRoutesEscalates(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[*trec]("vanish").
		State("idle").
		State("supervising").
		Initial("idle").
		Transition("idle").On("start").GoTo("supervising").Spawn("child", "vanish-kid", state.WithSpawnOnError("childErr")).
		Quench()
	parent := m.Cast(&trec{}, state.WithInitialState[string]("idle"))
	sys := state.NewActorSystem(parent).Register("child", settleChildBehavior())
	sys.WithSettleTransform(func(event string, output any, err error) (any, error, error) {
		if err != nil {
			// Recover it into a success nothing routes.
			return "recovered", nil, nil
		}
		return output, err, nil
	})
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	if parent.Current() != "supervising" {
		t.Fatalf("parent state = %q, want supervising", parent.Current())
	}

	routed, ok := sys.SettleError(ctx, "vanish-kid", errors.New("boom"))
	if ok || routed.Err != nil {
		t.Fatalf("SettleError = %+v, %v; want nothing routed: the rewritten outcome has no onDone", routed, ok)
	}
	if parent.Current() != "supervising" {
		t.Fatalf("parent state = %q, want supervising: nothing fired", parent.Current())
	}
	if n := len(parent.Entity().notes); n != 0 {
		t.Fatalf("the settle ran %d Assigns, want none", n)
	}
	// The ORIGINAL failure escalated — a transform may rewrite the outcome, not
	// erase the failure — and the rewritten outcome is what LastOutput reports.
	esc := sys.LastEscalation()
	if esc == nil || esc.ActorID != "vanish-kid" || esc.Err == nil || esc.Err.Error() != "boom" {
		t.Fatalf("LastEscalation = %+v, want the ORIGINAL boom escalated for the vanished failure", esc)
	}
	if out, got := sys.LastOutput(); !got || out != "recovered" {
		t.Fatalf("LastOutput = %v, %v; want the rewritten outcome \"recovered\"", out, got)
	}
	if sys.IsRunning("vanish-kid") {
		t.Fatal("the actor didn't settle")
	}
}

// TestSettleError_AnAlreadySettledActorIsRefused pins the refusal the settle
// recheck gives: SettleError on an actor a previous settle already settled — not
// stopped, still registered — returns the zero result and false, fires nothing
// and escalates nothing, whether or not a transform is set. On main it re-settled
// the actor through onError, moving the parent again.
func TestSettleError_AnAlreadySettledActorIsRefused(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		transform func(string, any, error) (any, error, error)
	}{
		{"no transform", nil},
		{"a transform", func(event string, output any, err error) (any, error, error) { return output, err, nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, sys, id := settleHarness(t, tc.transform)
			if !sys.DeliverByID(ctx, id, "finish") {
				t.Fatal("DeliverByID returned false; the actor was running")
			}
			if parent.Current() != "complete" {
				t.Fatalf("parent state = %q, want complete after the settled success", parent.Current())
			}
			notes := len(parent.Entity().notes)

			res, ok := sys.SettleError(ctx, id, errors.New("boom"))
			if ok || res.Err != nil {
				t.Fatalf("SettleError on the settled actor = %+v, %v; want the zero result and false: the actor is settled", res, ok)
			}
			if parent.Current() != "complete" || len(parent.Entity().notes) != notes {
				t.Fatalf("the second settle moved the parent: %q with %d notes, want complete with %d", parent.Current(), len(parent.Entity().notes), notes)
			}
			if esc := sys.LastEscalation(); esc != nil {
				t.Fatalf("the refused settle escalated: %+v", esc)
			}
		})
	}
}

// TestWithSettleTransform_AnUnboundActorErrorStaysAnError pins the keep in the
// transform's application: a settle handed an *UnboundActorError stays an error
// even when the transform returns no error for it, so it settles through onError
// — a transform can't turn an unbound spawn's failure into a success.
func TestWithSettleTransform_AnUnboundActorErrorStaysAnError(t *testing.T) {
	ctx := context.Background()
	parent, sys, id := settleHarness(t, func(string, any, error) (any, error, error) {
		// Refuse to keep the error: the keep must override this.
		return "recovered", nil, nil
	})
	unbound := &state.UnboundActorError{Name: "child"}
	res, ok := sys.SettleError(ctx, id, unbound)
	if !ok || res.Err != nil {
		t.Fatalf("SettleError = %+v, %v; want the unbound failure routed through onError", res, ok)
	}
	if parent.Current() != "failed" {
		t.Fatalf("parent state = %q, want failed: the unbound error stayed an error", parent.Current())
	}
	if err := sys.LastError(); err == nil || !errors.Is(err, unbound) {
		t.Fatalf("LastError = %v, want the unbound error the settle kept", err)
	}
	if got := strings.Join(parent.Entity().notes, ","); !strings.Contains(got, "child") {
		t.Fatalf("notes = %q, want the unbound error's message recorded", got)
	}
}

// panickingStepChild is an actor that panics while stepping the event it's
// delivered: its Fire is guarded by the system, and the panic settles the actor
// as a failure, whose settle the transform below aborts.
type panickingStepChild struct{}

func (*panickingStepChild) DeliverFire(_ context.Context, ev any) (bool, any) {
	if ev == "finish" {
		panic("ka-boom")
	}
	return false, nil
}
func (*panickingStepChild) ChildEffects() []state.Effect { return nil }
func (*panickingStepChild) Output() any                  { return nil }

// TestTick_APanickingActorsAbortedSettleReturnsTheAbort pins the panic branch of
// Tick: a child that panics while stepping settles as a failure, and a transform
// that aborts that settle makes Tick return the abort as its one FireResult — the
// outcome a host driving Tick itself reads — with the actor left running and the
// abort reported to the observer.
func TestTick_APanickingActorsAbortedSettleReturnsTheAbort(t *testing.T) {
	ctx := context.Background()
	abort := errors.New("no recording")
	reports := &abortReports{}
	m := settleParentMachine()
	parent := m.Cast(&trec{}, state.WithInitialState[string]("idle"))
	sys := state.NewActorSystem(parent).Register("child", func(map[string]any) (state.ActorInstance, error) {
		return &panickingStepChild{}, nil
	})
	id := state.ActorID(m.Name(), "supervising", 0)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	sys.WithSettleTransform(func(event string, output any, err error) (any, error, error) {
		if event == "childErr" {
			// The first settle (a SettleError below) blocks here, owning the actor,
			// so the delivery's envelope waits in the mailbox for the host's Tick;
			// the panicking step's settle aborts at once.
			once.Do(func() {
				close(started)
				<-release
			})
		}
		return nil, nil, abort
	})
	sys.WithSettleAbortObserver(reports.add)
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	settled := make(chan struct{})
	go func() {
		defer close(settled)
		sys.SettleError(ctx, id, errors.New("boom"))
	}()
	<-started
	if !sys.DeliverByID(ctx, id, "finish") {
		t.Fatal("DeliverByID returned false; the actor was running")
	}
	close(release)
	<-settled
	results := sys.Tick(ctx, id)
	if len(results) != 1 || !errors.Is(results[0].Err, abort) {
		t.Fatalf("Tick = %+v, want the one aborted settle's FireResult for the panicking step", results)
	}
	got := reports.take()
	if len(got) != 2 || got[1].Event != "childErr" || got[1].Abort != abort {
		t.Fatalf("the observer got %+v, want the SettleError's report then the panicking step's childErr one", got)
	}
	if parent.Current() != "supervising" || !sys.IsRunning(id) {
		t.Fatalf("the aborted settle fired something or finished the actor: %q running=%v", parent.Current(), sys.IsRunning(id))
	}
	if n := len(parent.Entity().notes); n != 0 {
		t.Fatalf("the aborted settles ran %d Assigns, want none", n)
	}
}
