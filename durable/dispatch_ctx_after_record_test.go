package durable_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// cancelAfterAppendStore is a MemStore that ends a step's context just after its
// Record is appended, as a host's deadline passing between the record and the
// dispatch would. It ends it once, when cancel is set.
type cancelAfterAppendStore struct {
	*durable.MemStore
	cancel *context.CancelFunc
}

func (s cancelAfterAppendStore) Append(ctx context.Context, id durable.InstanceID, rec durable.Record, opts ...durable.AppendOption) (int64, error) {
	n, err := s.MemStore.Append(ctx, id, rec, opts...)
	if *s.cancel != nil {
		(*s.cancel)()
		*s.cancel = nil
	}
	return n, err
}

// ctxNotingHandler is an effect handler that notes whether each effect's context had
// ended, and, when honor is set, refuses the effect with the context's error.
type ctxNotingHandler struct {
	honor bool
	ended []bool
}

func (c *ctxNotingHandler) handle(ctx context.Context, _ string, _ state.Effect) error {
	c.ended = append(c.ended, ctx.Err() != nil)
	if c.honor {
		return ctx.Err()
	}
	return nil
}

// effectFn is an EffectHandler's signature.
type effectFn = func(context.Context, string, state.Effect) error

// TestStep_AContextThatEndsAfterTheRecord_DispatchesAsFireDoes pins what an effect
// handler sees when the step's context ends after the step is recorded and before
// its effects are dispatched, and that Tick and RunService match Fire. The dispatch
// runs under the caller's context, not a detached one, on every path. A handler that
// ignores the context applies the effect and the step succeeds; one that honors it
// fails the step with ErrEffectDispatch and the Handle with it, and Recover then
// sends the effect once, since the step was recorded.
func TestStep_AContextThatEndsAfterTheRecord_DispatchesAsFireDoes(t *testing.T) {
	ctx := context.Background()
	type path struct {
		name string
		// run starts an instance on st, wired to handle, drives it to the step under
		// test, and runs that step on a context st ends after its append. It returns
		// the step's error, and a func that recovers the instance with another handler.
		run func(t *testing.T, st cancelAfterAppendStore, handle effectFn) (recoverWith func(effectFn) error, step error)
	}
	paths := []path{
		{"fire", func(t *testing.T, st cancelAfterAppendStore, handle effectFn) (func(effectFn) error, error) {
			h, err := durable.NewRunner(effectMachine(), st, durable.WithEffectHandler[string, string, *runCtx](handle)).
				Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			turn, cancel := context.WithCancel(ctx)
			*st.cancel = cancel
			_, stepErr := h.Fire(turn, "go")
			return func(hh effectFn) error {
				_, err := durable.Recover(ctx, effectMachine(), st, "i1", durable.WithEffectHandler[string, string, *runCtx](hh))
				return err
			}, stepErr
		}},
		{"tick", func(t *testing.T, st cancelAfterAppendStore, handle effectFn) (func(effectFn) error, error) {
			clk := state.NewFakeClock(epoch)
			opts := func(hh effectFn) []durable.Option[string, string, *runCtx] {
				return []durable.Option[string, string, *runCtx]{
					durable.WithEffectHandler[string, string, *runCtx](hh),
					durable.WithRunnerClock[string, string, *runCtx](clk),
				}
			}
			h, err := durable.NewRunner(timerNotifyMachine(), st, opts(func(context.Context, string, state.Effect) error { return nil })...).
				Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err = h.Fire(ctx, "go"); err != nil {
				t.Fatalf("Fire go: %v", err)
			}
			// The Fire's effect went to a no-op handler and is marked; only the Tick's
			// effect reaches handle, through a Handle recovered with it.
			h, err = durable.Recover(ctx, timerNotifyMachine(), st, "i1", opts(handle)...)
			if err != nil {
				t.Fatalf("Recover before the tick: %v", err)
			}
			clk.Advance(2 * time.Second)
			turn, cancel := context.WithCancel(ctx)
			*st.cancel = cancel
			_, stepErr := h.Tick(turn)
			return func(hh effectFn) error {
				_, err := durable.Recover(ctx, timerNotifyMachine(), st, "i1", opts(hh)...)
				return err
			}, stepErr
		}},
		{"service", func(t *testing.T, st cancelAfterAppendStore, handle effectFn) (func(effectFn) error, error) {
			var calls int64
			fn := counterService(&calls)
			reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
			h, err := durable.NewRunner(fetchThenWaitMachine(fn), st, reg, durable.WithEffectHandler[string, string, svcCtx](handle)).
				Start(ctx, "i1", svcCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err = h.Fire(ctx, "start"); err != nil {
				t.Fatalf("Fire start: %v", err)
			}
			turn, cancel := context.WithCancel(ctx)
			*st.cancel = cancel
			_, _, stepErr := h.RunService(turn, state.InvokeID("fetchThenWait", "loading", 0))
			return func(hh effectFn) error {
				_, err := durable.Recover(ctx, fetchThenWaitMachine(fn), st, "i1", reg, durable.WithEffectHandler[string, string, svcCtx](hh))
				return err
			}, stepErr
		}},
	}
	for _, p := range paths {
		t.Run(p.name+"/handler ignores ctx", func(t *testing.T) {
			var cancel context.CancelFunc
			st := cancelAfterAppendStore{durable.NewMemStore(), &cancel}
			live := &ctxNotingHandler{}
			recoverWith, stepErr := p.run(t, st, live.handle)
			if stepErr != nil {
				t.Fatalf("step = %v, want nil: the step was recorded, and the handler applied its effect", stepErr)
			}
			if len(live.ended) != 1 || !live.ended[0] {
				t.Fatalf("handler saw ended contexts %v, want [true]: the dispatch runs under the caller's context", live.ended)
			}
			healthy := newEffectRecorder()
			if err := recoverWith(healthy.handler()); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if len(healthy.order) != 0 {
				t.Fatalf("Recover sent %v, want nothing: live applied and marked the effect", healthy.order)
			}
		})
		t.Run(p.name+"/handler honors ctx", func(t *testing.T) {
			var cancel context.CancelFunc
			st := cancelAfterAppendStore{durable.NewMemStore(), &cancel}
			live := &ctxNotingHandler{honor: true}
			recoverWith, stepErr := p.run(t, st, live.handle)
			if !errors.Is(stepErr, durable.ErrEffectDispatch) || !errors.Is(stepErr, durable.ErrHandleFailed) || !errors.Is(stepErr, context.Canceled) {
				t.Fatalf("step = %v, want ErrEffectDispatch and ErrHandleFailed wrapping context.Canceled", stepErr)
			}
			healthy := newEffectRecorder()
			if err := recoverWith(healthy.handler()); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if len(healthy.order) != 1 {
				t.Fatalf("Recover sent %v, want the recorded step's one effect", healthy.order)
			}
		})
	}
}
