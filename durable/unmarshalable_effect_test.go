package durable_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// badEffect is a KindedEffect with no JSON form: encoding/json can't encode a chan.
type badEffect struct {
	Done chan struct{} `json:"done"`
}

func (badEffect) Kind() string { return "demo.bad" }

func emitBad[C any](state.ActionCtx[C]) (state.Effect, error) {
	return badEffect{Done: make(chan struct{})}, nil
}

// badEffectMachine emits a badEffect on "go", on the timer that follows, and on the
// settle of the service "loading" invokes, so each kind of step can emit one.
func badEffectMachine(fn state.ServiceFn[svcCtx]) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("bad").
		Service("fetch", fn).
		Action("bad", emitBad[svcCtx]).
		State("idle").
		State("waiting").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("ok")).
		State("done").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("done").Do("bad").
		Transition("idle").On("wait").GoTo("waiting").
		Transition("waiting").After(time.Second).GoTo("done").Do("bad").
		Transition("idle").On("load").GoTo("loading").
		Transition("loading").On("ok").GoTo("done").Do("bad").
		Quench()
}

// badRun is a Runner set up for badEffectMachine with no effect handler, and the count
// of ServiceFn runs.
type badRun struct {
	m     *state.Machine[string, string, svcCtx]
	store *durable.MemStore
	clk   *state.FakeClock
	opts  []durable.Option[string, string, svcCtx]
	runs  *int64
}

func newBadRun() *badRun {
	runs := new(int64)
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		atomic.AddInt64(runs, 1)
		return "ok", nil
	}
	clk := state.NewFakeClock(epoch)
	return &badRun{
		m:     badEffectMachine(fn),
		store: durable.NewMemStore(),
		clk:   clk,
		runs:  runs,
		opts: []durable.Option[string, string, svcCtx]{
			durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
			durable.WithRunnerClock[string, string, svcCtx](clk),
		},
	}
}

func (b *badRun) start(t *testing.T) *durable.Handle[string, string, svcCtx] {
	t.Helper()
	h, err := durable.NewRunner(b.m, b.store, b.opts...).Start(context.Background(), "bad-1", svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return h
}

func (b *badRun) recover(t *testing.T) *durable.Handle[string, string, svcCtx] {
	t.Helper()
	h, err := durable.Recover(context.Background(), b.m, b.store, "bad-1", b.opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	return h
}

// metaMarshalError is the EffectEnvelope.Meta key that holds why an effect's payload is
// missing.
const metaMarshalError = "crucible.marshalError"

// mustRecordBadByIDAndKind fails unless the journal's Record at step names the
// badEffect it emitted by id and kind, with no payload and its marshal error in Meta.
func mustRecordBadByIDAndKind(t *testing.T, st *durable.MemStore, id durable.InstanceID, step int) {
	t.Helper()
	_, recs, err := st.Load(context.Background(), id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, rec := range recs {
		if rec.Step != step {
			continue
		}
		if len(rec.Effects) != 1 {
			t.Fatalf("step %d records %d effects, want the badEffect", step, len(rec.Effects))
		}
		env := rec.Effects[0]
		wantID := fmt.Sprintf("%d#0#demo.bad", step)
		if env.Kind != "demo.bad" || env.EffectID != wantID || len(env.Payload) != 0 {
			t.Fatalf("step %d records %+v, want kind demo.bad, id %s and no payload", step, env, wantID)
		}
		if msg, _ := env.Meta[metaMarshalError].(string); !strings.Contains(msg, "unsupported type") {
			t.Fatalf("step %d Meta = %v, want the marshal error under %q", step, env.Meta, metaMarshalError)
		}
		return
	}
	t.Fatalf("no Record at step %d", step)
}

// TestUnmarshalableEffect_NoHandler_RecordsByIDAndKind: with no WithEffectHandler wired,
// a step that emits a KindedEffect with no JSON form records it by id and kind, as an
// effect that isn't a KindedEffect is, with its marshal error in Meta, and succeeds,
// whatever the step: Fire, a Tick's timer, a RunService's settle, or the stateless
// Runner.Fire. On main a Fire failed this way; a Tick and a RunService didn't, since
// they recorded no effects. The ServiceFn runs once.
func TestUnmarshalableEffect_NoHandler_RecordsByIDAndKind(t *testing.T) {
	ctx := context.Background()
	t.Run("Fire", func(t *testing.T) {
		b := newBadRun()
		if _, err := b.start(t).Fire(ctx, "go"); err != nil {
			t.Fatalf("Fire: %v", err)
		}
		mustRecordBadByIDAndKind(t, b.store, "bad-1", 0)
		if got := b.recover(t).Instance().Current(); got != "done" {
			t.Fatalf("recovered in %q, want done", got)
		}
	})
	t.Run("stateless Fire", func(t *testing.T) {
		b := newBadRun()
		b.start(t)
		if _, err := durable.NewRunner(b.m, b.store, b.opts...).Fire(ctx, "bad-1", "go"); err != nil {
			t.Fatalf("Runner.Fire: %v", err)
		}
		mustRecordBadByIDAndKind(t, b.store, "bad-1", 0)
	})
	t.Run("Tick", func(t *testing.T) {
		b := newBadRun()
		if _, err := b.start(t).Fire(ctx, "wait"); err != nil {
			t.Fatalf("Fire wait: %v", err)
		}
		b.clk.Advance(2 * time.Second)
		if _, err := b.recover(t).Tick(ctx); err != nil {
			t.Fatalf("Tick: %v", err)
		}
		mustRecordBadByIDAndKind(t, b.store, "bad-1", 1)
		if got := b.recover(t).Instance().Current(); got != "done" {
			t.Fatalf("recovered in %q, want done", got)
		}
	})
	t.Run("RunService", func(t *testing.T) {
		b := newBadRun()
		if _, err := b.start(t).Fire(ctx, "load"); err != nil {
			t.Fatalf("Fire load: %v", err)
		}
		if _, ran, err := b.recover(t).RunService(ctx, state.InvokeID("bad", "loading", 0)); !ran || err != nil {
			t.Fatalf("RunService = %v, %v; want ran", ran, err)
		}
		mustRecordBadByIDAndKind(t, b.store, "bad-1", 1)
		if got := b.recover(t).Instance().Current(); got != "done" {
			t.Fatalf("recovered in %q, want done", got)
		}
		if got := atomic.LoadInt64(b.runs); got != 1 {
			t.Fatalf("the ServiceFn ran %d times, want 1", got)
		}
	})
}

// badSteps drives b's instance to emit a badEffect through each kind of step, and
// names the effect id each records: Fire, a Tick's timer, a RunService's settle, and
// the stateless Runner.Fire.
var badSteps = []struct {
	name string
	id   string
	run  func(t *testing.T, b *badRun) error
}{
	{"Fire", "0#0#demo.bad", func(t *testing.T, b *badRun) error {
		_, err := b.start(t).Fire(context.Background(), "go")
		return err
	}},
	{"stateless Fire", "0#0#demo.bad", func(t *testing.T, b *badRun) error {
		b.start(t)
		_, err := durable.NewRunner(b.m, b.store, b.opts...).Fire(context.Background(), "bad-1", "go")
		return err
	}},
	{"Tick", "1#0#demo.bad", func(t *testing.T, b *badRun) error {
		h := b.start(t)
		if _, err := h.Fire(context.Background(), "wait"); err != nil {
			t.Fatalf("Fire wait: %v", err)
		}
		b.clk.Advance(2 * time.Second)
		_, err := h.Tick(context.Background())
		return err
	}},
	{"RunService", "1#0#demo.bad", func(t *testing.T, b *badRun) error {
		h := b.start(t)
		if _, err := h.Fire(context.Background(), "load"); err != nil {
			t.Fatalf("Fire load: %v", err)
		}
		_, _, err := h.RunService(context.Background(), state.InvokeID("bad", "loading", 0))
		return err
	}},
}

// TestUnmarshalableEffect_HandlerAppliesItLiveOnce: with a handler wired, the effect
// reaches it once, live, as the value the action emitted, on every kind of step.
func TestUnmarshalableEffect_HandlerAppliesItLiveOnce(t *testing.T) {
	for _, step := range badSteps {
		t.Run(step.name, func(t *testing.T) {
			b := newBadRun()
			rec := newEffectRecorder()
			b.opts = append(b.opts, durable.WithEffectHandler[string, string, svcCtx](rec.handler()))
			if err := step.run(t, b); err != nil {
				t.Fatalf("step: %v", err)
			}
			b.recover(t)
			if n := rec.count(step.id); n != 1 || rec.totalApplies() != 1 {
				t.Fatalf("the handler applied %s %d times (%d in all), want once", step.id, n, rec.totalApplies())
			}
			if _, ok := rec.values[0].(badEffect); !ok {
				t.Fatalf("the handler got %T, want the badEffect", rec.values[0])
			}
		})
	}
}

// TestUnmarshalableEffect_RecoverRetriesItOnce: with a handler wired, an effect whose
// dispatch failed live is applied once by Recover, however often it recovers.
func TestUnmarshalableEffect_RecoverRetriesItOnce(t *testing.T) {
	for _, step := range badSteps {
		t.Run(step.name, func(t *testing.T) {
			b := newBadRun()
			live := newEffectRecorder()
			live.failKinds["demo.bad"] = errors.New("downstream unavailable")
			opts := b.opts
			b.opts = append(opts, durable.WithEffectHandler[string, string, svcCtx](live.handler()))
			if err := step.run(t, b); !errors.Is(err, durable.ErrEffectDispatch) {
				t.Fatalf("step: %v, want ErrEffectDispatch", err)
			}
			healthy := newEffectRecorder()
			b.opts = append(opts, durable.WithEffectHandler[string, string, svcCtx](healthy.handler()))
			b.recover(t)
			b.recover(t)
			if n := healthy.count(step.id); n != 1 || healthy.totalApplies() != 1 {
				t.Fatalf("Recover applied %s %d times (%d in all), want once", step.id, n, healthy.totalApplies())
			}
			if _, ok := healthy.values[0].(badEffect); !ok {
				t.Fatalf("Recover's handler got %T, want the badEffect", healthy.values[0])
			}
			if got := atomic.LoadInt64(b.runs); got > 1 {
				t.Fatalf("the ServiceFn ran %d times, want at most 1", got)
			}
		})
	}
}

// TestUnmarshalableEffect_HandlerWiredOnlyAtRecoverSeesIt: a handler wired only at
// Recover applies the effect a step recorded with no handler wired, once.
func TestUnmarshalableEffect_HandlerWiredOnlyAtRecoverSeesIt(t *testing.T) {
	for _, step := range badSteps {
		t.Run(step.name, func(t *testing.T) {
			b := newBadRun()
			if err := step.run(t, b); err != nil {
				t.Fatalf("step: %v", err)
			}
			rec := newEffectRecorder()
			b.opts = append(b.opts, durable.WithEffectHandler[string, string, svcCtx](rec.handler()))
			b.recover(t)
			b.recover(t)
			if n := rec.count(step.id); n != 1 || rec.totalApplies() != 1 {
				t.Fatalf("the Recover-only handler applied %s %d times (%d in all), want once", step.id, n, rec.totalApplies())
			}
		})
	}
}
