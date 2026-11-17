package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// errUndecodable is the error undecodableDue returns for the due event.
var errUndecodable = errors.New("can't decode due")

// countingCodec is the JSON event codec, counting its decodes, that fails to decode
// the due event when failDue is set.
type countingCodec struct {
	decodes int
	failDue bool
}

func (c *countingCodec) Decode(payload []byte) (string, error) {
	c.decodes++
	var e string
	if len(payload) == 0 {
		return e, nil
	}
	if err := json.Unmarshal(payload, &e); err != nil {
		return e, err
	}
	if c.failDue && e == "due" {
		return "", errUndecodable
	}
	return e, nil
}

// TestTick_RebuildsATimersEventOnlyToForwardIt pins that a Tick decodes a fired timer's
// event only when the timer's transition forwards it to an actor: with no actor
// palette, or a transition with no forwardTo, the codec isn't called.
func TestTick_RebuildsATimersEventOnlyToForwardIt(t *testing.T) {
	for _, tc := range []struct {
		name             string
		palette, forward bool
		want             int
	}{
		{"no actor palette", false, false, 0},
		{"no forwardTo", true, false, 0},
		{"forwardTo", true, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			machine := "decodeOnce"
			b := state.ForgeFor[stepCtx](machine).State("idle")
			if tc.palette {
				b = b.Actor("child").State("pair").InvokeActor("child")
			} else {
				b = b.State("pair")
			}
			b = b.Initial("idle").
				Transition("idle").On("go").GoTo("pair")
			if tc.forward {
				b = b.Transition("pair").After(5 * time.Second).On("due").GoTo("pair").ForwardTo(state.ActorID(machine, "pair", 0))
			} else {
				b = b.Transition("pair").After(5 * time.Second).On("due").GoTo("pair")
			}
			m := b.Quench()
			clk := state.NewFakeClock(epoch)
			codec := &countingCodec{}
			opts := []stepOption{
				durable.WithRunnerClock[string, string, stepCtx](clk),
				durable.WithEventCodec[string, string, stepCtx](codec),
			}
			if tc.palette {
				opts = append(opts, durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
					"child": func(map[string]any) (state.ActorInstance, error) { return &recordingActor{}, nil },
				}))
			}
			h, err := durable.NewRunner(m, durable.NewMemStore(), opts...).Start(ctx, "decode", stepCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err = h.Fire(ctx, "go"); err != nil {
				t.Fatalf("Fire(go): %v", err)
			}
			clk.Advance(6 * time.Second)
			codec.decodes = 0
			if fired, err := h.Tick(ctx); err != nil || len(fired) != 1 {
				t.Fatalf("Tick fired %d timers, err %v; want one", len(fired), err)
			}
			if codec.decodes != tc.want {
				t.Fatalf("Tick decoded %d events, want %d", codec.decodes, tc.want)
			}
		})
	}
}

// TestTick_ATimersEventTheCodecCantDecode_IsReported pins that a Tick whose timer
// forwards an event the Runner's codec can't rebuild reports the codec's error rather
// than forward nothing in silence, and forwards nothing to the actor. The tick is still
// recorded, its effects dispatched, and the Handle carries on, as for a message an actor can't take: the timers applied, and failing the Handle
// would have Recover fire them, and run the actors' behaviors, again, to meet the same
// error.
func TestTick_ATimersEventTheCodecCantDecode_IsReported(t *testing.T) {
	ctx := context.Background()
	machine := "undecodable"
	childID := state.ActorID(machine, "pair", 0)
	m := state.ForgeFor[stepCtx](machine).
		Action("notify", func(state.ActionCtx[stepCtx]) (state.Effect, error) {
			return notifyEffect{To: "ops@example.com"}, nil
		}).
		Actor("child").
		State("idle").
		State("pair").InvokeActor("child").
		State("done").
		Initial("idle").
		Transition("idle").On("go").GoTo("pair").
		Transition("pair").After(5 * time.Second).On("due").GoTo("pair").ForwardTo(childID).Do("notify").
		Transition("pair").On("finish").GoTo("done").
		Quench()
	clk := state.NewFakeClock(epoch)
	var children []*recordingActor
	dispatched := 0
	opts := []stepOption{
		durable.WithRunnerClock[string, string, stepCtx](clk),
		durable.WithEventCodec[string, string, stepCtx](&countingCodec{failDue: true}),
		durable.WithEffectHandler[string, string, stepCtx](func(context.Context, string, state.Effect) error {
			dispatched++
			return nil
		}),
		durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
			"child": func(map[string]any) (state.ActorInstance, error) {
				children = append(children, &recordingActor{})
				return children[len(children)-1], nil
			},
		}),
	}
	store, id := durable.NewMemStore(), durable.InstanceID("undecodable")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	clk.Advance(6 * time.Second)
	fired, err := h.Tick(ctx)
	if !errors.Is(err, errUndecodable) || errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("Tick = %v, want the codec's error, with the Handle carrying on", err)
	}
	if len(fired) != 1 || fired[0].Err != nil {
		t.Fatalf("Tick fired %v, want the due timer applied", fired)
	}
	if len(children) != 1 || len(children[0].got) != 0 {
		t.Fatalf("child got %v, want nothing forwarded", children[0].got)
	}
	if dispatched != 1 {
		t.Fatalf("dispatched %d effects, want the due transition's notify", dispatched)
	}
	if _, err = h.Fire(ctx, "finish"); err != nil {
		t.Fatalf("Fire(finish) after the Tick: %v, want the Handle healthy", err)
	}
	// The tick was recorded: a Recover replays it, then finish.
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if cfg := fmt.Sprint(recovered.Instance().Configuration()); cfg != "[done]" {
		t.Fatalf("recovered configuration %s, want [done]", cfg)
	}
	if tail := tailLen(t, store, id); tail != 3 {
		t.Fatalf("journal holds %d records, want go, the tick and finish", tail)
	}
}
