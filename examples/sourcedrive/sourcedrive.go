// SPDX-License-Identifier: Apache-2.0

package sourcedrive

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/statemachine"
	"github.com/stablekernel/crucible/state"
)

// Shipment is the entity each fulfillment instance carries. Funds gates the pay
// transition, so an unfunded pay is a state-aware rejection rather than a
// transient error. Stage records the lifecycle state the shipment has reached;
// CurrentStateFn reads it so a shipment cast from a record already in flight
// resumes at its real state instead of restarting at pending.
type Shipment struct {
	Funds bool   `json:"funds"`
	Stage string `json:"stage"`
}

// currentStage derives a shipment's current lifecycle state for CurrentStateFn.
// A nil entity (a fresh cast with no record yet) or an empty Stage means the
// instance has not advanced, so it starts at pending; otherwise the recorded
// stage is honored so resume and seek land on the real state.
func currentStage(s *Shipment) string {
	if s == nil || s.Stage == "" {
		return "pending"
	}
	return s.Stage
}

// Command is the JSON body of an inbound message: the event to fire against the
// shipment the message key identifies.
type Command struct {
	// Op is the statechart event name, for example "pay" or "deliver".
	Op string `json:"op"`
}

// Fulfillment bundles the wired statechart, its durable store, and the
// source.Handler the source/statemachine bridge produces. A caller (the example
// test, the cmd program, the README walkthrough) builds one, then drives any
// source.Inlet through Handler via Run.
type Fulfillment struct {
	// Machine is the shipment lifecycle statechart.
	Machine *state.Machine[string, string, *Shipment]
	// Store persists one instance per shipment key under optimistic concurrency.
	Store *statemachine.MemStore[string, string, string, *Shipment]
	// Registry decodes JSON Command bodies.
	Registry *source.Registry
	// Handler is the consume → decode → route → Fire → persist → ack binding.
	Handler source.Handler
}

// NewFulfillment forges the shipment statechart and binds it to a fresh
// in-memory store through statemachine.Drive, returning the wired Fulfillment.
// The lifecycle is:
//
//	pending --pay[funded]--> shipped --deliver--> delivered
//
// pay is guarded by funded so an unfunded pay is rejected as invalid-for-state,
// and deliver from pending has no transition, so both state-aware rejection
// shapes are reachable from a stream.
func NewFulfillment() *Fulfillment {
	machine := state.Forge[string, string, *Shipment]("fulfillment").
		Guard("funded", func(ctx state.GuardCtx[*Shipment]) bool {
			return ctx.Entity != nil && ctx.Entity.Funds
		}).
		State("pending").
		State("shipped").
		State("delivered").
		Initial("pending").
		CurrentStateFn(currentStage).
		Transition("pending").On("pay").GoTo("shipped").When("funded").
		Transition("shipped").On("deliver").GoTo("delivered").
		Quench(state.Strict())

	store := statemachine.NewMemStore[string, string, string, *Shipment]()
	reg := source.NewRegistry().Register("application/json", source.NewJSONCodec[Command]())

	handler := statemachine.Drive[string, string, *Shipment](machine, store, routeCommand(reg))

	return &Fulfillment{
		Machine:  machine,
		Store:    store,
		Registry: reg,
		Handler:  handler,
	}
}

// routeCommand resolves the instance key from the message key and decodes the
// event op from the JSON body. A decode/route failure returns an error, which
// the bridge treats as poison (Term): an unroutable message cannot be retried
// into legibility.
func routeCommand(reg *source.Registry) statemachine.Router[string, string] {
	return func(m source.Message) (string, string, error) {
		cmd, err := source.DecodeTyped[Command](reg, m)
		if err != nil {
			return "", "", fmt.Errorf("decode command: %w", err)
		}
		key := string(m.Key())
		if key == "" {
			return "", "", fmt.Errorf("command missing shipment key")
		}
		return key, cmd.Op, nil
	}
}

// Seed persists a starting pending instance for key with the given funding, so a
// guarded transition can be driven from a known state. It is a convenience for
// the example and tests; a production consumer lets Drive cast a fresh instance
// on first delivery instead.
func (f *Fulfillment) Seed(ctx context.Context, key string, funds bool) error {
	inst := f.Machine.Cast(&Shipment{Funds: funds}, state.WithInitialState("pending"))
	rec := statemachine.Record[string, string, *Shipment]{Snapshot: inst.Snapshot(), Version: 1}
	if err := f.Store.Save(ctx, key, rec, 0); err != nil {
		return fmt.Errorf("seed %q: %w", key, err)
	}
	return nil
}

// Run drives inlet through the fulfillment handler with a source.Hopper until
// ctx is canceled or the subscription drains. It consumes the given topics under
// group, decoding and firing each message against the statechart, persisting the
// transition before the ack. Run works with any source.Inlet: the example test
// passes an in-memory memsource.Inlet, and RunKafka passes a Kafka inlet.
func Run(ctx context.Context, logger *slog.Logger, inlet source.Inlet, f *Fulfillment, topics []string, group string) error {
	if logger == nil {
		logger = slog.Default()
	}
	sub, err := inlet.Subscribe(ctx, source.SubscribeConfig{Topics: topics, Group: group})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	defer func() { _ = sub.Close() }()

	hopper := source.New(
		source.WithName("sourcedrive"),
		source.WithLogger(logger),
	)
	defer func() { _ = hopper.Close() }()

	logger.Info("sourcedrive consuming", slog.Any("topics", topics), slog.String("group", group))
	if err := hopper.Run(ctx, sub, f.Handler); err != nil {
		return fmt.Errorf("hopper run: %w", err)
	}
	return nil
}
