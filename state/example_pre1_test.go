package state_test

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// gate is the entity the finalize-seam example guards and assigns against.
type gate struct {
	approved bool
	stamped  bool
}

// ExampleBuilder_Quench walks the full Forge -> Temper -> Quench finalize seam:
// Forge opens a builder, Temper lints it without freezing (returning diagnostics
// a tool can surface), and Quench binds refs and freezes the builder into an
// immutable Machine ready to Cast. A fully specified machine tempers with no
// findings and quenches without panicking.
func ExampleBuilder_Quench() {
	b := state.ForgeFor[gate]("turnstile").
		Guard("approved", func(c state.GuardCtx[gate]) bool { return c.Entity.approved }).
		// CurrentStateFn lets the kernel derive the current state from the entity,
		// which keeps Temper clean (no "missing CurrentStateFn" warning).
		CurrentStateFn(func(g gate) string {
			if g.stamped {
				return "open"
			}
			return "locked"
		}).
		State("locked").
		Transition("locked").On("push").GoTo("open").When("approved").
		State("open").
		Initial("locked")

	// Temper lints without freezing: a tool can show findings before committing.
	fmt.Println("temper findings:", len(b.Temper()))

	// Quench binds and freezes into an immutable Machine ready to Cast.
	m := b.Quench()

	denied := m.Cast(gate{approved: false})
	denied.Fire(context.Background(), "push")
	fmt.Println("denied:", denied.Current())

	allowed := m.Cast(gate{approved: true})
	allowed.Fire(context.Background(), "push")
	fmt.Println("allowed:", allowed.Current())
	// Output:
	// temper findings: 0
	// denied: locked
	// allowed: open
}

// ExampleIR_Provide shows the JSON-rehydrate-then-run story: a machine authored
// in code is serialized with ToJSON, reloaded with LoadFromJSON into a behavior-
// free IR, and only then bound to host behavior with Provide before Quench. The
// guard func is supplied at Provide time, after the JSON was loaded, proving the
// structure travels as data while the Go behavior is re-attached by the host.
func ExampleIR_Provide() {
	// Author and freeze a machine in code, then serialize its structure. The
	// guard func itself is not serializable; only its name travels in the JSON.
	authored := state.ForgeFor[gate]("turnstile").
		Guard("approved", func(c state.GuardCtx[gate]) bool { return c.Entity.approved }).
		State("locked").
		Transition("locked").On("push").GoTo("open").When("approved").
		State("open").
		Initial("locked").
		Quench()

	jsonBytes, err := authored.ToJSON(state.WithoutSrcPos())
	if err != nil {
		fmt.Println("ToJSON err:", err)
		return
	}

	// Reload the structure with no behavior attached.
	ir, err := state.LoadFromJSON[string, string, gate](jsonBytes)
	if err != nil {
		fmt.Println("LoadFromJSON err:", err)
		return
	}

	// Bind the guard func AFTER loading, supplying host behavior by name.
	reg := state.NewRegistry[gate]().
		Guard("approved", func(c state.GuardCtx[gate]) bool { return c.Entity.approved })
	m := ir.Provide(reg).Quench()

	allowed := m.Cast(gate{approved: true}, state.WithInitialState("locked"))
	allowed.Fire(context.Background(), "push")
	fmt.Println("rehydrated:", allowed.Current())
	// Output:
	// rehydrated: open
}

// pingPong is the child-actor entity: it counts the pings it receives before it
// is told to finish.
type pingPong struct {
	pings int
}

// ExampleActorSystem exercises the actor lifecycle end to end: a parent state
// dynamically Spawns a child-machine actor, the host delivers events to it by id
// and observes its progress, and the host Stops it explicitly. The ActorSystem is
// the host-side driver that turns a parent's SpawnActor/StopActor effects into
// running child actors and routes their completion back through the parent.
func ExampleActorSystem() {
	// The child machine counts pings, then completes on "finish".
	child := state.ForgeFor[*pingPong]("counter").
		Action("count", func(c state.ActionCtx[*pingPong]) (state.Effect, error) {
			c.Entity.pings++
			return nil, nil
		}).
		State("counting").
		State("done").Final().
		Initial("counting").
		Transition("counting").On("ping").GoTo("counting").Do("count").
		Transition("counting").On("finish").GoTo("done").
		Quench()

	// The parent spawns the child on "start" and reacts to its completion.
	parent := state.ForgeFor[*pingPong]("supervisor").
		State("idle").
		State("active").
		Initial("idle").
		Transition("idle").On("start").GoTo("active").
		Spawn("counter", "worker", state.WithSpawnOnDone("workerDone")).
		Transition("active").On("workerDone").GoTo("idle").
		Quench()

	ctx := context.Background()
	root := parent.Cast(&pingPong{}, state.WithInitialState("idle"))

	// A spawn behavior Casts a fresh child per spawn and exposes its ping count.
	behavior := func(map[string]any) (state.ActorInstance, error) {
		inst := child.Cast(&pingPong{}, state.WithInitialState("counting"))
		return state.NewActor(inst, func(i *state.Instance[string, string, *pingPong]) any {
			return i.Entity().pings
		}), nil
	}
	sys := state.NewActorSystem(root).Register("counter", behavior)

	// Firing "start" emits the SpawnActor effect; Absorb spawns the child actor.
	res := root.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	fmt.Println("running after spawn:", sys.Running())

	// Deliver two pings to the child by id; each steps it through its counter.
	sys.DeliverByID(ctx, "worker", "ping")
	sys.DeliverByID(ctx, "worker", "ping")

	// Stop the actor explicitly through the ref the host tracks.
	ref, _ := sys.Ref("worker")
	sys.Stop(ref)
	fmt.Println("running after stop:", sys.Running())
	// Output:
	// running after spawn: 1
	// running after stop: 0
}
