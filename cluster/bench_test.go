package cluster_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/state"
)

// pingEnt is a long-lived bench actor's context.
type pingEnt struct{}

// pingMachine stays running: a self-transition on "ping" keeps the actor alive so
// a benchmark can deliver to it repeatedly without re-spawning.
func pingMachine() *state.Machine[string, string, *pingEnt] {
	return state.Forge[string, string, *pingEnt]("ping").
		State("up").
		Initial("up").
		Transition("up").On("ping").GoTo("up").
		Quench()
}

func pingBehavior() state.ActorBehavior {
	m := pingMachine()
	return func(map[string]any) (state.ActorInstance, error) {
		return state.NewActor(m.Cast(&pingEnt{}, state.WithInitialState("up")), nil), nil
	}
}

// pingSystem builds a node-scoped System running one long-lived ping actor, and
// returns the System and the actor's ref.
func pingSystem(node string, opts ...cluster.Option) (*cluster.System[string, string, *parentEnt], state.ActorRef) {
	parent := parentMachine().Cast(&parentEnt{}, state.WithInitialState("idle"))
	actorSys := state.NewActorSystem(parent).Register("child", pingBehavior())
	ctx := context.Background()
	actorSys.Absorb(ctx, []state.Effect{state.SpawnActor{ID: "ping-1", Src: state.Ref{Name: "child"}}})
	sys := cluster.NewSystem(node, actorSys, opts...)
	ref, _ := sys.Ref("ping-1")
	return sys, ref
}

// BenchmarkDeliver measures the per-delivery overhead the System adds: the local
// path is a thin pass-through over the kernel ActorSystem, and the remote path
// adds the transport's node lookup and the delegating call.
func BenchmarkDeliver(b *testing.B) {
	ctx := context.Background()

	b.Run("local", func(b *testing.B) {
		sys, ref := pingSystem("node-a")
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := sys.Deliver(ctx, ref, "ping"); err != nil {
				b.Fatalf("deliver: %v", err)
			}
		}
	})

	b.Run("remote/inmemory", func(b *testing.B) {
		tr := cluster.NewInMemoryTransport()
		sysB, localRef := pingSystem("node-b")
		parentA := parentMachine().Cast(&parentEnt{}, state.WithInitialState("idle"))
		sysA := cluster.NewSystem("node-a", state.NewActorSystem(parentA), cluster.WithTransport(tr))
		tr.Register("node-a", sysA)
		tr.Register("node-b", sysB)
		remote := state.ActorRef{ID: localRef.ID, Src: localRef.Src, Node: "node-b"}

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := sysA.Deliver(ctx, remote, "ping"); err != nil {
				b.Fatalf("remote deliver: %v", err)
			}
		}
	})
}
