package cluster_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/state"
)

// failOneChild casts the parent, spawns worker-1, and fails it with cause so the
// failure (the parent declares no onError) escalates through the wired handler.
func failOneChild(t *testing.T, sup *cluster.Supervisor, cause error) {
	t.Helper()
	parent := parentMachine().Cast(&parentEnt{}, state.WithInitialState("idle"))
	actorSys := state.NewActorSystem(parent).
		Register("child", childBehavior()).
		WithEscalationHandler(sup.Handle)
	ctx := context.Background()
	res := parent.Fire(ctx, "go")
	actorSys.Absorb(ctx, res.Effects)
	if _, routed := actorSys.SettleError(ctx, "worker-1", cause); routed {
		t.Fatal("SettleError routed an onError, but the parent declared none")
	}
}

func TestDecision_String(t *testing.T) {
	cases := map[cluster.Decision]string{
		cluster.Escalate:     "escalate",
		cluster.Stop:         "stop",
		cluster.Decision(99): "unknown",
	}
	for d, want := range cases {
		if got := d.String(); got != want {
			t.Fatalf("Decision(%d).String() = %q, want %q", d, got, want)
		}
	}
}

func TestSupervisor_DecisionFor(t *testing.T) {
	sup := cluster.NewSupervisor(
		cluster.WithDefaultDecision(cluster.Stop),
		cluster.WithDecision("child", cluster.Escalate),
	)
	if got := sup.DecisionFor("child"); got != cluster.Escalate {
		t.Fatalf("DecisionFor(child) = %v, want Escalate", got)
	}
	if got := sup.DecisionFor("other"); got != cluster.Stop {
		t.Fatalf("DecisionFor(other) = %v, want Stop (default)", got)
	}
	// The zero-value default is Escalate.
	if got := cluster.NewSupervisor().DecisionFor("anything"); got != cluster.Escalate {
		t.Fatalf("default DecisionFor = %v, want Escalate", got)
	}
}

func TestSupervisor_EscalateForwardsToSink(t *testing.T) {
	var sunk []*state.ActorEscalation
	sup := cluster.NewSupervisor(
		cluster.WithDecision("child", cluster.Escalate),
		cluster.WithEscalationSink(func(_ context.Context, esc *state.ActorEscalation) {
			sunk = append(sunk, esc)
		}),
	)
	cause := errors.New("boom")
	failOneChild(t, sup, cause)

	if len(sunk) != 1 {
		t.Fatalf("sink received %d escalations, want 1", len(sunk))
	}
	if !errors.Is(sunk[0], cause) {
		t.Fatalf("forwarded escalation does not wrap cause: %v", sunk[0])
	}
	handled := sup.Handled()
	if len(handled) != 1 || handled[0].Decision != cluster.Escalate || handled[0].ActorID != "worker-1" {
		t.Fatalf("Handled() = %+v, want one Escalate of worker-1", handled)
	}
	if !errors.Is(handled[0].Err, cause) {
		t.Fatalf("handled record does not carry cause: %v", handled[0].Err)
	}
}

func TestSupervisor_StopContainsFailure(t *testing.T) {
	var sunk int
	sup := cluster.NewSupervisor(
		cluster.WithDefaultDecision(cluster.Stop),
		cluster.WithEscalationSink(func(context.Context, *state.ActorEscalation) { sunk++ }),
	)
	failOneChild(t, sup, errors.New("boom"))

	if sunk != 0 {
		t.Fatalf("Stop decision forwarded to sink %d times, want 0 (contained)", sunk)
	}
	handled := sup.Handled()
	if len(handled) != 1 || handled[0].Decision != cluster.Stop {
		t.Fatalf("Handled() = %+v, want one Stop", handled)
	}
}

// TestSupervisor_EscalateNoSink confirms an Escalate decision with no sink is a
// safe no-op beyond the recorded default the kernel already keeps.
func TestSupervisor_EscalateNoSink(t *testing.T) {
	sup := cluster.NewSupervisor() // default Escalate, no sink
	failOneChild(t, sup, errors.New("boom"))
	if len(sup.Handled()) != 1 {
		t.Fatalf("Handled() = %d, want 1", len(sup.Handled()))
	}
}
