package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// svcCtx is a value-semantics context for the service onDone-assign test.
type svcCtx struct {
	Notes []string
}

// TestAssign_ServiceOnDoneReadsResult asserts a service's result reaches its
// onDone transition's Assign via AssignCtx.Event (the done-event payload), with
// no host side channel: the runner re-fires onDone carrying the result.
func TestAssign_ServiceOnDoneReadsResult(t *testing.T) {
	m := state.Forge[string, string, svcCtx]("svcassign").
		Service("fetch", func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
			return "payload", nil
		}).
		Reducer("recordResult", func(in state.AssignCtx[svcCtx]) svcCtx {
			c := in.Entity
			if r, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, "result:"+r)
			}
			return c
		}).
		State("idle").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("ok"), state.WithInvokeOnError("fail")).
		State("ready").
		State("errored").
		Initial("idle").
		Transition("idle").On("start").GoTo("loading").
		Transition("loading").On("ok").GoTo("ready").Assign("recordResult").
		Transition("loading").On("fail").GoTo("errored").
		Quench()

	inst := m.Cast(svcCtx{}, state.WithInitialState[string]("idle"))
	run := state.NewServiceRunner(inst, nil)
	ctx := context.Background()

	res := inst.Fire(ctx, "start")
	run.Absorb(ctx, res.Effects)
	id := state.InvokeID("svcassign", "loading", 0)

	if _, ok := run.SettleDone(ctx, id, "payload"); !ok {
		t.Fatalf("SettleDone reported no in-flight service %q", id)
	}
	if inst.Current() != "ready" {
		t.Fatalf("after onDone, want ready, got %q", inst.Current())
	}
	got := inst.Entity().Notes
	if len(got) != 1 || got[0] != "result:payload" {
		t.Fatalf("onDone assign did not read service result from event: %v", got)
	}
}

// TestAssign_ActorOnDoneReadsResult asserts a child actor's done-data reaches its
// onDone transition's Assign via AssignCtx.Event, with no side channel: the
// ActorSystem re-fires the parent's onDone carrying the child output.
func TestAssign_ActorOnDoneReadsResult(t *testing.T) {
	child := state.Forge[string, string, svcCtx]("childmach").
		State("working").
		State("done").Final().
		Initial("working").
		Transition("working").On("finish").GoTo("done").
		Quench()
	childBeh := func(map[string]any) (state.ActorInstance, error) {
		inst := child.Cast(svcCtx{}, state.WithInitialState[string]("working"))
		return state.NewActor(inst, func(*state.Instance[string, string, svcCtx]) any {
			return "child-output"
		}), nil
	}

	parent := state.Forge[string, string, svcCtx]("parentmach").
		Reducer("recordChild", func(in state.AssignCtx[svcCtx]) svcCtx {
			c := in.Entity
			if o, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, "child:"+o)
			}
			return c
		}).
		Actor("spawnChild").
		State("supervising").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childFail")).
		State("complete").
		State("failed").
		Initial("supervising").
		Transition("supervising").On("childDone").GoTo("complete").Assign("recordChild").
		Transition("supervising").On("childFail").GoTo("failed").
		Quench()

	parentInst := parent.Cast(svcCtx{}, state.WithInitialState[string]("supervising"))
	sys := state.NewActorSystem(parentInst).Register("spawnChild", childBeh)
	ctx := context.Background()
	sys.Absorb(ctx, parentInst.StartEffects())

	id := state.ActorID("parentmach", "supervising", 0)
	ref, ok := sys.Ref(id)
	if !ok {
		t.Fatalf("no actor ref for id %q", id)
	}
	sys.Deliver(ctx, ref, "finish")

	if parentInst.Current() != "complete" {
		t.Fatalf("after child done, want complete, got %q", parentInst.Current())
	}
	got := parentInst.Entity().Notes
	if len(got) != 1 || got[0] != "child:child-output" {
		t.Fatalf("onDone assign did not read actor output from event: %v", got)
	}
}
