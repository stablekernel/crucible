package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestStartEffects_IncludesTheSuperstatesInvokes proves StartEffects and ResumeEffects
// report the services and actors a compound state declares while one of its children
// is active, the compound state's before its child's.
func TestStartEffects_IncludesTheSuperstatesInvokes(t *testing.T) {
	svc := func(context.Context, state.ServiceCtx[*trec]) (any, error) { return nil, nil }
	m := state.ForgeFor[*trec]("nest").
		Service("outerSvc", svc).
		Service("innerSvc", svc).
		Actor("outerActor").
		SuperState("outer").Invoke("outerSvc").InvokeActor("outerActor").
		SubState("inner").Invoke("innerSvc").
		Initial("inner").
		EndSuperState().
		Initial("outer").
		Quench()
	inst := m.Cast(&trec{}, state.WithInitialState("inner"))
	var services, actors []string
	for _, eff := range inst.StartEffects() {
		switch e := eff.(type) {
		case state.StartService:
			services = append(services, e.Src.Name)
		case state.SpawnActor:
			actors = append(actors, e.Src.Name)
		}
	}
	if len(services) != 2 || services[0] != "outerSvc" || services[1] != "innerSvc" {
		t.Fatalf("services = %v, want [outerSvc innerSvc]", services)
	}
	if len(actors) != 1 || actors[0] != "outerActor" {
		t.Fatalf("actors = %v, want [outerActor]", actors)
	}
	var resumed []string
	for _, eff := range inst.ResumeEffects() {
		if e, ok := eff.(state.SpawnActor); ok {
			resumed = append(resumed, e.Src.Name)
		}
	}
	if len(resumed) != 1 || resumed[0] != "outerActor" {
		t.Fatalf("ResumeEffects actors = %v, want [outerActor]", resumed)
	}
}
