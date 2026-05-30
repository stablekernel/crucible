package state_test

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// ExampleForge builds a document-approval machine with the Forge DSL and fires a
// single event, showing the resulting state and the effect the transition
// emitted.
func ExampleForge() {
	m := buildDocMachine()
	doc := &Document{Status: Draft}
	res := m.Cast(doc).Fire(context.Background(), Submit)

	fmt.Println("state:", res.NewState)
	fmt.Println("effects:", res.Effects)
	// Output:
	// state: 1
	// effects: [{submitted}]
}

// ExampleInstance_FireSeq drives a machine through a sequence of events, walking
// a document from Draft to Published in one batch.
func ExampleInstance_FireSeq() {
	m := buildDocMachine()
	doc := &Document{Status: Draft, ReviewerID: strptr("rev-1")}
	batch := m.Cast(doc).FireSeq(context.Background(), []DocEvent{Submit, Approve, Publish})

	fmt.Println("steps:", len(batch.Steps))
	fmt.Println("final:", batch.Steps[len(batch.Steps)-1].NewState)
	// Output:
	// steps: 3
	// final: 3
}

// ExampleMachine_PlanPath finds the shortest event sequence that drives a
// document from Draft to Published, honoring guards against the entity.
func ExampleMachine_PlanPath() {
	m := buildDocMachine()
	doc := &Document{Status: Draft, ReviewerID: strptr("rev-1")}
	path, err := m.PlanPath(Draft, Published, doc)

	fmt.Println("err:", err)
	fmt.Println("steps:", len(path))
	// Output:
	// err: <nil>
	// steps: 3
}

// ExampleMachine_ToJSON serializes a machine's IR and reports that the canonical
// definition round-trips: loading the JSON and reserializing yields identical
// bytes.
func ExampleMachine_ToJSON() {
	m := buildDocMachine()
	data, _ := m.ToJSON()

	ir, _ := state.LoadFromJSON[DocState, DocEvent, *Document](data)
	m2 := ir.Provide(docRegistry()).Quench()
	data2, _ := m2.ToJSON()

	fmt.Println("stable:", string(data) == string(data2))
	// Output:
	// stable: true
}

// ExampleMachine_Assay checks an externally-built entity against a state's
// declarative requirements without firing a transition.
func ExampleMachine_Assay() {
	m := buildDocMachine()

	missing := m.Assay(Approved, &Document{Status: Approved})
	ok := m.Assay(Approved, &Document{Status: Approved, ReviewerID: strptr("rev-1")})

	fmt.Println("missing reviewer:", missing != nil)
	fmt.Println("with reviewer:", ok)
	// Output:
	// missing reviewer: true
	// with reviewer: <nil>
}

// ExampleMachine_Cast_hierarchical enters a hierarchical machine: casting into a
// compound state descends to its initial child, so the job starts in Starting
// under the Running superstate.
func ExampleMachine_Cast_hierarchical() {
	m := buildJobMachine()
	job := &Job{Status: Queued}
	inst := m.Cast(job)
	res := inst.Fire(context.Background(), Enqueue)

	fmt.Println("state:", res.NewState)
	// Output:
	// state: Starting
}
