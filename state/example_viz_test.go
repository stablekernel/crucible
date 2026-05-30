package state_test

import "fmt"

// ExampleMachine_ToMermaid renders a hierarchical machine as a Mermaid
// stateDiagram-v2: the initial marker, the Running superstate as a nested block
// with its own initial child, the cross-cutting Cancel transition, and the
// final-state markers.
func ExampleMachine_ToMermaid() {
	fmt.Println(buildJobMachine().ToMermaid())
	// Output:
	// stateDiagram-v2
	//     [*] --> Queued
	//     state Running {
	//         [*] --> Running__Starting
	//         Running__Starting
	//         Running__Executing
	//         Running__Starting --> Running__Executing: 1
	//     }
	//     JobDone --> [*]
	//     Canceled --> [*]
	//     Queued --> Running: 0
	//     Running --> Canceled: 3
	//     Running__Executing --> JobDone: 2
	//     classDef owner_Scheduler fill:#f0d9ff
	//     classDef owner_Worker fill:#d9f2f2
	//     class Queued owner_Scheduler
	//     class Running owner_Worker
}
