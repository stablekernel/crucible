package state_test

import "github.com/stablekernel/crucible/state"

// This file defines neutral example machines exercising the hierarchical and
// orthogonal (parallel) features: a job lifecycle with a Running superstate over
// Starting/Executing, and a worker with two orthogonal regions. The domains are
// deliberately generic and carry no real-world coupling.

// JobStatus is the example hierarchical state type.
type JobStatus int

const (
	Queued JobStatus = iota
	Running
	Starting
	Executing
	JobDone
	Canceled
)

// String renders a JobStatus by name so traces read symbolically.
func (s JobStatus) String() string {
	switch s {
	case Queued:
		return "Queued"
	case Running:
		return "Running"
	case Starting:
		return "Starting"
	case Executing:
		return "Executing"
	case JobDone:
		return "JobDone"
	case Canceled:
		return "Canceled"
	default:
		return "JobStatus?"
	}
}

// JobEvent is the example hierarchical event type.
type JobEvent int

const (
	Enqueue JobEvent = iota
	Begin
	Finish
	Cancel
)

// String renders a JobEvent by name so diagrams and traces read symbolically.
func (e JobEvent) String() string {
	switch e {
	case Enqueue:
		return "Enqueue"
	case Begin:
		return "Begin"
	case Finish:
		return "Finish"
	case Cancel:
		return "Cancel"
	default:
		return "JobEvent?"
	}
}

// Job is the example entity for the hierarchical machine.
type Job struct {
	Status JobStatus
}

// recordEntry / recordExit / recordDone are example actions that append the
// state name to a shared sink so cascade order is observable in a test.
func recordAction(label string) state.ActionFn[*Job] {
	return func(ctx state.ActionCtx[*Job]) (state.Effect, error) {
		name, _ := ctx.Params["state"].(string)
		return cascadeNote{Label: label, State: name}, nil
	}
}

// cascadeNote is a concrete effect recording one entry/exit/done step.
type cascadeNote struct {
	Label string
	State string
}

// buildJobMachine forges the hierarchical example: a Running superstate whose
// initial child is Starting, with a cross-cutting Cancel transition and a Finish
// transition out of Executing into the final Done state.
func buildJobMachine() *state.Machine[JobStatus, JobEvent, *Job] {
	return state.Forge[JobStatus, JobEvent, *Job]("job").
		Action("entry", recordAction("entry")).
		Action("exit", recordAction("exit")).
		Action("done", recordAction("done")).
		State(Queued).OwnedBy("Scheduler").
		Transition(Queued).On(Enqueue).GoTo(Running).
		SuperState(Running).OwnedBy("Worker").
		Initial(Starting).
		OnEntry("entry", state.P{"state": "Running"}).
		OnExit("exit", state.P{"state": "Running"}).
		SubState(Starting).
		OnEntry("entry", state.P{"state": "Starting"}).
		OnExit("exit", state.P{"state": "Starting"}).
		On(Begin).GoTo(Executing).
		SubState(Executing).
		OnEntry("entry", state.P{"state": "Executing"}).
		OnExit("exit", state.P{"state": "Executing"}).
		On(Finish).GoTo(JobDone).
		// Cross-cutting: declared on the superstate so it applies to any active
		// substate of Running and resolves via the child-first bubble.
		Transition(Running).On(Cancel).GoTo(Canceled).
		EndSuperState().
		State(JobDone).Final().
		State(Canceled).Final().
		Initial(Queued).
		CurrentStateFn(func(j *Job) JobStatus { return j.Status }).
		Quench()
}

// WorkerState is the example orthogonal state type.
type WorkerState int

const (
	Offline WorkerState = iota
	Active
	// Execution region.
	Idle
	Busy
	ExecDone
	// Telemetry region.
	Silent
	Reporting
	TelemetryDone
)

// String renders a WorkerState by name so traces read symbolically.
func (s WorkerState) String() string {
	switch s {
	case Offline:
		return "Offline"
	case Active:
		return "Active"
	case Idle:
		return "Idle"
	case Busy:
		return "Busy"
	case ExecDone:
		return "ExecDone"
	case Silent:
		return "Silent"
	case Reporting:
		return "Reporting"
	case TelemetryDone:
		return "TelemetryDone"
	default:
		return "WorkerState?"
	}
}

// WorkerEvent is the example orthogonal event type.
type WorkerEvent int

const (
	Activate WorkerEvent = iota
	StartWork
	StopWork
	EnableReporting
	DisableReporting
	FinishExecution
	FinishTelemetry
)

// String renders a WorkerEvent by name so diagrams and traces read symbolically.
func (e WorkerEvent) String() string {
	switch e {
	case Activate:
		return "Activate"
	case StartWork:
		return "StartWork"
	case StopWork:
		return "StopWork"
	case EnableReporting:
		return "EnableReporting"
	case DisableReporting:
		return "DisableReporting"
	case FinishExecution:
		return "FinishExecution"
	case FinishTelemetry:
		return "FinishTelemetry"
	default:
		return "WorkerEvent?"
	}
}

// Worker is the example entity for the orthogonal machine.
type Worker struct {
	State WorkerState
}

// buildWorkerMachine forges the orthogonal example: an Active superstate holding
// two parallel regions — Execution (Idle/Busy/ExecDone) and Telemetry
// (Silent/Reporting/TelemetryDone). Active is done when both regions reach their
// final states.
func buildWorkerMachine() *state.Machine[WorkerState, WorkerEvent, *Worker] {
	return state.Forge[WorkerState, WorkerEvent, *Worker]("worker").
		Action("activeDone", recordWorkerDone).
		State(Offline).
		Transition(Offline).On(Activate).GoTo(Active).
		SuperState(Active).OwnedBy("Supervisor").
		OnDone("activeDone").
		Region("Execution").
		Initial(Idle).
		SubState(Idle).
		On(StartWork).GoTo(Busy).
		SubState(Busy).
		On(StopWork).GoTo(Idle).
		On(FinishExecution).GoTo(ExecDone).
		SubState(ExecDone).Final().
		EndRegion().
		Region("Telemetry").
		Initial(Silent).
		SubState(Silent).
		On(EnableReporting).GoTo(Reporting).
		SubState(Reporting).
		On(DisableReporting).GoTo(Silent).
		On(FinishTelemetry).GoTo(TelemetryDone).
		SubState(TelemetryDone).Final().
		EndRegion().
		EndSuperState().
		Initial(Offline).
		CurrentStateFn(func(w *Worker) WorkerState { return w.State }).
		Quench()
}

// workerDoneFired records that the Active superstate's OnDone ran.
var workerDoneFired bool

func recordWorkerDone(ctx state.ActionCtx[*Worker]) (state.Effect, error) {
	workerDoneFired = true
	return cascadeNote{Label: "done", State: "Active"}, nil
}
