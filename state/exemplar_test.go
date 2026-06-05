package state_test

import (
	"context"
	"errors"
	"time"

	"github.com/stablekernel/crucible/state"
)

// This file defines the flagship end-to-end exemplar: a resilient connection
// lifecycle machine that exercises most of the engine at once — hierarchy with
// parallel regions, deep history, guard combinators and the stateIn built-in,
// eventless run-to-completion, a delayed (`after`) retry backoff, an invoked
// service for the dial, and a spawned child-machine worker actor that messages
// its parent on completion. The domain is a generic transport connection and
// carries no real-world coupling.
//
// The machine, drawn as a graph:
//
//	Disconnected --Connect--> Connecting
//	  Connecting invokes the "dial" service; after a ConnectTimeout it fires
//	  Retry (a backoff) back into Connecting; on dial onDone it fires Dialed.
//	Connecting --Dialed[canAdmit]--> Connected
//	  Connected is a parallel superstate with two orthogonal regions:
//	    Heartbeat: Beating <--Pong/Ping--> Missed
//	    Work:      WorkIdle --Assign--> Processing --Completed--> Drained(final)
//	      Processing spawns a "worker" child actor; the worker runs to its own
//	      final state and sendParents the Completed event back to the connection.
//	  Connected --Close--> Closing --always--> Closed(final)
//	A reconnect re-enters Connected through its deep history, resuming the Work
//	region where it left off rather than at WorkIdle.

// Conn is the example state type for the connection lifecycle.
type Conn int

// Connection states. Connected is a compound state that nests the Live parallel
// superstate (Heartbeat + Work regions) plus a deep-history pseudo-state Resume,
// so a reconnect resumes the full parallel configuration where it left off rather
// than at the regions' initial leaves.
const (
	Disconnected Conn = iota
	Connecting
	Backoff
	Connected
	Live
	// Resume is the deep-history pseudo-state of Connected; it never appears in the
	// active configuration.
	Resume
	// Heartbeat region.
	Beating
	Missed
	// Work region.
	WorkIdle
	Processing
	Drained
	// Shutdown spine.
	Closing
	Closed
)

// String renders a Conn by name so traces and diagrams read symbolically.
func (s Conn) String() string {
	switch s {
	case Disconnected:
		return "Disconnected"
	case Connecting:
		return "Connecting"
	case Backoff:
		return "Backoff"
	case Connected:
		return "Connected"
	case Live:
		return "Live"
	case Resume:
		return "Resume"
	case Beating:
		return "Beating"
	case Missed:
		return "Missed"
	case WorkIdle:
		return "WorkIdle"
	case Processing:
		return "Processing"
	case Drained:
		return "Drained"
	case Closing:
		return "Closing"
	case Closed:
		return "Closed"
	default:
		return "Conn?"
	}
}

// ConnEvent is the example event type for the connection lifecycle.
type ConnEvent int

// Connection events. Dialed/DialFailed route the invoked dial service's outcome;
// Retry is the delayed backoff; Assign/Completed drive the Work region; Ping/Pong
// drive the Heartbeat region; Drop/Reconnect exercise deep-history resume; Close
// drives shutdown.
const (
	Connect ConnEvent = iota
	Dialed
	DialFailed
	Retry
	Ping
	Pong
	Assign
	Completed
	Drop
	Reconnect
	Close
)

// String renders a ConnEvent by name so traces and diagrams read symbolically.
func (e ConnEvent) String() string {
	switch e {
	case Connect:
		return "Connect"
	case Dialed:
		return "Dialed"
	case DialFailed:
		return "DialFailed"
	case Retry:
		return "Retry"
	case Ping:
		return "Ping"
	case Pong:
		return "Pong"
	case Assign:
		return "Assign"
	case Completed:
		return "Completed"
	case Drop:
		return "Drop"
	case Reconnect:
		return "Reconnect"
	case Close:
		return "Close"
	default:
		return "ConnEvent?"
	}
}

// Link is the entity the connection machine is bound to. Admitted gates the
// guard combinator on the Dialed edge; Dials counts dial attempts; Notes records
// the actions the run took so a test can assert observable behavior.
type Link struct {
	Admitted bool     `json:"admitted"`
	Healthy  bool     `json:"healthy"`
	Dials    int      `json:"dials"`
	Notes    []string `json:"notes"`
}

// ConnectTimeout is the delay after which a failed dial's Backoff state retries.
// The exemplar's FakeClock advances past it to drive the delayed Retry edge.
const ConnectTimeout = 2 * time.Second

// workerID is the registry key the dynamically spawned worker actor is addressed
// by, so the harness can resolve its ActorRef to step it to completion.
const workerID = "worker-1"

// taskEntity is the entity a worker child actor is bound to; result is the
// output the worker exposes on completion.
type taskEntity struct {
	result string
}

// taskState / taskEvent are the worker child machine's own types. The worker is
// a tiny two-state run-to-final machine: Working --Run--> Done(final).
type taskState int

const (
	taskWorking taskState = iota
	taskDone
)

func (s taskState) String() string {
	if s == taskDone {
		return "taskDone"
	}
	return "taskWorking"
}

type taskEvent int

const (
	taskRun taskEvent = iota
)

func (e taskEvent) String() string { return "taskRun" }

// workerMachine builds the worker child machine: it starts Working and reaches
// the final Done state on taskRun, recording a result the actor adapter surfaces
// as the worker's output.
func workerMachine() *state.Machine[taskState, taskEvent, *taskEntity] {
	return state.Forge[taskState, taskEvent, *taskEntity]("worker").
		Action("finish", func(c state.ActionCtx[*taskEntity]) (state.Effect, error) {
			c.Entity.result = "task-output"
			return nil, nil
		}).
		State(taskWorking).
		State(taskDone).Final().OnEntry("finish").
		Initial(taskWorking).
		Transition(taskWorking).On(taskRun).GoTo(taskDone).
		Quench()
}

// workerBehavior returns an ActorBehavior that Casts a fresh worker per spawn,
// exposing the worker entity's result as the actor output.
func workerBehavior() state.ActorBehavior {
	wm := workerMachine()
	return func(input map[string]any) (state.ActorInstance, error) {
		inst := wm.Cast(&taskEntity{}, state.WithInitialState(taskWorking))
		return state.NewActor(inst, func(i *state.Instance[taskState, taskEvent, *taskEntity]) any {
			return i.Entity().result
		}), nil
	}
}

// buildConnMachine forges the connection lifecycle exemplar. It is the single
// realistic machine the e2e test and the BenchmarkE2E benchmark drive through the
// wired runtime (ActorSystem + Scheduler + ServiceRunner).
//
// It models the recommended v1 patterns: the context is a value (Link, not *Link),
// and every context change flows through an Assign reducer — the sole writer — so
// guards and the kernel observe context read-only. The dial service's token and the
// worker actor's output reach their onDone reducers through AssignCtx.Event (the
// re-fired done event's payload), not a host side channel; no run/sys back-reference
// is needed.
func buildConnMachine() *state.Machine[Conn, ConnEvent, Link] {
	return state.Forge[Conn, ConnEvent, Link]("connection").
		// The dial service: a host-run unit of work whose result routes through
		// the Dialed event. This fn is the production implementation a real host
		// would resolve and run; the e2e test settles the in-flight dial
		// deterministically to drive the success and failure edges explicitly.
		Service("dial", func(_ context.Context, in state.ServiceCtx[Link]) (any, error) {
			if in.Entity.Dials < 2 {
				return nil, errors.New("transient dial failure")
			}
			return "session-token", nil
		}).
		// canAdmit gates the Dialed -> Connected edge inside a guard combinator.
		Guard("canAdmit", func(c state.GuardCtx[Link]) bool { return c.Entity.Admitted }).
		Guard("isHealthy", func(c state.GuardCtx[Link]) bool { return c.Entity.Healthy }).
		// countDial is a pure reducer: it folds a dial attempt into the next context
		// value (the sole context-mutation site under the value-semantics contract).
		Reducer("countDial", func(in state.AssignCtx[Link]) Link {
			c := in.Entity
			c.Dials++
			c.Notes = append(c.Notes, "dial")
			return c
		}).
		// captureToken reads the dial service's result from the done-event payload
		// (AssignCtx.Event), with no host side channel: the runner re-fires Dialed
		// carrying the token.
		Reducer("captureToken", func(in state.AssignCtx[Link]) Link {
			c := in.Entity
			if r, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, "token:"+r)
			}
			return c
		}).
		// captureWork reads the worker actor's output from the done-event payload:
		// the ActorSystem re-fires Completed carrying the worker output.
		Reducer("captureWork", func(in state.AssignCtx[Link]) Link {
			c := in.Entity
			if o, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, "work:"+o)
			}
			return c
		}).
		// Disconnected: the resting state; Connect begins a fresh dial, while
		// Reconnect resumes a previously-dropped session through Connected's deep
		// history (restoring the last Live configuration rather than re-dialing).
		State(Disconnected).
		Transition(Disconnected).On(Connect).GoTo(Connecting).Assign("countDial").
		Transition(Disconnected).On(Reconnect).GoTo(Resume).
		// Connecting invokes the dial service. On dial success it fires Dialed; on
		// failure it falls back to Backoff to wait out a retry delay.
		State(Connecting).
		Invoke("dial", state.WithInvokeOnDone(Dialed), state.WithInvokeOnError(DialFailed)).
		Transition(Connecting).On(DialFailed).GoTo(Backoff).
		// Backoff waits out a connect-timeout delay, then the delayed Retry edge
		// re-enters Connecting (re-arming the dial). This is the retry/backoff loop.
		State(Backoff).
		Transition(Backoff).After(ConnectTimeout).On(Retry).GoTo(Connecting).Assign("countDial").
		// The Dialed edge is guarded by a combinator: admit only when the link is
		// admitted AND (healthy OR not yet in the Connected configuration). The
		// stateIn leaf reads the live active spine.
		Transition(Connecting).On(Dialed).
		WhenExpr(state.And(
			state.Guard[Conn]("canAdmit"),
			state.Or(state.Guard[Conn]("isHealthy"), state.Not(state.StateIn(Connected))),
		)).
		GoTo(Connected).Assign("captureToken").
		// Connected is a compound state nesting a deep-history pseudo-state (Resume)
		// and the Live parallel superstate. Entering Connected normally descends to
		// Live; targeting Resume on a reconnect restores Live's full nested leaf
		// configuration (both region leaves) where it last left off.
		SuperState(Connected).
		Initial(Live).
		History(Resume, state.HistoryDeep).DefaultTo(Live).
		// Live runs two orthogonal regions at once: a Heartbeat liveness loop and a
		// Work pipeline that spawns a worker actor per assigned task. It is a nested
		// parallel superstate, so Connected's deep history restores both region
		// leaves on a reconnect.
		SuperState(Live).
		Region("Heartbeat").
		Initial(Beating).
		SubState(Beating).
		On(Ping).GoTo(Missed).
		SubState(Missed).
		On(Pong).GoTo(Beating).
		EndRegion().
		Region("Work").
		Initial(WorkIdle).
		SubState(WorkIdle).
		// Assigning work spawns a worker child actor with the dynamic spawn
		// built-in: when the actor reaches its own final state, the ActorSystem
		// re-fires Completed (carrying the worker's output) back through the
		// connection's Fire, advancing the Work region to Drained.
		On(Assign).GoTo(Processing).
		Spawn("worker", workerID,
			state.WithSpawnOnDone(Completed), state.WithSpawnOnError(DialFailed)).
		SubState(Processing).
		On(Completed).GoTo(Drained).Assign("captureWork").
		SubState(Drained).Final().
		EndRegion().
		EndSuperState().
		// Cross-cutting transitions on the Connected compound apply to any active
		// substate. Drop records the live configuration in deep history and falls
		// back to Disconnected; Close begins shutdown.
		Transition(Connected).On(Drop).GoTo(Disconnected).
		Transition(Connected).On(Close).GoTo(Closing).
		EndSuperState().
		// Closing runs to completion: an eventless Always edge drains it to Closed.
		State(Closing).
		Transition(Closing).Always().GoTo(Closed).
		State(Closed).Final().
		Initial(Disconnected).
		CurrentStateFn(func(Link) Conn { return Disconnected }).
		Quench()
}

// connHarness wires the exemplar's three host drivers around one instance: the
// ServiceRunner (dial), the Scheduler (connect-timeout backoff), and the
// ActorSystem (worker actors). It is the realistic runtime the e2e test and the
// e2e benchmark drive every Fire's effects through.
type connHarness struct {
	inst *state.Instance[Conn, ConnEvent, Link]
	run  *state.ServiceRunner[Conn, ConnEvent, Link]
	sch  *state.Scheduler[Conn, ConnEvent, Link]
	sys  *state.ActorSystem[Conn, ConnEvent, Link]
	clk  *state.FakeClock
}

// newConnHarness casts a fresh connection instance and wires all three drivers,
// arming the initial configuration's effects. Each onDone reducer reads its result
// from the re-fired done event's payload, so no back-reference to the drivers is
// needed.
func newConnHarness() *connHarness {
	m := buildConnMachine()

	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m.Cast(Link{Admitted: true, Healthy: true},
		state.WithInitialState(Disconnected), state.WithClock[Conn](clk),
		state.WithFullTrace[Conn]())
	run := state.NewServiceRunner(inst, nil)
	sys := state.NewActorSystem(inst).Register("worker", workerBehavior())
	sch := state.NewScheduler(inst)

	h := &connHarness{inst: inst, run: run, sch: sch, sys: sys, clk: clk}
	h.absorb(context.Background(), inst.StartEffects())
	return h
}

// fire drives one event through the instance and routes the resulting effects
// through every host driver, so timers arm, services start, and actors spawn as
// a real host would. It returns the FireResult for assertions.
func (h *connHarness) fire(ctx context.Context, ev ConnEvent) state.FireResult[Conn] {
	res := h.inst.Fire(ctx, ev)
	h.absorb(ctx, res.Effects)
	return res
}

// absorb feeds an effect slice to all three drivers; each ignores effects not its
// own, so one call wires schedule/service/actor effects uniformly.
func (h *connHarness) absorb(ctx context.Context, effects []state.Effect) {
	h.run.Absorb(ctx, effects)
	h.sch.Absorb(ctx, effects)
	h.sys.AbsorbFor(ctx, nil, effects)
}

// dialID is the stable id of the dial service invoked while Connecting is active.
func (h *connHarness) dialID() string { return state.InvokeID("connection", Connecting, 0) }

// settleDial settles the in-flight dial service, routing Dialed on success or
// DialFailed on failure through the instance, and absorbs the follow-on effects.
func (h *connHarness) settleDial(ctx context.Context, ok bool) state.FireResult[Conn] {
	if ok {
		fr, _ := h.run.SettleDone(ctx, h.dialID(), "session-token")
		h.absorb(ctx, fr.Effects)
		return fr
	}
	fr, _ := h.run.SettleError(ctx, h.dialID(), errors.New("transient dial failure"))
	h.absorb(ctx, fr.Effects)
	return fr
}

// advance moves the FakeClock past the connect timeout and ticks the Scheduler,
// firing the delayed Retry backoff and absorbing its effects.
func (h *connHarness) advancePastTimeout(ctx context.Context) []state.FireResult[Conn] {
	h.clk.Advance(ConnectTimeout)
	out := h.sch.Tick(ctx)
	for _, fr := range out {
		h.absorb(ctx, fr.Effects)
	}
	return out
}

// runWorkers delivers taskRun to every live worker actor, stepping each to its
// final state so its completion sendParents Completed back through the connection.
func (h *connHarness) runWorkers(ctx context.Context) {
	for _, id := range h.sys.IDs() {
		ref, ok := h.sys.Ref(id)
		if !ok {
			continue
		}
		h.sys.Deliver(ctx, ref, taskRun)
	}
}
