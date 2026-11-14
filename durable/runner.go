package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/stablekernel/crucible/state"
)

// Runner is the durable wrapper around a state.Machine: it drives the kernel's
// pure transition function while recording each step to a Store, so an instance
// can be checkpointed, crash, and resume by replaying the recorded driving
// events rather than re-deriving them.
//
// A Runner is created with NewRunner and bound to one machine and one Store; it
// is safe to drive many instances (distinguished by InstanceID) through a single
// Runner. The recording model is write-ahead: every Fire persists its Record
// before returning, so a crash after a successful Fire never loses the step.
//
// # Record / replay model
//
// For an event-driven machine every transition is a pure function of
// (configuration, context, event payload, machine definition). The only input a
// Runner must record to reproduce a run is therefore the driving event and any
// payload it carried: each Fire appends a Record{Step, Event} where Step is the
// step's ordinal and Event is the kernel's structured Trace.EventPayload,
// plus the payload's JSON form for a state.WithEventData Fire. Periodically — governed by
// the checkpoint policy (WithCheckpointEvery) — the Runner also persists a full
// marshaled Snapshot and compacts the journal tail through that step, so recovery
// replays only the tail after the latest checkpoint rather than the whole run.
//
// The first nondeterministic source recorded as Record.Entries is the clock: a
// Runner owns each instance's delayed-transition scheduler and wraps the clock
// (WithRunnerClock) so every reading the scheduler consumes — arming a timer's
// deadline or testing dueness — is journaled and returned verbatim on recovery,
// making timer-driven transitions durable and replay wall-clock-independent.
//
// Invoked services are the second recorded seam: a service runs on the live path
// (Handle.RunService, against the registry supplied with WithServiceRegistry) and its
// result is journaled as a JournalServiceResult; on recovery the recorded result is
// replayed back through the kernel's settle seam, so that service is never re-invoked
// and the same onDone / onError event re-fires with the same data. A result that wasn't
// recorded has nothing to replay, so the service runs again when the host calls
// RunService.
//
// Child-machine actors are the third recorded seam: an actor's behavior runs
// on the live path (Handle.DeliverToActor, against the palette
// supplied with WithActorPalette) and each parent transition the delivery drives —
// the actor's onDone / onError, or a message it sends — is journaled as a
// JournalActorMessage; on recovery the recorded transition is replayed by re-firing
// the parent directly with the recorded done-data, so the actor behavior is never
// re-instantiated. A Fire's or RunService's Record also journals the settles and
// failed spawns' onErrors its absorb drove, replayed after its own work, and the step
// after Start or Recover journals the ones their spawns drove, replayed first. A
// purely event-driven machine records no Entries.
type Runner[S comparable, E comparable, C any] struct {
	machine *state.Machine[S, E, C]
	store   Store
	cfg     runnerConfig[S, E, C]
}

// NewRunner binds a machine and a Store into a durable Runner. Behavior is tuned
// with functional options — the checkpoint policy (WithCheckpointEvery) and the
// event codec (WithEventCodec) — each additive and defaulting to a working
// baseline (no periodic checkpoint, JSON event encoding).
func NewRunner[S comparable, E comparable, C any](m *state.Machine[S, E, C], st Store, opts ...Option[S, E, C]) *Runner[S, E, C] {
	return &Runner[S, E, C]{
		machine: m,
		store:   st,
		cfg:     resolveRunner(opts...),
	}
}

// Handle is a live durable instance: the recovered or freshly started kernel
// Instance bound to its Runner and InstanceID, so subsequent Fires continue to
// record. It owns the instance's delayed-transition scheduler and the recording
// clock that journals every reading the scheduler consumes, so timer-driven
// transitions are durable and replay wall-clock-independent. Obtain a Handle from
// Runner.Start or Recover.
//
// Fire, Tick, RunService and DeliverToActor are safe to call from different
// goroutines: each holds the Handle's step lock while it applies to the kernel and
// writes its Record, including any service or actor it runs. A step's domain effects
// dispatch after it releases the lock, so an effect handler may call back into the
// Handle. Effects of steps on different goroutines can dispatch in either order. Steps
// fired one after another dispatch in order, except that a step fired from inside an
// effect handler dispatches before the rest of that handler's step. A checkpoint waits
// until every step at or before it has dispatched, so it never compacts an effect still
// being sent. An effect handler that panics fails the Handle like one that returns an
// error.
//
// Code that runs under the lock (a service, guard, action, reducer or actor behavior)
// can't act on the Handle itself; to act on the Handle when a step happens, an action
// returns an effect, and its handler runs after the lock and may call back. A call back
// during a live step that carries the step's context, as a service gets and a guard,
// action or reducer finds in its input's Ctx, returns ErrReentrantStep at once. One
// without that context deadlocks, and so does one under middleware that passed down a
// context not derived from the step's. A step waiting for the lock returns its context's
// error once the context ends, and changes nothing. View reads the instance under the
// lock; Instance returns it outside the lock. A stateless Runner.Fire builds its own
// Handle, so it isn't serialized with this one.
//
// A step that applied to the kernel but may not have been recorded leaves the Handle out
// of step with the journal, and so does one that panicked in host code; the panic
// reaches the caller after the Handle fails. So does a Tick, RunService or DeliverToActor whose context
// ended during the step, and any step, Fire included, that drove a Fire refused because
// a context ended, the caller's or one middleware derived from it. That call and every later one return ErrHandleFailed, wrapping the cause,
// which can be a context that ended during the Store write. Before recovering, call
// Drain: a step on another goroutine may still be recording, which a recovered Handle
// wouldn't load, or sending an effect, which it would send again. Recovering a healthy
// Handle's instance instead needs this Handle's calls to have returned first. Recover a
// new Handle with the same options to continue. A service
// whose result wasn't recorded is in flight again, so RunService runs it a second time.
// A running InvokeActor actor restarts from its initial state on any Recover, losing every
// delivery so far, and a Spawn actor doesn't return. If the cause is ErrFenced, a
// newer owner holds the instance: stop, because recovering won't help.
type Handle[S comparable, E comparable, C any] struct {
	runner *Runner[S, E, C]
	id     InstanceID
	inst   *state.Instance[S, E, C]

	// lock is the step lock: a step holds its one slot while it applies to the instance
	// and records, and it guards the instance and every field below except out.
	lock     chan struct{}
	nextStep int

	sched    *state.Scheduler[S, E, C]
	recClock *recordingClock         // owns the clock-read accumulator and its mutex
	timers   map[string]pendingTimer // armed `after` timers with absolute deadlines, persisted per checkpoint

	svc    *state.ServiceRunner[S, E, C] // host driver for invoked services, nil when none wired
	svcBuf *[]state.JournalEntry         // recording service-result accumulator, flushed per step

	actors     *state.ActorSystem[S, E, C] // host driver for child-machine actors, nil when none wired
	ended      recordedSet                 // the ended actors the journal last recorded
	unstarted  recordedSet                 // the unstarted actors the journal last recorded
	seen       map[string]bool             // declared actors seen running, so a stopped one counts as ended
	lastSets   *actorSetsAt[S]             // what actorSets last computed, and from what
	pending    []state.ParentFire[S]       // parent Fires Start's or Recover's spawns drove, journaled with the next step
	actorBuf   *[]state.JournalEntry       // recording actor-transition accumulator, flushed per step
	actorFires *parentFires[S]             // the parent Fires the actor system drove, taken per step

	out outbox // what a step leaves behind once it releases the step lock
}

// outbox holds what outlives a step's hold on the step lock: the failure latch, the
// steps whose domain effects are still being dispatched, and the checkpoint waiting on
// them. A step's effects dispatch after it releases the step lock, so the checkpoint
// policy can fall due while an earlier step's effect is still unmarked; compacting
// through that step would leave nothing for recovery to redispatch. So a due
// checkpoint waits until every step at or before it has dispatched, and the step that
// finishes last writes it. The mutex is taken inside the step lock, never around it. It
// is held across a pending checkpoint's Store write, so a slow Checkpoint delays the
// next step's registration, as it did when checkpoints ran under the step lock.
type outbox struct {
	mu           sync.Mutex
	failed       error            // why the Handle is out of step with the journal, nil until then
	dispatching  map[int]struct{} // steps whose effects are recorded but not yet dispatched
	pending      *dueCheckpoint   // the newest due checkpoint, waiting on dispatching
	checkpointed int              // the newest step this Handle checkpointed through
	idle         chan struct{}    // closed when dispatching empties; nil while it's empty
}

// dueCheckpoint is a checkpoint the policy made due at step, with the snapshot the
// step recorded.
type dueCheckpoint struct {
	step     int
	snapshot []byte
	retry    bool // an earlier write failed, so the Store may already hold it
	waitDue  bool // the Store refused it: wait for a step the policy makes due
}

// stepKey marks a context passed to host code during a step, so a call from that code
// back into the same Handle fails at once instead of waiting forever for the step lock.
type stepKey struct{}

// stepMark records the Handles whose steps a context is running inside.
type stepMark struct {
	handle any
	outer  *stepMark
}

// stepContext returns ctx marked as inside a step of h.
func (h *Handle[S, E, C]) stepContext(ctx context.Context) context.Context {
	outer, _ := ctx.Value(stepKey{}).(*stepMark)
	return context.WithValue(ctx, stepKey{}, &stepMark{handle: h, outer: outer})
}

// inStep reports whether ctx is running inside a step of h.
func (h *Handle[S, E, C]) inStep(ctx context.Context) bool {
	m, _ := ctx.Value(stepKey{}).(*stepMark)
	for ; m != nil; m = m.outer {
		if m.handle == any(h) {
			return true
		}
	}
	return false
}

// acquire takes the step lock, or returns ctx's error if ctx ends first, or
// ErrReentrantStep if ctx is already inside a step of h.
func (h *Handle[S, E, C]) acquire(ctx context.Context) error {
	if h.inStep(ctx) {
		return fmt.Errorf("%w: %q", ErrReentrantStep, h.id)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case h.lock <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release gives up the step lock.
func (h *Handle[S, E, C]) release() { <-h.lock }

// dispatchKey marks the context an effect handler gets, so a call it makes back into
// the Handle is known to come from inside a dispatch.
type dispatchKey struct{}

// dispatchContext returns ctx marked as dispatching h's effects.
func (h *Handle[S, E, C]) dispatchContext(ctx context.Context) context.Context {
	outer, _ := ctx.Value(dispatchKey{}).(*stepMark)
	return context.WithValue(ctx, dispatchKey{}, &stepMark{handle: h, outer: outer})
}

// inDispatch reports whether ctx is an effect handler's, dispatching h's effects.
func (h *Handle[S, E, C]) inDispatch(ctx context.Context) bool {
	m, _ := ctx.Value(dispatchKey{}).(*stepMark)
	for ; m != nil; m = m.outer {
		if m.handle == any(h) {
			return true
		}
	}
	return false
}

// Drain waits until no step of h is still recording and no domain effect of h is still
// being dispatched, or ctx ends. Call it after ErrHandleFailed and before Recover: a
// step on another goroutine may still be appending its Record, which a recovered
// Handle wouldn't load, or sending an effect it recorded, which a recovered Handle
// would send again because it isn't marked yet. Drain first waits for the step lock, so
// a step that was recording has finished, then for dispatch. Once the Handle has
// failed, no step can start, so Drain covers every step and effect. On a healthy Handle
// it covers only work already under way: a step can start after Drain returns, so
// before handing the instance to a new Handle, stop driving this one and let its calls
// return. It returns ctx's error if ctx ends first, at once if ctx has already ended:
// then nothing is drained, so don't Recover until Drain returns nil. Give it a context
// of its own, not the failed call's. It returns ErrReentrantStep, without waiting, when
// called from inside a step or an effect handler of h, whose own work it would wait on
// forever.
// A call from a goroutine an effect handler started, without the handler's context,
// can't be told apart and waits for that handler; one from a goroutine a service or a
// View callback started, without the step's context, waits for that step.
func (h *Handle[S, E, C]) Drain(ctx context.Context) error {
	if h.inStep(ctx) || h.inDispatch(ctx) {
		return fmt.Errorf("%w: %q: Drain from inside a step or an effect handler", ErrReentrantStep, h.id)
	}
	// A step that holds the lock may be partway through its Append. Wait it out, so
	// Recover loads its Record and any effect it registers is covered below.
	if err := h.acquire(ctx); err != nil {
		return err
	}
	h.release()
	h.out.mu.Lock()
	idle := h.out.idle
	h.out.mu.Unlock()
	if idle == nil {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// refuse reports why a step that holds the lock must not run: ErrHandleFailed once
// the Handle is out of step with the journal, or ctx's error if ctx ended as the lock
// was taken. Either way the step changes nothing. A Tick that ran on an ended context
// would consume a due timer whose Fire the kernel then refuses.
func (h *Handle[S, E, C]) refuse(ctx context.Context) error {
	h.out.mu.Lock()
	err := h.refuseLocked()
	h.out.mu.Unlock()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// refuseLocked is refuse with h.out.mu held.
func (h *Handle[S, E, C]) refuseLocked() error {
	if h.out.failed == nil {
		return nil
	}
	return fmt.Errorf("%w: %q: %w", ErrHandleFailed, h.id, h.out.failed)
}

// fail records err as the reason the Handle is out of step with the journal, unless
// an earlier step already failed it, and returns the refusal every later step gets.
func (h *Handle[S, E, C]) fail(err error) error {
	h.out.mu.Lock()
	defer h.out.mu.Unlock()
	return h.failLocked(err)
}

// failLocked is fail with h.out.mu held. A failed Handle writes no more checkpoints.
func (h *Handle[S, E, C]) failLocked(err error) error {
	if h.out.failed == nil {
		h.out.failed = err
	}
	h.out.pending = nil
	return h.refuseLocked()
}

// drainClock returns and clears the clock readings accumulated since the last
// drain, so each recorded step carries exactly the readings consumed during it.
// The drain is taken under the recording clock's mutex, so closing a step's
// Record is safe against a Now() the scheduler's timer goroutine issues
// concurrently (the documented "Tick from its own timer loop" pattern).
func (h *Handle[S, E, C]) drainClock() []state.JournalEntry {
	if h.recClock == nil {
		return nil
	}
	return h.recClock.drain()
}

// absorbDrivers feeds a transition's effects into every wired host driver so the
// durable seams compose: the scheduler arms or cancels delayed (`after`) timers (and
// the Handle's timer table mirrors their deadlines for checkpointing), the service
// runner starts or stops invoked services, and the actor system spawns or stops
// child-machine actors. It is the shared absorb the live seam methods (Fire,
// RunService, DeliverToActor, Tick) call for the effects they produced, so a state a
// settle or delivery enters arms whatever it declares — a service whose onDone spawns
// an actor, an actor whose onDone arms a timer, and so on. Absorbing a driver's
// effects is a no-op when that driver is not wired or the effects name none of its
// kind.
func (h *Handle[S, E, C]) absorbDrivers(ctx context.Context, effects []state.Effect) {
	// Arm or cancel timers only when the effects actually schedule or cancel one: the
	// kernel Scheduler reads the clock unconditionally on Absorb, so skipping the call
	// for a timer-free transition keeps a non-timer machine free of clock reads.
	if hasTimerEffect(effects) {
		h.absorbTimers(ctx, effects)
	}
	if h.svc != nil {
		h.svc.Absorb(ctx, effects)
	}
	if h.actors != nil {
		h.actorFires.attempt(effects)
		h.actors.Absorb(ctx, effects)
	}
}

// absorbParentFire arms or cancels the timers and services a parent Fire the actor
// system drove declares, which the system itself leaves alone: the state an actor's
// onDone, onError or message enters runs its timers and services, as one a Fire enters
// does. Replay arms them the same way, in the same order, so the clock reads match.
func (h *Handle[S, E, C]) absorbParentFire(effects []state.Effect) {
	ctx := context.Background()
	if hasTimerEffect(effects) {
		h.absorbTimers(ctx, effects)
	}
	if h.svc != nil {
		h.svc.Absorb(ctx, effects)
	}
}

// absorbTimers arms or cancels the step's delayed (`after`) timers on the kernel
// scheduler and mirrors the same arming into the Handle's own timer table, so each
// armed timer's absolute deadline is available to persist at a checkpoint. It must
// be called only for a step that scheduled or canceled a timer (hasTimerEffect),
// matching the kernel Scheduler's own unconditional clock read.
//
// The kernel Scheduler.Absorb reads the recording clock exactly once — that
// reading is the `now` it computes every deadline against — and the reading lands
// in the recording clock's buffer. The Handle captures it by noting the buffer's
// length (markBuf) before the Absorb and reading the entry the Absorb appended
// (armReadingAt), both under the clock mutex, so the durable table mirrors the
// scheduler's deadlines with no extra clock read (an extra read would corrupt the
// recorded sequence) and stays correct against a concurrent timer-goroutine
// Now(). When no recording clock is wired the Handle falls back to a direct clock
// read, keeping the table correct off the recording path.
func (h *Handle[S, E, C]) absorbTimers(ctx context.Context, effects []state.Effect) {
	before := -1
	if h.recClock != nil {
		before = h.recClock.markBuf()
	}
	h.sched.Absorb(ctx, effects)
	now := h.armNow(before)
	for _, eff := range effects {
		switch e := eff.(type) {
		case state.ScheduleAfter:
			if h.timers == nil {
				h.timers = map[string]pendingTimer{}
			}
			h.timers[e.ID] = pendingTimer{id: e.ID, due: now.Add(e.Delay), event: e.Event}
		case state.CancelScheduled:
			delete(h.timers, e.ID)
		}
	}
}

// armNow returns the clock instant the just-completed Scheduler.Absorb computed its
// deadlines against: the clock read the Absorb appended to the recording clock's
// buffer at index before, captured under the clock mutex so the Handle's timer
// table mirrors the scheduler without a second clock read. It falls back to a
// fresh clock read only when no recording clock is wired (off the recording path).
func (h *Handle[S, E, C]) armNow(before int) time.Time {
	if before >= 0 && h.recClock != nil {
		if now, ok := h.recClock.armReadingAt(before); ok {
			return now
		}
	}
	return h.runner.cfg.clock.Now()
}

// armTimersFrom mirrors the ScheduleAfter/CancelScheduled effects a chained timer
// transition emitted into the Handle's timer table at the tick instant now, so a
// timer a fired transition re-armed (the next link in an `after` chain) is tracked
// for the next checkpoint. It is the Tick-side analog of absorbTimers, sharing the
// tick's single recorded instant rather than reading the clock again.
func (h *Handle[S, E, C]) armTimersFrom(effects []state.Effect, now time.Time) {
	for _, eff := range effects {
		switch e := eff.(type) {
		case state.ScheduleAfter:
			if h.timers == nil {
				h.timers = map[string]pendingTimer{}
			}
			h.timers[e.ID] = pendingTimer{id: e.ID, due: now.Add(e.Delay), event: e.Event}
		case state.CancelScheduled:
			delete(h.timers, e.ID)
		}
	}
}

// dropFiredTimers removes from the Handle's timer table the timers a Tick fired, so
// a checkpoint taken after the tick does not persist an already-fired timer. The
// scheduler removes a due timer before re-firing it; the Handle mirrors that by
// dropping every timer whose deadline is at or before now, matching the scheduler's
// dueLocked selection.
func (h *Handle[S, E, C]) dropFiredTimers(now time.Time) {
	for id, pt := range h.timers {
		if !pt.due.After(now) {
			delete(h.timers, id)
		}
	}
}

// Instance returns the underlying kernel Instance the Handle wraps, for reads
// such as Configuration, Snapshot, or Current. Drive it through the Handle's Fire
// (or the Runner's) so steps continue to be recorded; firing the bare Instance
// bypasses durability. It's outside the Handle's step lock, so read it only while no
// step is running, or use View. After ErrHandleFailed it shows state the journal may
// not hold.
func (h *Handle[S, E, C]) Instance() *state.Instance[S, E, C] { return h.inst }

// View runs fn with the live instance while it holds the Handle's step lock, so the
// read can't race a step on another goroutine. fn must not call back into the Handle,
// and must not keep the instance past its return. View returns ctx's error if ctx ends
// before the lock is free, ErrReentrantStep if ctx is a step's own context, and
// ErrHandleFailed once the Handle has failed, without running fn.
func (h *Handle[S, E, C]) View(ctx context.Context, fn func(*state.Instance[S, E, C])) error {
	if err := h.acquire(ctx); err != nil {
		return err
	}
	defer h.release()
	if err := h.refuse(ctx); err != nil {
		return err
	}
	fn(h.inst)
	return nil
}

// ID returns the InstanceID the Handle records under.
func (h *Handle[S, E, C]) ID() InstanceID { return h.id }

// Start creates and registers a fresh durable instance: it casts the machine on
// input, persists a baseline checkpoint so the instance is loadable from the
// first step, and returns a live Handle. Cast options (for example
// state.WithInitialState) configure the initial configuration. Starting an
// InstanceID that already exists in the Store reports ErrInstanceExists rather
// than clobbering its recorded baseline.
//
// If a context ending (ctx's, or one middleware derived from it) refuses a parent Fire
// an initial actor drove, such as a failed spawn's onError, Start returns an error
// wrapping the context's error and no Handle. The baseline is recorded by then, so
// Recover the instance rather than Start it again. A ctx that ends with no Fire refused
// doesn't fail Start.
func (r *Runner[S, E, C]) Start(ctx context.Context, id InstanceID, input C, opts ...state.CastOption[S]) (*Handle[S, E, C], error) {
	if _, _, err := r.store.Load(ctx, id); err == nil {
		return nil, fmt.Errorf("%w: %q", ErrInstanceExists, id)
	} else if !errors.Is(err, ErrInstanceNotFound) {
		return nil, fmt.Errorf("durable: probing instance %q: %w", id, err)
	}

	// Install the recording clock so every reading the delayed-transition
	// scheduler consumes is journaled for replay. The clock seam is wired at Cast
	// (WithClock); the scheduler reads it when arming and ticking timers.
	buf := make([]state.JournalEntry, 0)
	recClock := newRecordingClock(r.cfg.clock, &buf)
	// Force unbounded full-trace history: the runner journals each step's Trace
	// (notably the EventPayload) and derives the checkpoint snapshots from the
	// instance's retained Trace history, so a recover can replay it
	// byte-identically. WithUnboundedHistory elevates to full trace and retains every
	// step; it is prepended so a caller's opts cannot downgrade it.
	castOpts := append([]state.CastOption[S]{state.WithClock[S](recClock), state.WithUnboundedHistory[S]()}, opts...)
	inst := r.machine.Cast(input, castOpts...)

	// Persist a baseline checkpoint at baselineStep (below the first fired step)
	// so the instance is reconstructable from the Store before any event is fired.
	// The baseline is the cast snapshot; the tail then accumulates fired steps on
	// top of it.
	snap, err := state.MarshalSnapshot(inst.Snapshot())
	if err != nil {
		return nil, fmt.Errorf("durable: marshaling start snapshot for %q: %w", id, err)
	}
	// Wrap the baseline snapshot in a checkpoint envelope (no timers armed yet) so
	// every checkpoint a recover loads carries the same durable envelope shape.
	baseEnv, err := marshalCheckpoint(checkpointEnvelope{Snapshot: snap}, nil)
	if err != nil {
		return nil, fmt.Errorf("durable: marshaling start checkpoint envelope for %q: %w", id, err)
	}
	if err := r.store.Checkpoint(ctx, id, baseEnv, baselineStep, WithCheckpointFence(r.cfg.fence)); err != nil {
		return nil, fmt.Errorf("durable: checkpointing start baseline for %q: %w", id, err)
	}

	// Install the invoked-service host driver and arm the initial configuration's
	// services, so a service declared on the very first state is in flight before
	// the first RunService. The service buffer journals each settled outcome for
	// replay. A machine with no service registry wired runs no services and records
	// no service entries.
	svcBuf := make([]state.JournalEntry, 0)
	svc := r.newServiceRunner(inst)
	if svc != nil {
		svc.Absorb(ctx, inst.StartEffects())
	}

	// Install the child-machine actor host driver and spawn the initial
	// configuration's actors, so an actor declared on the very first state is running
	// before the first DeliverToActor. The actor buffer journals each parent
	// transition a delivery drives for replay. A machine with no actor palette wired
	// spawns no actor and records no actor entries. A parent Fire a starting actor
	// drives arms its state's timers and services on the Handle's drivers.
	actorBuf := make([]state.JournalEntry, 0)
	actors, actorFires := r.newActorSystem(inst)
	h := &Handle[S, E, C]{
		runner:     r,
		id:         id,
		inst:       inst,
		nextStep:   0,
		sched:      state.NewScheduler(inst),
		recClock:   recClock,
		timers:     map[string]pendingTimer{},
		svc:        svc,
		svcBuf:     &svcBuf,
		actors:     actors,
		actorBuf:   &actorBuf,
		actorFires: actorFires,
		lock:       make(chan struct{}, 1),
		out:        outbox{dispatching: map[int]struct{}{}, checkpointed: baselineStep},
	}
	if actors != nil {
		actorFires.arm = h.absorbParentFire
		actorFires.attempt(inst.StartEffects())
		actors.Absorb(ctx, inst.StartEffects())
	}
	// A parent Fire a starting actor drove (a message it sent as it started, or a failed
	// spawn's onError) applied to the instance after its baseline, so the first step
	// journals it, ahead of its own work. One a context's ending refused never applied,
	// and replaying it under a live context would apply it, so Start fails: Recover
	// starts from the baseline. A context that ended with no Fire refused cost nothing.
	startFires := actorFires.take()
	if refused := refusedByContext(parentFireErrs(startFires)...); refused != nil {
		return nil, fmt.Errorf("durable: starting %q: %w", id, refused)
	}
	// The actors that started are seen running, so one later stopped counts as ended.
	h.seen = map[string]bool{}
	if actors != nil {
		for _, id := range actors.IDs() {
			h.seen[id] = true
		}
	}
	h.pending = startFires
	return h, nil
}

// newServiceRunner builds the kernel host driver for invoked services bound to
// inst, or nil when no service registry was wired (a purely event/timer-driven
// machine invokes none).
func (r *Runner[S, E, C]) newServiceRunner(inst *state.Instance[S, E, C]) *state.ServiceRunner[S, E, C] {
	if r.cfg.serviceReg == nil {
		return nil
	}
	return state.NewServiceRunner(inst, r.cfg.serviceReg)
}

// newActorSystem builds the kernel host driver for child-machine actors bound to
// inst, registering each behavior in the configured palette, and the collector of the
// parent Fires it drives. Both are nil when no actor palette was wired (a machine
// that spawns no actor needs none).
func (r *Runner[S, E, C]) newActorSystem(inst *state.Instance[S, E, C]) (*state.ActorSystem[S, E, C], *parentFires[S]) {
	if len(r.cfg.actorPalette) == 0 {
		return nil, nil
	}
	fires := &parentFires[S]{}
	sys := state.NewActorSystem(inst).WithParentFireObserver(fires.note).
		WithActorInspector(state.InspectorFunc(fires.inspect))
	for src, behavior := range r.cfg.actorPalette {
		sys.Register(src, behavior)
	}
	return sys, fires
}

// parentFires collects the parent Fires an actor system drives, so the step that drove
// them can check and record them, and the actors it spawned, so an actor spawned and
// stopped inside one step still counts as having run.
type parentFires[S comparable] struct {
	// arm, when set, arms the timers and services a parent Fire's effects declare, as a
	// host Fire's absorb does: the actor system handles only their actors. It's set
	// before any spawn and runs on the goroutine driving the system.
	arm func(effects []state.Effect)

	mu        sync.Mutex
	fires     []state.ParentFire[S]
	spawned   map[string]bool // actors whose latest spawn this step started
	attempted map[string]bool // actors the system was asked to spawn this step
	failed    map[string]bool // actors whose latest spawn failed and whose onError applied
}

// attempt notes the actors effects ask the system to spawn.
func (p *parentFires[S]) attempt(effects []state.Effect) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, eff := range effects {
		if sp, ok := eff.(state.SpawnActor); ok {
			if p.attempted == nil {
				p.attempted = map[string]bool{}
			}
			p.attempted[sp.ID] = true
			// The latest attempt decides: until it starts, the actor hasn't run again.
			delete(p.spawned, sp.ID)
			delete(p.failed, sp.ID)
		}
	}
}

// takeLifecycle returns the actors spawned and the actors asked to spawn since the
// last call, and forgets them. It's safe on a nil collector, which has none.
func (p *parentFires[S]) takeLifecycle() (spawned, attempted, failed map[string]bool) {
	if p == nil {
		return nil, nil, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	spawned, attempted, failed = p.spawned, p.attempted, p.failed
	p.spawned, p.attempted, p.failed = nil, nil, nil
	return spawned, attempted, failed
}

// inspect notes each actor the system spawned. The system reports a spawn only once
// the actor started, so a spawn that failed isn't noted.
func (p *parentFires[S]) inspect(ev state.InspectionEvent) {
	if ev.Kind != state.InspectActor || ev.ActorPhase != state.ActorSpawned {
		return
	}
	p.mu.Lock()
	if p.spawned == nil {
		p.spawned = map[string]bool{}
	}
	p.spawned[ev.ActorID] = true
	p.mu.Unlock()
}

func (p *parentFires[S]) note(pf state.ParentFire[S]) {
	p.mu.Lock()
	p.fires = append(p.fires, pf)
	// A spawn whose onError applied is handled: the journal holds the onError, so
	// retrying the spawn would apply it twice.
	if pf.SpawnFailed && pf.Result.Err == nil {
		if p.failed == nil {
			p.failed = map[string]bool{}
		}
		p.failed[pf.ActorID] = true
	}
	p.mu.Unlock()
	if p.arm != nil {
		p.arm(pf.Result.Effects)
	}
	// The system absorbs a parent Fire's effects next, which spawns what they declare.
	p.attempt(pf.Result.Effects)
}

// take returns the Fires noted since the last take, in fire order, and forgets them.
// It's safe on a nil collector, which has none.
func (p *parentFires[S]) take() []state.ParentFire[S] {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	fires := p.fires
	p.fires = nil
	return fires
}

// parentFireErrs returns each parent Fire's error, nil ones included.
func parentFireErrs[S comparable](fires []state.ParentFire[S]) []error {
	errs := make([]error, len(fires))
	for i := range fires {
		errs[i] = fires[i].Result.Err
	}
	return errs
}

// Fire drives one event through a durable instance identified by id, loading and
// replaying it from the Store first, then recording the step. Handle.Fire describes
// how a state.WithEventData payload is handled, and when it's refused. It is the
// stateless entry point (no Handle required); for a hot path that fires many
// events in sequence, hold a Handle from Start or Recover and use Handle.Fire to
// avoid reloading between steps. Calls on one id aren't serialized, and must not overlap
// each other or a live Handle.
func (r *Runner[S, E, C]) Fire(ctx context.Context, id InstanceID, event E, opts ...state.FireOption) (state.FireResult[S], error) {
	h, err := r.recover(ctx, id)
	if err != nil {
		return state.FireResult[S]{}, err
	}
	return h.Fire(ctx, event, opts...)
}

// Fire drives one event through the Handle's live instance and records the step:
// it Fires the kernel, appends a Record carrying the driving event at the
// step's ordinal (write-ahead, before returning), and — when the
// checkpoint policy is due — persists a full Snapshot and compacts the tail. A
// kernel transition error journals nothing (no step was produced) and is
// returned to the caller. A failed checkpoint returns the result and that error: the
// step is recorded and its effects sent, the Handle carries on, and the next step the
// policy makes due checkpoints. A checkpoint this step wrote for an earlier one, which
// failed because ctx ended, isn't reported, since this step succeeded. If another
// goroutine failed the Handle while this step was recording, Fire returns
// ErrHandleFailed saying the step was recorded; Recover sends its effects.
//
// A Fire the kernel refuses, because ctx ended or for any other reason, returns that
// error, records nothing and leaves the Handle usable. When the event applies, an actor
// the step spawned can fail and route its onError through the instance; if a context
// ending, ctx or one derived from it, refuses that onError, Fire fails the Handle with
// ErrHandleFailed and records nothing, though the event applied. Recover restores the
// state from before the event, so fire it again on the recovered Handle, and don't act
// on the result Fire returned with that error. A Fire that drove no Fire a context
// refused records, even if ctx ends after the kernel applied it. A panic in host code
// during the step fails the Handle, then reaches the caller: recover it, then Drain
// and Recover as for ErrHandleFailed.
//
// A state.WithEventData payload reaches the transition's Assign as the Runner's
// EventDataDecoder rebuilds it from the payload's JSON form, live and on replay alike, so
// both see the same value. Decode it as the type the caller passed: a payload that
// decodes as another type refuses the Fire with ErrEventDataEncoding before the kernel
// runs. The default decoder gives JSON's own form, so it refuses a struct or an int
// until WithEventDataDecoder covers that event. An error arrives
// as an error with the same message, with any invalid UTF-8 replaced, though errors.Is
// no longer matches it. Keep its message, not the error, in context: a checkpoint turns
// an error into a map, and Recover fails on an error-typed field. A payload that can't
// take this form, an error whose Error panics (such as a typed nil), or one over
// WithMaxEventDataBytes, also refuses the Fire with ErrEventDataEncoding before the
// kernel runs. The payload is read on the calling goroutine with no lock of its own, so
// don't mutate it concurrently with Fire.
func (h *Handle[S, E, C]) Fire(ctx context.Context, event E, opts ...state.FireOption) (state.FireResult[S], error) {
	if err := h.acquire(ctx); err != nil {
		return state.FireResult[S]{}, err
	}
	res, rec, des, err := h.lockedFireStep(ctx, event, opts)
	if err != nil {
		return res, err
	}
	// Dispatch after releasing the step lock, so an effect handler can call back into
	// this Handle. The Record already names every effect (write-ahead), so a crash here
	// leaves it for recovery to redispatch, deduped by id.
	return res, h.finishStep(ctx, rec, des)
}

// lockedFireStep runs fireStep and releases the step lock however it returns. A panic
// in host code fails the Handle first, since the kernel may have applied the event.
func (h *Handle[S, E, C]) lockedFireStep(ctx context.Context, event E, opts []state.FireOption) (state.FireResult[S], *Record, []dispatchableEffect, error) {
	defer h.release()
	defer h.failOnPanic()
	return h.fireStep(h.stepContext(ctx), event, opts)
}

// fireStep is the part of Fire that holds the step lock: it applies the event to the
// kernel and appends the step's Record, naming its domain effects, before any is
// dispatched. ctx is the step's marked context.
func (h *Handle[S, E, C]) fireStep(ctx context.Context, event E, opts []state.FireOption) (state.FireResult[S], *Record, []dispatchableEffect, error) {
	if err := h.refuse(ctx); err != nil {
		return state.FireResult[S]{}, nil, nil, err
	}
	// Encode the WithEventData payload before the kernel runs, so a payload that
	// can't be recorded refuses the Fire rather than applying a step it can't record.
	payload, opts, err := encodeEventData(event, opts, h.runner.cfg.maxEventData, h.runner.cfg.eventDataDecoder)
	if err != nil {
		return state.FireResult[S]{}, nil, nil, fmt.Errorf("durable: recording event data for %q: %w", h.id, err)
	}
	res := h.inst.Fire(ctx, event, opts...)
	if res.Err != nil {
		return res, nil, nil, res.Err
	}

	// Absorb the step's driver effects into every host driver — timers, services, and
	// actors — so a transition that enters a state declaring any combination of them
	// arms each for the seam method that settles it next. The seams compose: a Fire
	// may enter a state that both invokes a service and spawns an actor and arms a
	// timer, and each driver must see its effects.
	h.absorbDrivers(ctx, res.Effects)

	// An actor the absorb spawned can fail and route its onError through the parent.
	// A context that ended can refuse that Fire, so live never applied it, and
	// journaling this step would let Recover re-spawn the actor under a live context,
	// where the onError applies. Record nothing. A Fire that drove no Fire a context refused
	// applied whole, so it records even when ctx ended afterward.
	fires := h.actorFires.take()
	if refused := refusedByContext(parentFireErrs(fires)...); refused != nil {
		return res, nil, nil, h.fail(fmt.Errorf("durable: firing %q: %w", h.id, refused))
	}
	// Every parent Fire the absorb drove (a message an actor sent as it spawned, a
	// settle, a failed spawn's onError) applied after the event, so the step journals
	// them, and replay re-fires them after it.
	driven, err := recordParentFires(fires, false)
	if err != nil {
		return res, nil, nil, h.fail(fmt.Errorf("durable: recording %q's parent Fires: %w", h.id, err))
	}

	step := h.nextStep
	// Stamp this step's domain effects with their deterministic ids and carry them
	// in the Record so the write-ahead append durably names every effect that must
	// be dispatched before any dispatch happens.
	des := dispatchableEffects(step, res.Effects)
	effEnvs, err := recordEffects(des)
	if err != nil {
		return res, nil, nil, h.fail(err)
	}
	rec := &Record{Step: step, Event: []byte(res.Trace.EventPayload), EventData: payload.data, EventDataError: payload.errMsg, Entries: append(h.drainClock(), driven...), Effects: effEnvs}
	if err := h.appendStep(ctx, step, rec); err != nil {
		return res, nil, nil, h.fail(err)
	}
	h.nextStep = rec.followingStep()
	h.out.mu.Lock()
	defer h.out.mu.Unlock()
	// Another goroutine's dispatch may have failed the Handle while this step held the
	// lock, and its caller may already be recovering. Leave this step's effects unmarked
	// in the journal for that Recover to send, rather than race it.
	if err := h.recordedOnFailedLocked(step); err != nil {
		return res, nil, nil, err
	}
	if len(des) > 0 {
		if len(h.out.dispatching) == 0 {
			h.out.idle = make(chan struct{})
		}
		h.out.dispatching[step] = struct{}{}
	}
	return res, rec, des, nil
}

// finishStep dispatches a recorded step's domain effects, outside the step lock, then
// offers its checkpoint. A failed dispatch fails the Handle: the effect stays unmarked
// in the journal, and Recover dispatches it again. A handler that panics fails it the
// same way, rather than leave the step dispatching forever, which would stop every
// later checkpoint.
func (h *Handle[S, E, C]) finishStep(ctx context.Context, rec *Record, des []dispatchableEffect) error {
	var dispatchErr error
	if len(des) > 0 {
		dispatchErr = h.dispatchGuarded(h.dispatchContext(ctx), rec.Step, des)
	}
	h.out.mu.Lock()
	defer h.out.mu.Unlock()
	if len(des) > 0 {
		delete(h.out.dispatching, rec.Step)
		if len(h.out.dispatching) == 0 && h.out.idle != nil {
			close(h.out.idle)
			h.out.idle = nil
		}
	}
	if dispatchErr != nil {
		return h.failLocked(dispatchErr)
	}
	return h.offerCheckpointLocked(ctx, rec)
}

// dispatchGuarded dispatches des, turning a panic in the effect handler, or in the
// Store's dispatch tracking, into an ErrEffectDispatch error.
func (h *Handle[S, E, C]) dispatchGuarded(ctx context.Context, step int, des []dispatchableEffect) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%w: step %d for %q: effect dispatch panicked: %v", ErrEffectDispatch, step, h.id, p)
		}
	}()
	return h.dispatchEffects(ctx, des)
}

// Tick advances the Handle's delayed-transition scheduler: it fires every timer
// whose recorded deadline is at or before the recording clock's current time, in
// due order, re-firing the delayed events through the durable instance and
// recording the clock readings the tick consumed. A host calls it from its own
// timer loop (with a real clock) or a test calls it after advancing a fake clock.
// It records one tick barrier — carrying the consumed clock readings and the
// count of timers fired — so recovery re-derives the same timers at their
// recorded instants. It returns the FireResults of the timers it fired, in order. A
// failed checkpoint returns them with that error, and the Handle carries on. If another
// goroutine failed the Handle while this step was recording, it returns them with
// ErrHandleFailed saying the step was recorded. If ctx ends during the Tick, or a
// context derived from it (by middleware, say) refuses a timer's Fire, it fails the
// Handle with ErrHandleFailed and records nothing, even for timers that applied: the
// results it returns weren't recorded, so don't act on their effects; a recovered
// Handle re-arms those timers and fires them again. A timer's Fire refused for another
// reason, such as a failing guard, is recorded, and replay meets the same refusal.
func (h *Handle[S, E, C]) Tick(ctx context.Context) ([]state.FireResult[S], error) {
	if err := h.acquire(ctx); err != nil {
		return nil, err
	}
	defer h.release()
	defer h.failOnPanic()
	if err := h.refuse(ctx); err != nil {
		return nil, err
	}
	// The tick's first read lands past what the buffer holds now, which can include reads
	// of an earlier Start's or Recover's spawns, recorded with this step.
	mark := h.recClock.markBuf()
	results := h.sched.Tick(h.stepContext(ctx))
	now, haveNow := h.recClock.armReadingAt(mark)
	// A context that ended partway can make the kernel refuse a due timer's Fire after
	// the scheduler consumed it. Journaling that would commit on recovery what live never
	// applied, so record nothing: Recover re-arms the timers.
	// A tick that fired no timer consumed nothing, so an ended ctx can't have cost it
	// anything: it records as usual.
	if err := h.consumedCanceled(ctx, len(results) > 0, append(resultErrs(results), parentFireErrs(h.actorFires.take())...)...); err != nil {
		return results, h.fail(fmt.Errorf("durable: ticking %q: %w", h.id, err))
	}

	step := h.nextStep
	rec := Record{Step: step, Tick: true, TickSteps: len(results), Entries: h.drainClock()}
	// Reconcile the Handle's timer table with the tick: drop the timers it fired and
	// arm any a chained transition scheduled, so a checkpoint after the tick persists
	// exactly the still-pending deadlines. The clock does not advance within one tick
	// (a FakeClock advances only between ticks; a SystemClock's intra-tick drift is
	// sub-timer), so the tick's recorded reads share one instant: use it for both the
	// drop (timers due at or before it fired) and any chained re-arm.
	if haveNow {
		h.dropFiredTimers(now)
		for i := range results {
			h.armTimersFrom(results[i].Effects, now)
		}
	}
	// A tick that read no clock and fired nothing records nothing: there is no
	// nondeterminism to journal and no step was produced.
	if len(rec.Entries) == 0 && rec.TickSteps == 0 {
		return results, nil
	}
	if err := h.persistStep(ctx, step, &rec); err != nil {
		return results, err
	}
	return results, nil
}

// persistStep write-ahead appends rec for a step with no domain effects to dispatch,
// advances the next step, then offers its checkpoint. A failed append fails the
// Handle, because the kernel already applied the step; a failed checkpoint doesn't,
// because the Record is durable. This relies on the Append running inline, under the
// step lock, before the step returns: an Append deferred off this goroutine would
// acknowledge a step whose outcome isn't known, and the Handle couldn't fail.
func (h *Handle[S, E, C]) persistStep(ctx context.Context, step int, rec *Record) error {
	if err := h.appendStep(ctx, step, rec); err != nil {
		return h.fail(err)
	}
	h.nextStep = rec.followingStep()
	h.out.mu.Lock()
	defer h.out.mu.Unlock()
	if err := h.recordedOnFailedLocked(step); err != nil {
		return err
	}
	return h.offerCheckpointLocked(ctx, rec)
}

// recordedOnFailedLocked reports a step that was recorded while another goroutine failed
// the Handle: ErrHandleFailed, saying the step was recorded and Recover sends any of its
// effects. It returns nil on a healthy Handle. h.out.mu must be held.
func (h *Handle[S, E, C]) recordedOnFailedLocked(step int) error {
	if err := h.refuseLocked(); err != nil {
		return fmt.Errorf("%w (step %d was recorded; Recover sends any of its effects)", err, step)
	}
	return nil
}

// offerCheckpointLocked makes rec's checkpoint pending when the policy is due at its
// step, then writes the pending checkpoint if no step at or before it is still
// dispatching. Otherwise the step that finishes dispatching last writes it, with its
// context. When that write fails because the context of a step that didn't make it due
// ended, that caller isn't told, since its own step succeeded, and the next step writes
// it. Any other failure is returned to the caller, and the checkpoint waits for the next
// step the policy makes due, whose newer checkpoint replaces it. A retry the Store
// refuses as not advancing landed the first time, and counts as written. A failed
// Handle writes none. h.out.mu must be held.
func (h *Handle[S, E, C]) offerCheckpointLocked(ctx context.Context, rec *Record) error {
	if h.out.failed != nil {
		return nil
	}
	if h.checkpointDue(rec.Step) && rec.Snapshot != nil && (h.out.pending == nil || rec.Step > h.out.pending.step) {
		h.out.pending = &dueCheckpoint{step: rec.Step, snapshot: rec.Snapshot}
	}
	cp := h.out.pending
	if cp == nil || (cp.waitDue && !h.checkpointDue(rec.Step)) {
		return nil
	}
	for step := range h.out.dispatching {
		if step <= cp.step {
			return nil
		}
	}
	h.out.pending = nil
	if cp.step <= h.out.checkpointed {
		return nil
	}
	if err := h.runner.store.Checkpoint(ctx, h.id, cp.snapshot, cp.step, WithCheckpointFence(h.runner.cfg.fence)); err != nil {
		if cp.retry && errors.Is(err, ErrCheckpointNotAdvancing) {
			// The failed write landed after all: the Store holds this checkpoint.
			h.out.checkpointed = cp.step
			return nil
		}
		cp.retry = true
		h.out.pending = cp
		if cp.step != rec.Step && ctx.Err() != nil && isContextErr(err) {
			return nil // not this caller's checkpoint: the next step writes it
		}
		cp.waitDue = true
		return fmt.Errorf("durable: checkpointing step %d for %q: %w", cp.step, h.id, err)
	}
	h.out.checkpointed = cp.step
	return nil
}

// checkpointDue reports whether the checkpoint policy lands a checkpoint at step.
func (h *Handle[S, E, C]) checkpointDue(step int) bool {
	return h.runner.cfg.checkpointEvery > 0 && (step+1)%h.runner.cfg.checkpointEvery == 0
}

// appendStep marshals the due checkpoint snapshot into rec (so the Record carries it)
// and write-ahead appends rec, WITHOUT compacting the tail yet. Splitting the append
// from the compaction lets a step's domain effects dispatch after the durable append
// but before any checkpoint compacts that step away — so a crash between append and
// dispatch leaves the effect recoverable from the still-present tail rather than
// orphaned behind the compaction.
func (h *Handle[S, E, C]) appendStep(ctx context.Context, step int, rec *Record) error {
	ended, unstarted := h.actorSets()
	held := len(h.pending)
	if held > 0 {
		before, err := recordParentFires(h.pending, true)
		if err != nil {
			return fmt.Errorf("durable: recording step %d for %q: %w", step, h.id, err)
		}
		// They ran before the step's own work, so they come first.
		rec.Entries = append(before, rec.Entries...)
	}
	if h.checkpointDue(step) {
		snap, err := state.MarshalSnapshot(h.inst.Snapshot())
		if err != nil {
			return fmt.Errorf("durable: marshaling checkpoint at step %d for %q: %w", step, h.id, err)
		}
		// Wrap the kernel snapshot with the absolute deadlines of the timers armed at
		// this checkpoint, so a recovery whose compacted tail no longer carries their
		// arming ScheduleAfter can re-arm them at their recorded instants.
		env, err := marshalCheckpoint(checkpointEnvelope{
			Snapshot:        snap,
			NextStep:        rec.followingStep(),
			EndedActors:     ended,
			UnstartedActors: unstarted,
			EndedServices:   h.endedServices(),
		}, h.timers)
		if err != nil {
			return fmt.Errorf("durable: marshaling checkpoint envelope at step %d for %q: %w", step, h.id, err)
		}
		rec.Snapshot = env
	}
	if h.ended.changedBy(ended) {
		rec.EndedActors = ended
	}
	if h.unstarted.changedBy(unstarted) {
		rec.UnstartedActors = unstarted
	}
	if _, err := h.runner.store.Append(ctx, h.id, *rec, WithFence(h.runner.cfg.fence)); err != nil {
		return fmt.Errorf("durable: recording step %d for %q: %w", step, h.id, err)
	}
	h.ended.note(ended)
	h.unstarted.note(unstarted)
	h.pending = h.pending[held:]
	return nil
}

// recordedSet is an actor set the journal records when a step changes it.
type recordedSet struct {
	ids []string // the set the journal last recorded
	set bool     // whether ids holds a recorded set
}

// changedBy reports whether now differs from the set the journal last recorded, so the
// step records it. A nil now records nothing.
func (r *recordedSet) changedBy(now *ActorSet) bool {
	return now != nil && (!r.set || !slices.Equal(now.IDs, r.ids))
}

// note remembers now as recorded, once the step that carries it is appended.
func (r *recordedSet) note(now *ActorSet) {
	if now != nil {
		r.ids, r.set = now.IDs, true
	}
}

// actorSetsAt is what actorSets last computed, and the configuration and running
// actors it computed them from.
type actorSetsAt[S comparable] struct {
	config           []S
	running          []string
	ended, unstarted *ActorSet
}

// actorSets returns the actors the instance's state declares that ran and have ended,
// and those that aren't running and haven't ended, and remembers them for the next
// step. A declared actor that isn't running has ended when it settled (the actor system
// keeps a settled actor), or when this step asked the system to spawn it and the latest
// of those spawns started it (then something stopped it), or, when this step didn't try
// to spawn it, when it was running or ended at the previous step. A spawn whose onError
// applied also counts as ended, since the journal holds that onError. So an actor whose
// spawn failed with no onError, at entry or on a re-entry, hasn't ended: it's unstarted,
// and Recover tries it again. Both are nil with no actor palette.
//
// A step that noted no spawn and changed neither the configuration nor the running
// actors can't change either set, so it gets the last ones back without a walk of the
// declared actors.
func (h *Handle[S, E, C]) actorSets() (ended, unstarted *ActorSet) {
	if h.actors == nil {
		return nil, nil
	}
	spawned, attempted, failed := h.actorFires.takeLifecycle()
	running := h.actors.IDs()
	config := h.inst.Configuration()
	if last := h.lastSets; last != nil && len(spawned) == 0 && len(attempted) == 0 && len(failed) == 0 &&
		slices.Equal(running, last.running) && slices.Equal(config, last.config) {
		return last.ended, last.unstarted
	}
	endedIDs, unstartedIDs := []string{}, []string{}
	for _, id := range declaredActors(h.inst.StartEffects()) {
		if slices.Contains(running, id) {
			continue
		}
		_, ran := h.actors.Ref(id)
		switch {
		case ran:
		case attempted[id]:
			ran = spawned[id] || failed[id]
		default:
			ran = h.seen[id]
		}
		if ran {
			endedIDs = append(endedIDs, id)
		} else {
			unstartedIDs = append(unstartedIDs, id)
		}
	}
	slices.Sort(endedIDs)
	endedIDs = slices.Compact(endedIDs)
	slices.Sort(unstartedIDs)
	unstartedIDs = slices.Compact(unstartedIDs)
	h.seen = map[string]bool{}
	for _, id := range append(running, endedIDs...) {
		h.seen[id] = true
	}
	ended, unstarted = &ActorSet{IDs: endedIDs}, &ActorSet{IDs: unstartedIDs}
	h.lastSets = &actorSetsAt[S]{config: config, running: running, ended: ended, unstarted: unstarted}
	return ended, unstarted
}

// endedServices returns the invoked services the active states declare that aren't in
// flight, sorted: each settled while the state that declares it stayed active. It is nil
// with no service registry.
func (h *Handle[S, E, C]) endedServices() []string {
	if h.svc == nil {
		return nil
	}
	var ids []string
	for _, eff := range h.inst.StartEffects() {
		if sv, ok := eff.(state.StartService); ok && !h.svc.HasPending(sv.ID) {
			ids = append(ids, sv.ID)
		}
	}
	slices.Sort(ids)
	return ids
}

// declaredActors returns the ids of the actors effects spawn.
func declaredActors(effects []state.Effect) []string {
	var ids []string
	for _, eff := range effects {
		if sp, ok := eff.(state.SpawnActor); ok {
			ids = append(ids, sp.ID)
		}
	}
	return ids
}

// startsExcept returns effects without the starts of kind T (a SpawnActor or a
// StartService) whose id, as id reads it, ids holds. Recover uses it so it doesn't
// spawn an actor the live run had ended, or start a service that settled while its
// state stayed active. The kind matters: an explicit invoke id can name both an actor
// and a service. With no ids it returns effects unchanged.
func startsExcept[T state.Effect](effects []state.Effect, ids []string, id func(T) string) []state.Effect {
	if len(ids) == 0 {
		return effects
	}
	out := make([]state.Effect, 0, len(effects))
	for _, eff := range effects {
		if start, ok := eff.(T); ok && slices.Contains(ids, id(start)) {
			continue
		}
		out = append(out, eff)
	}
	return out
}

// respawn spawns the actors effects declare. With a recorded unstarted set, an actor it
// leaves out was running, and the journal holds every parent Fire its start drove, so it
// spawns without letting its start reach the parent. An unstarted actor, or any actor
// when the journal recorded no set, spawns as new. The running actors spawn first, in
// order, then the rest: a message a new actor sends the parent can enter a state that
// spawns a running actor, which must already be running, or its start would reach the
// parent a second time.
func respawn[S comparable, E comparable, C any](ctx context.Context, actors *state.ActorSystem[S, E, C], effects []state.Effect, unstarted *ActorSet) {
	var running, fresh []state.Effect
	for _, eff := range effects {
		if sp, ok := eff.(state.SpawnActor); ok && unstarted != nil && !slices.Contains(unstarted.IDs, sp.ID) {
			running = append(running, eff)
			continue
		}
		fresh = append(fresh, eff)
	}
	actors.AbsorbRespawn(ctx, running)
	actors.Absorb(ctx, fresh)
}

// Recover reconstructs a durable instance purely from the Store: it loads the
// latest checkpoint Snapshot and the journal/effect tail after it, Restores the
// snapshot (firing nothing, no IO), and replays the tail's recorded driving
// events through the kernel to reach the instance's live state. The returned
// Handle continues recording subsequent Fires. Recover reports ErrInstanceNotFound
// for an instance that was never started, and the context's error if ctx ends or a
// context's ending refuses a parent Fire an actor it spawned drove.
func Recover[S comparable, E comparable, C any](ctx context.Context, m *state.Machine[S, E, C], st Store, id InstanceID, opts ...Option[S, E, C]) (*Handle[S, E, C], error) {
	r := NewRunner(m, st, opts...)
	return r.recover(ctx, id)
}

// recover is the Runner-bound reconstruction shared by Recover and the stateless
// Fire: Load, Restore under the replay clock, replay the recorded tail by
// reproducing the live driver sequence (Fire+Absorb for external steps, Tick for
// scheduler barriers), then continue live against the recording clock.
func (r *Runner[S, E, C]) recover(ctx context.Context, id InstanceID) (*Handle[S, E, C], error) {
	snapBytes, tail, err := r.store.Load(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("durable: loading instance %q: %w", id, err)
	}
	if snapBytes == nil {
		return nil, fmt.Errorf("durable: instance %q has no checkpoint to restore", id)
	}

	// Unwrap the durable checkpoint envelope: the kernel snapshot bytes, the absolute
	// deadlines of the timers armed at the checkpoint, and the next step ordinal. A bare
	// (pre-envelope) checkpoint yields the snapshot with no persisted timers.
	env := unmarshalCheckpoint(snapBytes)
	kernelSnap, persistedTimers, checkpointNext := env.Snapshot, env.Timers, env.NextStep

	snap, err := state.UnmarshalSnapshot[S, E, C](kernelSnap)
	if err != nil {
		return nil, fmt.Errorf("durable: unmarshaling checkpoint for %q: %w", id, err)
	}

	// The recovery clock replays the recorded readings in order while the cursor
	// is unexhausted, then falls through to a recording clock so live continuation
	// after replay keeps journaling. The Handle's recording buffer is that
	// fallback's buffer, so post-replay reads record durably.
	buf := make([]state.JournalEntry, 0)
	recClock := newRecordingClock(r.cfg.clock, &buf)
	repClock := newReplayClock(clockReadings(tail), recClock)

	inst, err := r.machine.Restore(snap, state.WithRestoreClock[S](repClock), state.WithRestoreUnboundedHistory[S]())
	if err != nil {
		return nil, fmt.Errorf("durable: restoring checkpoint for %q: %w", id, err)
	}
	sched := state.NewScheduler(inst)

	// Install the invoked-service host driver and arm the restored configuration's
	// services, so a service in flight at the checkpoint (or armed by a replayed
	// step) can be re-settled from its recorded outcome without re-running it. A
	// service that had settled by the checkpoint while its state stayed active isn't
	// armed: it would run again.
	svcBuf := make([]state.JournalEntry, 0)
	svc := r.newServiceRunner(inst)
	if svc != nil {
		svc.Absorb(ctx, startsExcept(inst.StartEffects(), env.EndedServices, func(sv state.StartService) string { return sv.ID }))
	}

	// Build the child-machine actor host driver, but DO NOT spawn the restored
	// configuration's actors yet: the recorded run's actors are reconstructed by
	// replaying their parent transitions directly (replayActor), not by re-running
	// behavior. Spawning here would re-instantiate an actor the recorded run already
	// settled. The live actors at the resume point are armed once, after replay,
	// from the final configuration's StartEffects.
	actorBuf := make([]state.JournalEntry, 0)
	actors, actorFires := r.newActorSystem(inst)

	// armed mirrors the scheduler's pending timers re-established by tail replay,
	// with their recorded deadlines, so a persisted-but-compacted timer is re-armed
	// only when the tail did not already re-arm it, and the Handle's table is seeded
	// with every still-pending timer's deadline for the next checkpoint.
	armed := map[string]pendingTimer{}

	// ended is the latest recorded set of the declared actors the live run had ended:
	// the checkpoint's, then each tail Record's that records one. Nil means none was
	// recorded, so Recover spawns every actor the configuration declares.
	ended := env.EndedActors
	// unstarted is the latest recorded set of the declared actors that weren't running
	// and hadn't ended, recorded alongside ended. Nil means none was recorded.
	unstarted := env.UnstartedActors

	for i := range tail {
		// A canceled context can stop a replayed settle or tick with no error, leaving
		// state short of the journal, so check it before each Record.
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("durable: recovering %q: %w", id, err)
		}
		rec := &tail[i]
		// The mirror tracks the still-pending timers in armed as replay arms, cancels and
		// fires them, with deadlines drawn from the recorded clock reads, not the live
		// wall clock.
		did, err := replayRecord(ctx, inst, &r.cfg, sched, svc, rec, &armMirror{armed: armed, clock: repClock})
		if err != nil {
			return nil, fmt.Errorf("durable: recovering %q: %w", id, err)
		}
		if rec.EndedActors != nil {
			ended = rec.EndedActors
		}
		if rec.UnstartedActors != nil {
			unstarted = rec.UnstartedActors
		}
		if did.fired != nil {
			// Re-dispatch this step's domain effects, deduped by the Store's dispatched
			// set: an effect already marked (it landed before the crash) is skipped; one
			// recorded-but-un-marked (the crash fell between append and dispatch) is
			// applied now — exactly-once across the crash boundary.
			if err := r.dispatchReplayEffects(ctx, id, rec.Step, did.fired.Effects); err != nil {
				return nil, err
			}
		}
	}

	// Drain any clock readings the replay re-recorded after the cursor exhausted
	// (none in the common case where replay consumes exactly the recorded
	// readings): they belong to no new step and must not leak into the next Fire.
	buf = buf[:0]

	// Re-arming reads the clock past the journal, and so can arming a timer for a parent
	// Fire an actor Recover spawns drives, which the next step records: re-arm before the
	// spawns, so the re-arm's read can be dropped while theirs are kept, and so one of
	// their Fires can cancel a re-armed timer.
	//
	// Re-arm any timer that was pending at the checkpoint but whose arming
	// ScheduleAfter was compacted out of the tail, so it survives compaction: the
	// tail replay could not re-arm it (its arm is gone), so re-arm it here at its
	// persisted absolute deadline. Already-armed timers (re-established by the tail)
	// are skipped, so a timer the tail still carried is never double-armed. The
	// remaining delay is the persisted deadline minus the recovery clock's now, so
	// the re-armed timer fires at its recorded instant — an already-elapsed deadline
	// arms as immediately due and fires on the next Tick.
	reEffects, reSeeded, reErr := reArmEffects(r.cfg.eventCodec, persistedTimers, armed, repClock.Now())
	if reErr != nil {
		return nil, fmt.Errorf("durable: re-arming pending timers for %q: %w", id, reErr)
	}
	if len(reEffects) > 0 {
		sched.Absorb(ctx, reEffects)
	}
	for id, pt := range reSeeded {
		armed[id] = pt
	}
	// The re-arm reads the recovery clock to compute remaining delays; that reading
	// belongs to no recorded step and must not leak into the next Fire's Record.
	buf = buf[:0]

	// The next step ordinal continues past the highest assigned step. Step ordinals
	// are assigned monotonically: each Record consumes one ordinal regardless of how
	// many parent Traces it produced (an actor delivery can drive several), except a
	// scheduler-tick barrier, which consumed one ordinal per timer it fired plus the
	// barrier's own. The next ordinal is therefore the last tail Record's Step plus
	// its own span. When the tail is empty (a fresh checkpoint compacted every
	// Record), the checkpoint carries the next ordinal. A Start baseline carries none and
	// has no Traces, so its count of zero is right. A checkpoint from before this field
	// falls back to the Trace count, which can be off (see checkpointEnvelope.NextStep):
	// a Store that reports its last step raises it past every recorded step, so no
	// Append conflicts. A step number inside a compacted tick barrier's span carries
	// nothing, so skipping one is harmless.
	next := checkpointNext
	if n := len(tail); n > 0 {
		next = tail[n-1].followingStep()
	} else if next == 0 {
		next = len(inst.History())
		if sr, ok := r.store.(StepReporter); ok {
			last, found, err := sr.LastStep(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("durable: reading the last step of %q: %w", id, err)
			}
			if found && last+1 > next {
				next = last + 1
			}
		}
	}
	// A parent Fire the actors Recover spawns drive arms its state's timers and services
	// on this Handle's drivers.
	h := &Handle[S, E, C]{
		runner:     r,
		id:         id,
		inst:       inst,
		nextStep:   next,
		sched:      sched,
		recClock:   recClock,
		timers:     armed,
		svc:        svc,
		svcBuf:     &svcBuf,
		actors:     actors,
		actorBuf:   &actorBuf,
		actorFires: actorFires,
		lock:       make(chan struct{}, 1),
		out:        outbox{dispatching: map[int]struct{}{}, checkpointed: baselineStep},
	}

	// Arm the live actors at the resume point from the final configuration's
	// StartEffects: this spawns exactly the actors running where replay left the
	// parent (for example the next child in a chain, after the prior one settled),
	// so a subsequent DeliverToActor finds them — without re-instantiating an actor
	// the recorded run already settled, which replay reconstructed by re-firing the
	// parent rather than re-running behavior.
	//
	// An actor that was running sends the parent nothing as it spawns again: the journal
	// holds what its start drove.
	if actors != nil {
		// With no recorded ended set, Recover spawns every declared actor, as it did
		// before the set was recorded.
		var endedIDs []string
		if ended != nil {
			endedIDs = ended.IDs
		}
		spawns := startsExcept(inst.StartEffects(), endedIDs, func(sp state.SpawnActor) string { return sp.ID })
		actorFires.arm = h.absorbParentFire
		actorFires.attempt(spawns)
		respawn(ctx, actors, spawns, unstarted)
	}
	// A parent Fire a newly started actor drove (a message it sent as it started, or a
	// failed spawn's onError) applied to the instance past its journal, so the next step
	// journals it, ahead of its own work. One a context's ending refused never applied,
	// and the Handle would hold state the journal can't reach, so Recover fails.
	recoverFires := actorFires.take()
	if refused := refusedByContext(parentFireErrs(recoverFires)...); refused != nil {
		return nil, fmt.Errorf("durable: recovering %q: %w", id, refused)
	}
	// The journal's ended actors stay ended, and seen, so the next step records a set
	// only when it changes them.
	h.seen = map[string]bool{}
	if actors != nil {
		for _, id := range actors.IDs() {
			h.seen[id] = true
		}
		h.ended.note(ended)
		h.unstarted.note(unstarted)
		if ended != nil {
			for _, id := range ended.IDs {
				h.seen[id] = true
			}
		}
	}

	h.pending = recoverFires
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("durable: recovering %q: %w", id, err)
	}
	return h, nil
}

// eventPayload is the recorded form of a WithEventData payload.
type eventPayload struct {
	data   []byte  // the JSON form, nil for none or for an error payload
	errMsg *string // an error payload's message
}

// encodeEventData returns the recorded form of the WithEventData payload in opts and
// the options to Fire with, which carry the payload as replay will see it, so the live
// run and a replay can't differ. With no payload it returns opts unchanged, because
// adding one would replace the event in AssignCtx.Event. It copies opts rather than
// appending, which could write into the caller's slice.
//
// The live run gets the payload back through the Runner's EventDataDecoder, as replay
// does. A payload that comes back as a different type than the caller passed refuses
// the Fire, so a reducer can't be handed a map where its caller passed a struct.
func encodeEventData[E comparable](event E, opts []state.FireOption, maxBytes int, decode EventDataDecoder[E]) (eventPayload, []state.FireOption, error) {
	data, ok := state.FireEventData(opts...)
	if !ok {
		return eventPayload{}, opts, nil
	}
	var payload eventPayload
	encoded := data
	e, isErr := data.(error)
	if isErr {
		msg, err := errorMessage(e)
		if err != nil {
			return payload, nil, fmt.Errorf("%w: %w", ErrEventDataEncoding, err)
		}
		encoded = msg
	}
	b, err := marshalPayload(encoded)
	if err != nil {
		return payload, nil, fmt.Errorf("%w: %w", ErrEventDataEncoding, err)
	}
	if len(b) > maxBytes {
		return payload, nil, eventDataTooLarge(len(b), maxBytes)
	}
	if isErr {
		// Take the message back from its JSON form, which is what a Store keeps: JSON
		// replaces invalid UTF-8, so the raw message could differ from a replay's.
		var msg string
		if err = json.Unmarshal(b, &msg); err != nil {
			return payload, nil, fmt.Errorf("%w: %w", ErrEventDataEncoding, err)
		}
		payload.errMsg = &msg
	} else {
		payload.data = b
	}
	value, err := decodePayload(payload, event, decode)
	if err != nil {
		return payload, nil, fmt.Errorf("%w: %w", ErrEventDataEncoding, err)
	}
	if !isErr {
		if got, want := reflect.TypeOf(value), reflect.TypeOf(data); got != want {
			return payload, nil, fmt.Errorf("%w: a %v payload decodes as %v; decode it as %v with WithEventDataDecoder",
				ErrEventDataEncoding, want, got, want)
		}
	}
	live := make([]state.FireOption, 0, len(opts)+1)
	return payload, append(append(live, opts...), state.WithEventData(value)), nil
}

// errorMessage returns e.Error(), or an error if it panics, as a typed nil can.
func errorMessage(e error) (msg string, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("error payload panicked: %v", p)
		}
	}()
	return e.Error(), nil
}

// marshalPayload returns v's JSON form, or an error if a custom marshaler panics, so
// a caller's payload can't panic out of Fire.
func marshalPayload(v any) (b []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("payload panicked while encoding: %v", p)
		}
	}()
	return json.Marshal(v)
}

// eventDataTooLarge reports a payload over the Runner's cap.
func eventDataTooLarge(size, maxBytes int) error {
	return fmt.Errorf("%w: payload is %d bytes, over the %d byte limit", ErrEventDataEncoding, size, maxBytes)
}

// decode returns the payload as a Fire sees it, live and when a replay re-Fires the
// step: an error payload as an error with its message, and data through decode.
func decodePayload[E comparable](p eventPayload, event E, decode EventDataDecoder[E]) (v any, err error) {
	if p.errMsg != nil {
		return errors.New(*p.errMsg), nil
	}
	if string(p.data) == "null" { // a nil payload stays nil, whatever the decoder
		return nil, nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("event data decoder panicked: %v", r)
		}
	}()
	return decode(event, p.data)
}

// EventDataDecoder rebuilds a state.WithEventData payload from its recorded JSON form,
// for the live Fire and for every replay of it. It gets the event the payload rode
// with, so one decoder serves a machine whose events carry different payload types.
// Return the payload as the same type the caller passed: a Fire whose payload decodes
// as another type is refused with ErrEventDataEncoding. Only the top-level type is
// checked, so a []any of structs still arrives as a []any of maps. An untyped nil
// payload arrives as nil without reaching the decoder; a typed nil, such as a nil
// pointer or map, encodes as null and is refused as a type mismatch. The default decodes with
// DecodeAs[any]. Supply one with WithEventDataDecoder.
type EventDataDecoder[E comparable] func(event E, data []byte) (any, error)

// DecodeAs decodes a recorded payload into a T, the usual body of an EventDataDecoder
// case: DecodeAs[Order] hands Assign an Order. DecodeAs[any] gives JSON's own form: a
// struct arrives as map[string]any and a number as float64.
func DecodeAs[T any](data []byte) (any, error) {
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// decodeAny is the default EventDataDecoder.
func decodeAny[E comparable](_ E, data []byte) (any, error) { return DecodeAs[any](data) }

// replayFireOptions returns the options that re-Fire rec's event with the payload
// the live Fire carried, decoded the same way, or none when it carried none.
func replayFireOptions[E comparable](rec *Record, event E, decode EventDataDecoder[E]) ([]state.FireOption, error) {
	if len(rec.EventData) == 0 && rec.EventDataError == nil {
		return nil, nil
	}
	value, err := decodePayload(eventPayload{data: rec.EventData, errMsg: rec.EventDataError}, event, decode)
	if err != nil {
		return nil, err
	}
	return []state.FireOption{state.WithEventData(value)}, nil
}

// baselineStep is the Step of the start baseline checkpoint, recorded before any
// event so a freshly started instance is loadable. It sits below the first fired
// step (0).
const baselineStep = -1

// EventCodec encodes and decodes an event value E to and from its structured
// JSON form, the inverse of the kernel's Trace.EventPayload marshaling. It is the
// seam by which Recover reconstructs the exact event to re-Fire. The default
// codec uses encoding/json; supply a custom one with WithEventCodec for events
// the default cannot round-trip.
type EventCodec[E comparable] interface {
	// Decode reconstructs the event value from its recorded payload. An empty
	// payload decodes to the zero event.
	Decode(payload []byte) (E, error)
}

// jsonEventCodec is the default EventCodec: it decodes an event through
// encoding/json, the inverse of the kernel's marshalEventPayload.
type jsonEventCodec[E comparable] struct{}

func (jsonEventCodec[E]) Decode(payload []byte) (E, error) {
	var e E
	if len(payload) == 0 {
		return e, nil
	}
	err := json.Unmarshal(payload, &e)
	return e, err
}

// isContextErr reports whether err is a context's cancellation or deadline, which a
// Store may report for either when the caller's context ends.
func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// stepCanceled reports why a Tick, RunService or DeliverToActor must record nothing:
// a context ended, so a Fire the step drove may have been refused, and live may not
// have applied what the step would journal. That's the caller's ctx having ended, or
// a Fire refused with a context's error (one middleware derived from ctx included)
// among errs, which hold the errors of the Fires the step drove, the parent Fires its
// actors drove among them. These steps check ctx itself because a service or timer
// they consumed can end on it without any Fire reporting it. A refusal with any other
// cause, such as a failing guard, is deterministic: the step is journaled, and replay
// meets the same refusal.
func (h *Handle[S, E, C]) stepCanceled(ctx context.Context, errs ...error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return refusedByContext(errs...)
}

// consumedCanceled is stepCanceled for a step that consumed something (a timer it
// fired, a delivery that drove the parent), and refusedByContext for one that didn't,
// which has nothing an ended ctx could have cost it.
func (h *Handle[S, E, C]) consumedCanceled(ctx context.Context, consumed bool, errs ...error) error {
	if consumed {
		return h.stepCanceled(ctx, errs...)
	}
	return refusedByContext(errs...)
}

// refusedByContext returns the first of errs that is a context's error: a Fire refused
// because the context it ran under ended.
func refusedByContext(errs ...error) error {
	for _, err := range errs {
		if err != nil && isContextErr(err) {
			return err
		}
	}
	return nil
}

// failOnPanic fails the Handle when host code panicked during a step, since the kernel
// may have applied what the step never recorded, then lets the panic go on. A step
// defers it after release, so it runs first, under the step lock.
func (h *Handle[S, E, C]) failOnPanic() {
	if p := recover(); p != nil {
		_ = h.fail(fmt.Errorf("durable: a step of %q panicked: %v", h.id, p))
		panic(p)
	}
}

// resultErrs returns each result's error, nil ones included.
func resultErrs[S comparable](results []state.FireResult[S]) []error {
	errs := make([]error, len(results))
	for i := range results {
		errs[i] = results[i].Err
	}
	return errs
}

// replayed is what replayRecord fired, for a caller that tracks more than the
// instance: a tick barrier's timer fires, or a plain event's Fire.
type replayed[S comparable] struct {
	tick  []state.FireResult[S]
	fired *state.FireResult[S]
}

// replayRecord re-applies one journaled Record to inst the way the live step applied
// it, running no service, actor or effect handler. Recover and StateAt both walk the
// journal with it; Recover adds its deadline table, which mirror tracks, and effect
// re-dispatch on top. A nil mirror tracks no deadlines.
//
//   - Parent Fires a Start's or Recover's spawns drove, recorded with the next step,
//     re-fire first, before the step's own work.
//   - An actor delivery re-fires each parent transition the delivery drove, in fire
//     order, with the recorded done-data or error, so the kernel re-derives the same
//     onDone / onError (or message) transition. A Fire's or settle's absorb-driven
//     parent Fires, such as a spawned actor's onError, re-fire after its own work. Each
//     re-arms the timers and services its state declares, as the live Fire did.
//   - A service settlement re-settles each recorded outcome through the same settle
//     seam, in settle order; the settle absorbs its own follow-on StartService
//     effects, so a chained invoke arms its successor for the next entry, and its
//     timers are re-armed as RunService armed them.
//   - A tick barrier ticks the replay clock, which returns the recorded readings, so
//     the same deadlines come due and the same timers fire at their recorded instants.
//   - A plain event re-fires it with its recorded event data, then re-arms its timers
//     and services as the live Fire did.
//   - A checkpoint-only Record drives nothing.
func replayRecord[S comparable, E comparable, C any](
	ctx context.Context,
	inst *state.Instance[S, E, C],
	cfg *runnerConfig[S, E, C],
	sched *state.Scheduler[S, E, C],
	svc *state.ServiceRunner[S, E, C],
	rec *Record,
	mirror *armMirror,
) (replayed[S], error) {
	var out replayed[S]
	before, rest, err := splitActorEntries(rec)
	if err != nil {
		return out, fmt.Errorf("at step %d: %w", rec.Step, err)
	}
	// Parent Fires a Start's or Recover's spawns drove ran before the step's own work.
	if err := replayActors(ctx, inst, cfg, sched, svc, rec, before, mirror); err != nil {
		return out, err
	}
	switch {
	case len(rec.Event) > 0:
		event, err := cfg.eventCodec.Decode(rec.Event)
		if err != nil {
			return out, fmt.Errorf("decoding recorded event at step %d: %w", rec.Step, err)
		}
		fireOpts, err := replayFireOptions(rec, event, cfg.eventDataDecoder)
		if err != nil {
			return out, fmt.Errorf("decoding recorded event data at step %d: %w", rec.Step, err)
		}
		res := inst.Fire(ctx, event, fireOpts...)
		if res.Err != nil {
			return out, fmt.Errorf("replaying step %d: %w", rec.Step, res.Err)
		}
		replayArm(ctx, sched, svc, res.Effects, mirror)
		out.fired = &res
	case hasServiceEntry(rec):
		for _, entry := range serviceEntries(rec) {
			effects, err := replayService(ctx, svc, entry)
			if err != nil {
				return out, fmt.Errorf("at step %d: %w", rec.Step, err)
			}
			// The settle armed its own services; arm its timers, as RunService did.
			replayArm(ctx, sched, nil, effects, mirror)
		}
	case rec.Tick:
		out.tick = sched.Tick(ctx)
		mirrorTick(mirror, out.tick)
	}
	// The rest are a delivery's parent Fires, or the ones a Fire's or settle's absorb
	// drove after it, such as a spawned actor's onError.
	return out, replayActors(ctx, inst, cfg, sched, svc, rec, rest, mirror)
}

// replayActors re-fires recorded parent transitions, in order, arming the timers and
// services each declares, as the live run did.
func replayActors[S comparable, E comparable, C any](
	ctx context.Context,
	inst *state.Instance[S, E, C],
	cfg *runnerConfig[S, E, C],
	sched *state.Scheduler[S, E, C],
	svc *state.ServiceRunner[S, E, C],
	rec *Record,
	entries []state.JournalEntry,
	mirror *armMirror,
) error {
	for _, entry := range entries {
		effects, err := replayActor(ctx, inst, cfg.eventCodec, entry)
		if err != nil {
			return fmt.Errorf("at step %d: %w", rec.Step, err)
		}
		replayArm(ctx, sched, svc, effects, mirror)
	}
	return nil
}

// replayArm arms the timers and services a replayed Fire's effects declare, as the live
// step's absorb did, reading the clock only when they schedule or cancel a timer, as it
// did. svc may be nil.
func replayArm[S comparable, E comparable, C any](ctx context.Context, sched *state.Scheduler[S, E, C], svc *state.ServiceRunner[S, E, C], effects []state.Effect, mirror *armMirror) {
	if hasTimerEffect(effects) {
		sched.Absorb(ctx, effects)
		mirror.arm(effects)
	}
	if svc != nil {
		svc.Absorb(ctx, effects)
	}
}

// armMirror tracks the timers a Recover's replay leaves pending, with the deadlines the
// scheduler computed from the recorded clock reads, so the Handle's timer table starts
// with them. A nil mirror, as a time-travel read uses, tracks nothing.
type armMirror struct {
	armed map[string]pendingTimer
	clock *replayClock
}

// arm folds a replayed absorb's timer effects into the table, at the reading the
// scheduler's absorb just took.
func (m *armMirror) arm(effects []state.Effect) {
	if m == nil {
		return
	}
	mirrorArmed(m.armed, effects, m.clock.lastRead())
}

// mirrorTick drops from m the timers a replayed tick fired and arms any a chained
// transition scheduled, at the instant the tick read: the tick's reads share one instant.
func mirrorTick[S comparable](m *armMirror, results []state.FireResult[S]) {
	if m == nil {
		return
	}
	now := m.clock.lastRead()
	for id, pt := range m.armed {
		if !pt.due.After(now) {
			delete(m.armed, id)
		}
	}
	for i := range results {
		mirrorArmed(m.armed, results[i].Effects, now)
	}
}
