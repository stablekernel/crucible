package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
// re-instantiated. A purely event-driven machine records no Entries.
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
// goroutines: each holds the Handle's lock for the whole step, including the Store
// write, effect dispatch and any service it runs. Any host callback run during a step
// (an effect handler, service, guard, action, reducer or actor behavior) must not call
// back into the same Handle, or it deadlocks. A step waits for the lock without watching
// its context, so a slow service delays other steps past their deadlines; a Tick,
// RunService or DeliverToActor whose context has ended by then returns that error and
// changes nothing, and the kernel refuses a Fire. Instance returns the
// live instance outside that lock, so read it only while no step is running. A
// stateless Runner.Fire builds its own Handle, so it isn't serialized with this one.
//
// A step that applied to the kernel but may not have been recorded, or a Tick, RunService
// or DeliverToActor whose context ended partway, leaves the Handle out of step with the
// journal. That call and every later one return ErrHandleFailed, wrapping the cause,
// which can be a context that ended during the Store write. Recover a new Handle with
// the same options to continue. A service
// whose result wasn't recorded is in flight again, so RunService runs it a second time.
// A running InvokeActor actor restarts from its initial state on any Recover, losing every
// delivery so far, and a Spawn actor doesn't return. If the cause is ErrFenced, a
// newer owner holds the instance: stop, because recovering won't help. A failed effect
// dispatch fails the Handle the same way, and Recover retries the effect.
type Handle[S comparable, E comparable, C any] struct {
	runner *Runner[S, E, C]
	id     InstanceID
	inst   *state.Instance[S, E, C]

	mu       sync.Mutex // serializes the steps, and guards the instance and every field below
	nextStep int
	failed   error // the error that took the Handle out of step with the journal, nil until then

	sched    *state.Scheduler[S, E, C]
	recClock *recordingClock         // owns the clock-read accumulator and its mutex
	carried  []state.JournalEntry    // a read recovery took for an older settle, for the next step's Record
	timers   map[string]pendingTimer // armed `after` timers with absolute deadlines, persisted per checkpoint

	svc    *state.ServiceRunner[S, E, C] // host driver for invoked services, nil when none wired
	svcBuf *[]state.JournalEntry         // recording service-result accumulator, flushed per step

	actors   *state.ActorSystem[S, E, C] // host driver for child-machine actors, nil when none wired
	actorBuf *[]state.JournalEntry       // recording actor-transition accumulator, flushed per step
}

// refuse reports ErrHandleFailed once the Handle is out of step with the journal.
func (h *Handle[S, E, C]) refuse() error {
	if h.failed == nil {
		return nil
	}
	return fmt.Errorf("%w: %q: %w", ErrHandleFailed, h.id, h.failed)
}

// fail records err as the reason the Handle is out of step with the journal, and
// returns the same refusal every later step gets.
func (h *Handle[S, E, C]) fail(err error) error {
	h.failed = err
	return h.refuse()
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
	out := append(h.carried, h.recClock.drain()...)
	h.carried = nil
	return out
}

// absorbDrivers feeds a transition's effects into every wired host driver so the
// durable seams compose: the scheduler arms or cancels delayed (`after`) timers (and
// the Handle's timer table mirrors their deadlines for checkpointing), the service
// runner starts or stops invoked services, and the actor system spawns or stops
// child-machine actors. It is the shared absorb Fire and RunService call for the
// effects they produced, so a state a settle enters arms whatever it declares, such as
// a service whose onDone spawns an actor. Absorbing a driver's
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
		h.actors.Absorb(ctx, effects)
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
// transition emitted into the Handle's timer table at now, the instant the scheduler
// armed them, so a timer a fired transition re-armed (the next link in an `after` chain)
// is tracked for the next checkpoint. It is the Tick-side analog of absorbTimers, using a
// recorded instant rather than reading the clock again.
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

// absorbRead returns the clock instant the scheduler armed result i's timers at, in a
// tick that fired n timers. A Scheduler.Tick reads the clock once to pick each due timer
// and once to absorb its effects, and a transition reads no clock, so result i's instant
// is read 2i+1 of the tick's own reads, the last tickReads(n) in entries.
func absorbRead(entries []state.JournalEntry, i, n int) (time.Time, bool) {
	var reads []int64
	for _, e := range entries {
		if e.Kind == state.JournalClockRead {
			reads = append(reads, e.ClockUnixNano)
		}
	}
	at := len(reads) - tickReads(n) + 2*i + 1
	if at < 0 || at >= len(reads) {
		return time.Time{}, false
	}
	return time.Unix(0, reads[at]).UTC(), true
}

// dropFiredTimers removes from table every timer the scheduler no longer holds, so a
// checkpoint taken after a tick does not persist one that fired. It asks the scheduler
// rather than comparing deadlines, because a timer can come due partway through a tick,
// or arm and fire within one.
func dropFiredTimers[S comparable, E comparable, C any](table map[string]pendingTimer, sched *state.Scheduler[S, E, C]) {
	for id := range table {
		if !sched.HasPending(id) {
			delete(table, id)
		}
	}
}

// Instance returns the underlying kernel Instance the Handle wraps, for reads
// such as Configuration, Snapshot, or Current. Drive it through the Handle's Fire
// (or the Runner's) so steps continue to be recorded; firing the bare Instance
// bypasses durability. Read it only while no step is running: it's outside the Handle's
// lock. After ErrHandleFailed it shows state the journal may not hold.
func (h *Handle[S, E, C]) Instance() *state.Instance[S, E, C] { return h.inst }

// ID returns the InstanceID the Handle records under.
func (h *Handle[S, E, C]) ID() InstanceID { return h.id }

// Start creates and registers a fresh durable instance: it casts the machine on
// input, persists a baseline checkpoint so the instance is loadable from the
// first step, and returns a live Handle. Cast options (for example
// state.WithInitialState) configure the initial configuration. Starting an
// InstanceID that already exists in the Store reports ErrInstanceExists rather
// than clobbering its recorded baseline.
//
// Start arms the initial state's `after` timers and carries them in the baseline, so a
// Tick fires them and recovery re-arms them.
//
// Start applies the initial entry's domain effects before the baseline write, and only
// if the entry succeeded: a failed entry still returns a Handle and a nil error, and
// Instance().InitialErr reports it. They're at-least-once, unlike a step's: a handler failure or a
// failed baseline write leaves no instance, so calling Start again re-applies every
// effect, as does a crash before the baseline. Overlapping Starts on one id, including a
// stale owner's, each apply them before the Store refuses all but one. To dedupe, key on
// EffectInstance plus the effect id. A handler wired only at Recover never sees them.
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

	// Arm the timers of the state the instance starts in, and carry their deadlines in
	// the baseline, which is what a recovery re-arms them from. The arming clock read
	// belongs to no step, so it doesn't reach the first Record.
	initial := inst.InitialEffects()
	sched := state.NewScheduler(inst)
	timers := map[string]pendingTimer{}
	if hasTimerEffect(initial) {
		sched.Absorb(ctx, initial)
		reads := recClock.drain()
		if n := len(reads); n > 0 {
			mirrorArmed(timers, initial, time.Unix(0, reads[n-1].ClockUnixNano).UTC())
		}
	}

	// Encode a baseline checkpoint at baselineStep (below the first fired step), so the
	// instance is reconstructable from the Store before any event is fired. The baseline
	// is the cast snapshot; the tail then accumulates fired steps on top of it. Encoding
	// comes before the effects, so an instance that can't be encoded applies none.
	snap, err := state.MarshalSnapshot(inst.Snapshot())
	if err != nil {
		return nil, fmt.Errorf("durable: marshaling start snapshot for %q: %w", id, err)
	}
	// Wrap the baseline snapshot in a checkpoint envelope so every checkpoint a recover
	// loads carries the same durable envelope shape.
	baseEnv, err := marshalCheckpoint(snap, timers, 0)
	if err != nil {
		return nil, fmt.Errorf("durable: marshaling start checkpoint envelope for %q: %w", id, err)
	}
	// Apply the initial entry's domain effects before the instance exists: a handler
	// failure then leaves nothing behind, so the caller can Start again. Nothing can
	// re-derive these effects after the baseline, so they're at-least-once. An entry that
	// failed applies none, as a failed Fire dispatches none.
	if inst.InitialErr() == nil {
		if err := r.applyStartEffects(ctx, id, dispatchableEffects(baselineStep, initial)); err != nil {
			return nil, err
		}
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
	// spawns no actor and records no actor entries.
	actorBuf := make([]state.JournalEntry, 0)
	actors := r.newActorSystem(inst)
	if actors != nil {
		actors.Absorb(ctx, inst.StartEffects())
	}

	return &Handle[S, E, C]{
		runner:   r,
		id:       id,
		inst:     inst,
		nextStep: 0,
		sched:    sched,
		recClock: recClock,
		timers:   timers,
		svc:      svc,
		svcBuf:   &svcBuf,
		actors:   actors,
		actorBuf: &actorBuf,
	}, nil
}

// newServiceRunner builds the kernel host driver for invoked services bound to
// inst, or nil when no service registry was wired (a purely event/timer-driven
// machine invokes none).
func (r *Runner[S, E, C]) newServiceRunner(inst *state.Instance[S, E, C]) *state.ServiceRunner[S, E, C] {
	if r.cfg.serviceReg == nil {
		return nil
	}
	return state.NewServiceRunner(inst, r.cfg.serviceReg).WithResultTransform(replayableOutcome)
}

// newActorSystem builds the kernel host driver for child-machine actors bound to
// inst, registering each behavior in the configured palette, or nil when no actor
// palette was wired (a machine that spawns no actor needs none).
func (r *Runner[S, E, C]) newActorSystem(inst *state.Instance[S, E, C]) *state.ActorSystem[S, E, C] {
	if len(r.cfg.actorPalette) == 0 {
		return nil
	}
	sys := state.NewActorSystem(inst)
	for src, behavior := range r.cfg.actorPalette {
		sys.Register(src, behavior)
	}
	return sys
}

// Fire drives one event through a durable instance identified by id, loading and
// replaying it from the Store first, then recording the step. Handle.Fire describes
// how a state.WithEventData payload is handled, and when it's refused. Loading can
// return ErrEffectDispatch before the event fires, as Recover can. It is the
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
// returned to the caller. Between the append and any checkpoint it applies the step's
// domain effects; a failed dispatch fails the Handle with ErrHandleFailed.
//
// A state.WithEventData payload reaches the transition's Assign in its JSON-decoded
// form, live and on replay alike, so both see the same value: a struct arrives as a
// map and an integer as a float64, which loses precision above 2^53. An error arrives
// as an error with the same message, with any invalid UTF-8 replaced, though errors.Is
// no longer matches it. Keep its message, not the error, in context: a checkpoint turns
// an error into a map, and Recover fails on an error-typed field. A payload that can't take this form, an error whose Error panics (such as
// a typed nil), or
// exceeds WithMaxEventDataBytes, refuses the Fire with ErrEventDataEncoding before the
// kernel runs. The payload is read on the calling goroutine with no lock of its own, so
// don't mutate it concurrently with Fire.
func (h *Handle[S, E, C]) Fire(ctx context.Context, event E, opts ...state.FireOption) (state.FireResult[S], error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.refuse(); err != nil {
		return state.FireResult[S]{}, err
	}
	// Encode the WithEventData payload before the kernel runs, so a payload that
	// can't be recorded refuses the Fire rather than applying a step it can't record.
	payload, opts, err := encodeEventData(opts, h.runner.cfg.maxEventData)
	if err != nil {
		return state.FireResult[S]{}, fmt.Errorf("durable: recording event data for %q: %w", h.id, err)
	}
	res := h.inst.Fire(ctx, event, opts...)
	if res.Err != nil {
		return res, res.Err
	}

	// Absorb the step's driver effects into every host driver — timers, services, and
	// actors — so a transition that enters a state declaring any combination of them
	// arms each for the seam method that settles it next. The seams compose: a Fire
	// may enter a state that both invokes a service and spawns an actor and arms a
	// timer, and each driver must see its effects.
	h.absorbDrivers(ctx, res.Effects)

	step := h.nextStep
	rec := Record{Step: step, Event: []byte(res.Trace.EventPayload), EventData: payload.data, EventDataError: payload.errMsg, Entries: h.drainClock()}
	if err := h.commitStep(ctx, &rec, res.Effects); err != nil {
		return res, err
	}
	return res, nil
}

// Tick advances the Handle's delayed-transition scheduler: it fires every timer
// whose recorded deadline is at or before the recording clock's current time, in
// due order, re-firing the delayed events through the durable instance and
// recording the clock readings the tick consumed. A host calls it from its own
// timer loop (with a real clock) or a test calls it after advancing a fake clock.
// When a timer fired it records one tick barrier — carrying the consumed clock readings
// and the count of timers fired — so recovery re-derives the same timers at their
// recorded instants. A tick that fires nothing records nothing. It returns the
// FireResults of the timers it fired, in order. A timer whose transition fails is
// consumed and reported only in its FireResult's Err, not in Tick's error. Tick applies
// the step's domain effects: a failed dispatch, or a KindedEffect that won't marshal,
// fails the Handle.
func (h *Handle[S, E, C]) Tick(ctx context.Context) ([]state.FireResult[S], error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.refuse(); err != nil {
		return nil, err
	}
	// A context that ended in the lock queue must stop here: the scheduler would consume
	// a due timer whose Fire the kernel then refuses.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	results := h.sched.Tick(ctx)
	// A context that ended partway makes the kernel refuse a due timer's Fire after the
	// scheduler consumed it. Journaling that would commit on recovery what live never
	// applied, so record nothing: Recover re-arms the timer.
	if err := ctx.Err(); err != nil {
		for i := range results {
			if results[i].Err != nil {
				return results, h.fail(fmt.Errorf("durable: ticking %q: %w", h.id, err))
			}
		}
	}

	// A tick that fired nothing changes nothing, so it records nothing, and its clock
	// read goes with it: replay runs no tick for it, so it consumes no recorded reading.
	if len(results) == 0 {
		if h.recClock != nil {
			h.recClock.drain()
		}
		return results, nil
	}
	step := h.nextStep
	rec := Record{Step: step, Tick: true, TickSteps: len(results), Entries: h.drainClock()}
	// Reconcile the Handle's timer table with the tick: drop the timers it fired and
	// arm any a chained transition scheduled, so a checkpoint after the tick persists
	// exactly the still-pending deadlines.
	for i := range results {
		if now, ok := absorbRead(rec.Entries, i, len(results)); ok {
			h.armTimersFrom(results[i].Effects, now)
		}
	}
	dropFiredTimers(h.timers, h.sched)
	var effects []state.Effect
	for i := range results {
		effects = append(effects, results[i].Effects...)
	}
	return results, h.commitStep(ctx, &rec, effects)
}

// commitStep makes a step durable, in write-ahead order. It stamps the step's domain
// effects and carries them in rec, appends rec, advances the next step, dispatches the
// effects, and only then checkpoints, so a checkpoint never compacts an effect that
// hasn't landed: a crash between append and dispatch leaves it in the tail for recovery
// to apply, at-least-once and deduped by id.
//
// The kernel has already applied the step, so a failed encode or append fails the
// Handle. So does a failed dispatch: the effect stays in the tail, and Recover retries
// it. A failed checkpoint doesn't, because the Record is durable. This relies on the
// Append running inline, under the Handle's lock, before the step returns: an Append
// deferred off this goroutine would acknowledge a step whose outcome isn't known, and
// the Handle couldn't fail.
func (h *Handle[S, E, C]) commitStep(ctx context.Context, rec *Record, effects []state.Effect) error {
	des := dispatchableEffects(rec.Step, effects)
	envs, err := recordEffects(des)
	if err != nil {
		return h.fail(err)
	}
	rec.Effects = envs
	if err := h.appendStep(ctx, rec.Step, rec); err != nil {
		return h.fail(err)
	}
	h.nextStep = rec.followingStep()
	if err := h.dispatchEffects(ctx, des); err != nil {
		return h.fail(err)
	}
	return h.checkpointStep(ctx, rec.Step, rec)
}

// checkpointDue reports whether the checkpoint policy lands a checkpoint at step.
func (h *Handle[S, E, C]) checkpointDue(step int) bool {
	return h.runner.cfg.checkpointEvery > 0 && (step+1)%h.runner.cfg.checkpointEvery == 0
}

// appendStep marshals the due checkpoint snapshot into rec (so the Record carries it)
// and write-ahead appends rec, WITHOUT compacting the tail yet. Splitting the append
// from the compaction lets commitStep dispatch a step's domain
// effects after the durable append but before the checkpoint compacts that step away
// — so a crash between append and dispatch leaves the effect recoverable from the
// still-present tail rather than orphaned behind the compaction.
func (h *Handle[S, E, C]) appendStep(ctx context.Context, step int, rec *Record) error {
	if h.checkpointDue(step) {
		snap, err := state.MarshalSnapshot(h.inst.Snapshot())
		if err != nil {
			return fmt.Errorf("durable: marshaling checkpoint at step %d for %q: %w", step, h.id, err)
		}
		// Wrap the kernel snapshot with the absolute deadlines of the timers armed at
		// this checkpoint, so a recovery whose compacted tail no longer carries their
		// arming ScheduleAfter can re-arm them at their recorded instants.
		env, err := marshalCheckpoint(snap, h.timers, rec.followingStep())
		if err != nil {
			return fmt.Errorf("durable: marshaling checkpoint envelope at step %d for %q: %w", step, h.id, err)
		}
		rec.Snapshot = env
	}
	if _, err := h.runner.store.Append(ctx, h.id, *rec, WithFence(h.runner.cfg.fence)); err != nil {
		return fmt.Errorf("durable: recording step %d for %q: %w", step, h.id, err)
	}
	return nil
}

// checkpointStep compacts the journal tail through step when the checkpoint policy is
// due, using the snapshot appendStep already stamped into rec. It is called after a
// step's domain effects have been dispatched, so the compaction never discards a
// not-yet-dispatched effect.
func (h *Handle[S, E, C]) checkpointStep(ctx context.Context, step int, rec *Record) error {
	if !h.checkpointDue(step) {
		return nil
	}
	if err := h.runner.store.Checkpoint(ctx, h.id, rec.Snapshot, step, WithCheckpointFence(h.runner.cfg.fence)); err != nil {
		return fmt.Errorf("durable: checkpointing step %d for %q: %w", step, h.id, err)
	}
	return nil
}

// Recover reconstructs a durable instance purely from the Store: it loads the
// latest checkpoint Snapshot and the journal/effect tail after it, Restores the
// snapshot (firing nothing, no IO), and replays the tail's recorded driving
// events through the kernel to reach the instance's live state. The returned
// Handle continues recording subsequent Fires. Recover reports ErrInstanceNotFound
// for an instance that was never started, and the context's error if ctx ends. It
// applies any recorded effect not yet marked dispatched, so it returns ErrEffectDispatch
// and no Handle while the effect handler keeps failing.
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
	kernelSnap, persistedTimers, checkpointNext, err := unmarshalCheckpoint(snapBytes)
	if err != nil {
		return nil, fmt.Errorf("durable: unmarshaling checkpoint envelope for %q: %w", id, err)
	}

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
	// step) can be re-settled from its recorded outcome without re-running it.
	svcBuf := make([]state.JournalEntry, 0)
	svc := r.newServiceRunner(inst)
	if svc != nil {
		svc.Absorb(ctx, inst.StartEffects())
	}

	// Build the child-machine actor host driver, but DO NOT spawn the restored
	// configuration's actors yet: the recorded run's actors are reconstructed by
	// replaying their parent transitions directly (replayActor), not by re-running
	// behavior. Spawning here would re-instantiate an actor the recorded run already
	// settled. The live actors at the resume point are armed once, after replay,
	// from the final configuration's StartEffects.
	actorBuf := make([]state.JournalEntry, 0)
	actors := r.newActorSystem(inst)

	// armed mirrors the scheduler's pending timers with their recorded deadlines. It starts
	// from the timers the checkpoint carries, tail replay updates it, and it seeds the
	// Handle's table for the next checkpoint.
	armed := map[string]pendingTimer{}

	// Re-arm the timers pending at the checkpoint before the tail replays, because the
	// tail can fire them. Their deadlines are absolute, so any fixed instant works as the
	// arming clock read, and holding the replay clock on one keeps this read from
	// consuming a recorded reading.
	const armedAt = 0
	reEffects, reSeeded, reErr := reArmEffects(r.cfg.eventCodec, persistedTimers, time.Unix(armedAt, 0).UTC())
	if reErr != nil {
		return nil, fmt.Errorf("durable: re-arming pending timers for %q: %w", id, reErr)
	}
	if len(reEffects) > 0 {
		repClock.hold(time.Unix(armedAt, 0).UTC())
		sched.Absorb(ctx, reEffects)
		repClock.release()
	}
	for id, pt := range reSeeded {
		armed[id] = pt
	}

	reads := 0 // clock readings through the current Record
	settles := settleClock{clock: repClock}
	carry := false
	for i := range tail {
		// A canceled context can stop a replayed settle or tick with no error, leaving
		// state short of the journal, so check it before each Record.
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("durable: recovering %q: %w", id, err)
		}
		rec := &tail[i]
		reads += clockReadCount(rec)
		settles.next(rec)
		switch {
		case hasActorEntry(rec):
			// Re-fire each recorded parent transition the live delivery drove, in
			// recorded (fire) order, carrying the recorded actor done-data / error so
			// the kernel re-derives the identical onDone / onError (or message) parent
			// transition — running no actor behavior.
			for _, entry := range actorEntries(rec) {
				if err := replayActor(ctx, inst, r.cfg.eventCodec, entry); err != nil {
					return nil, fmt.Errorf("durable: at step %d for %q: %w", rec.Step, id, err)
				}
			}
		case hasServiceEntry(rec):
			// Re-settle each recorded invoked-service outcome through the same settle
			// seam the live run drove, in recorded (settle) order, so the kernel
			// re-fires the identical onDone / onError event with the identical data —
			// running no service. The settle absorbs its own follow-on StartService
			// effects, so a chained invoke arms its successor for the next entry.
			var settled []state.Effect
			for _, entry := range serviceEntries(rec) {
				effects, err := replayService(ctx, svc, entry)
				if err != nil {
					return nil, fmt.Errorf("durable: at step %d for %q: %w", rec.Step, id, err)
				}
				settled = append(settled, effects...)
			}
			// Arm the timers the settle scheduled, as the live RunService did.
			if hasTimerEffect(settled) {
				at, lost := settles.absorb(rec, reads, func() { sched.Absorb(ctx, settled) })
				mirrorArmed(armed, settled, at)
				carry = carry || lost
			}
			if err := r.dispatchReplayEffects(ctx, id, rec.Step, settled); err != nil {
				return nil, err
			}
		case rec.Tick && rec.TickSteps == 0:
			// Only an older journal holds a tick that fired nothing. Don't tick: a timer
			// re-armed from a checkpoint could be due by now, and the Record says otherwise.
		case rec.Tick:
			// Re-derive the timers the live tick fired: ticking the replay clock
			// returns the recorded readings, so the same deadlines come due and the
			// same timers fire, at their recorded instants. Mirror the tick into the
			// armed table — drop the timers it fired, arm any a chained transition
			// re-scheduled — so the deadline table tracks the still-pending set.
			repClock.skipTo(reads - tickReads(rec.TickSteps))
			tickRes := sched.Tick(ctx)
			for ri := range tickRes {
				if now, ok := absorbRead(rec.Entries, ri, rec.TickSteps); ok {
					mirrorArmed(armed, tickRes[ri].Effects, now)
				}
			}
			dropFiredTimers(armed, sched)
			var ticked []state.Effect
			for ri := range tickRes {
				ticked = append(ticked, tickRes[ri].Effects...)
			}
			if err := r.dispatchReplayEffects(ctx, id, rec.Step, ticked); err != nil {
				return nil, err
			}
		case len(rec.Event) == 0:
			continue // a checkpoint-only Record drives no event
		default:
			event, err := r.cfg.eventCodec.Decode(rec.Event)
			if err != nil {
				return nil, fmt.Errorf("durable: decoding recorded event at step %d for %q: %w", rec.Step, id, err)
			}
			fireOpts, err := replayFireOptions(rec)
			if err != nil {
				return nil, fmt.Errorf("durable: decoding recorded event data at step %d for %q: %w", rec.Step, id, err)
			}
			res := inst.Fire(ctx, event, fireOpts...)
			if res.Err != nil {
				return nil, fmt.Errorf("durable: replaying step %d for %q: %w", rec.Step, id, res.Err)
			}
			// Re-arm/cancel this step's timers exactly as the live Fire did, so the
			// scheduler's pending set is reconstructed with the recorded deadlines.
			// Mirror the same arming into the armed table, drawing the deadline from
			// the recorded clock read the Absorb consumed (captured at the recorded
			// step rather than the live wall clock).
			if hasTimerEffect(res.Effects) {
				repClock.skipTo(reads - 1)
				sched.Absorb(ctx, res.Effects)
				// The recorded read the Absorb consumed is the instant to derive each
				// deadline from, so the mirror uses recorded time, not the wall clock.
				mirrorArmed(armed, res.Effects, repClock.lastRead())
			}
			// Arm this step's services so a service it entered is in flight for the
			// recorded settlement that follows, mirroring the live Fire path.
			if svc != nil {
				svc.Absorb(ctx, res.Effects)
			}
			// Re-dispatch this step's domain effects, deduped by the Store's
			// dispatched set: an effect already marked (it landed before the crash)
			// is skipped; one recorded-but-un-marked (the crash fell between append
			// and dispatch) is applied now — exactly-once across the crash boundary.
			if err := r.dispatchReplayEffects(ctx, id, rec.Step, res.Effects); err != nil {
				return nil, err
			}
		}
	}

	// Drain any clock readings the replay re-recorded after the cursor exhausted
	// (none in the common case where replay consumes exactly the recorded
	// readings): they belong to no new step and must not leak into the next Fire. The
	// exception is a read an older settle lost: the next step's Record carries it, so a
	// later replay arms that settle's timers at the same instant. Skip the cursor to the
	// end, so no reading left in the tail reaches the live clock.
	repClock.skipTo(reads)
	var carried []state.JournalEntry
	if carry {
		carried = append(carried, buf...)
	}
	buf = buf[:0]

	// Arm the live actors at the resume point from the final configuration's
	// StartEffects: this spawns exactly the actors running where replay left the
	// parent (for example the next child in a chain, after the prior one settled),
	// so a subsequent DeliverToActor finds them — without re-instantiating an actor
	// the recorded run already settled, which replay reconstructed by re-firing the
	// parent rather than re-running behavior.
	if actors != nil {
		actors.Absorb(ctx, inst.StartEffects())
	}

	// The next step ordinal continues past the highest assigned step. Step ordinals
	// are assigned monotonically: each Record consumes one ordinal regardless of how
	// many parent Traces it produced (an actor delivery can drive several), except a
	// scheduler-tick barrier, which consumed one ordinal per timer it fired plus the
	// barrier's own. The next ordinal is therefore the last tail Record's Step plus
	// its own span. When the tail is empty (a fresh checkpoint compacted every
	// Record), the checkpoint carries the next ordinal. A Start baseline carries none and
	// has no Traces, so its count of zero is right. A checkpoint from before this field
	// also falls back to the Trace count, which can be off (see
	// checkpointEnvelope.NextStep).
	next := checkpointNext
	if next == 0 {
		next = len(inst.History())
	}
	if n := len(tail); n > 0 {
		next = tail[n-1].followingStep()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("durable: recovering %q: %w", id, err)
	}
	return &Handle[S, E, C]{
		runner:   r,
		id:       id,
		inst:     inst,
		nextStep: next,
		sched:    sched,
		recClock: recClock,
		carried:  carried,
		timers:   armed,
		svc:      svc,
		svcBuf:   &svcBuf,
		actors:   actors,
		actorBuf: &actorBuf,
	}, nil
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
func encodeEventData(opts []state.FireOption, maxBytes int) (eventPayload, []state.FireOption, error) {
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
	decoded, err := payload.fireOption()
	if err != nil {
		return payload, nil, fmt.Errorf("%w: %w", ErrEventDataEncoding, err)
	}
	live := make([]state.FireOption, 0, len(opts)+1)
	return payload, append(append(live, opts...), decoded), nil
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

// fireOption returns the option carrying the payload as a Fire sees it: live, and when
// a replay re-Fires the step.
func (p eventPayload) fireOption() (state.FireOption, error) {
	if p.errMsg != nil {
		return state.WithEventData(errors.New(*p.errMsg)), nil
	}
	var data any
	if err := json.Unmarshal(p.data, &data); err != nil {
		return nil, err
	}
	return state.WithEventData(data), nil
}

// replayFireOptions returns the options that re-Fire rec's event with the payload
// the live Fire carried, or none when it carried none.
func replayFireOptions(rec *Record) ([]state.FireOption, error) {
	if len(rec.EventData) == 0 && rec.EventDataError == nil {
		return nil, nil
	}
	opt, err := eventPayload{data: rec.EventData, errMsg: rec.EventDataError}.fireOption()
	if err != nil {
		return nil, err
	}
	return []state.FireOption{opt}, nil
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
