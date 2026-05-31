package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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
// Runner must record to reproduce a run is therefore the driving event: each
// Fire appends a Record{Step, Event} where Step is the produced Trace ordinal and
// Event is the kernel's structured Trace.EventPayload. Periodically — governed by
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
// Invoked services are the second recorded seam: a service runs exactly once on
// the live path (Handle.RunService, against the registry supplied with
// WithServiceRegistry) and its result is journaled as a JournalServiceResult; on
// recovery the recorded result is replayed back through the kernel's settle seam,
// so the service is never re-invoked and the same onDone / onError event re-fires
// with the same data.
//
// Child-machine actors are the third recorded seam: an actor's behavior runs
// exactly once on the live path (Handle.DeliverToActor, against the palette
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
type Handle[S comparable, E comparable, C any] struct {
	runner   *Runner[S, E, C]
	id       InstanceID
	inst     *state.Instance[S, E, C]
	nextStep int

	sched    *state.Scheduler[S, E, C]
	clockBuf *[]state.JournalEntry // recording-clock accumulator, flushed per step

	svc    *state.ServiceRunner[S, E, C] // host driver for invoked services, nil when none wired
	svcBuf *[]state.JournalEntry         // recording service-result accumulator, flushed per step

	actors   *state.ActorSystem[S, E, C] // host driver for child-machine actors, nil when none wired
	actorBuf *[]state.JournalEntry       // recording actor-transition accumulator, flushed per step
}

// drainClock returns and clears the clock readings accumulated since the last
// drain, so each recorded step carries exactly the readings consumed during it.
func (h *Handle[S, E, C]) drainClock() []state.JournalEntry {
	if h.clockBuf == nil || len(*h.clockBuf) == 0 {
		return nil
	}
	out := make([]state.JournalEntry, len(*h.clockBuf))
	copy(out, *h.clockBuf)
	*h.clockBuf = (*h.clockBuf)[:0]
	return out
}

// Instance returns the underlying kernel Instance the Handle wraps, for reads
// such as Configuration, Snapshot, or Current. Drive it through the Handle's Fire
// (or the Runner's) so steps continue to be recorded; firing the bare Instance
// bypasses durability.
func (h *Handle[S, E, C]) Instance() *state.Instance[S, E, C] { return h.inst }

// ID returns the InstanceID the Handle records under.
func (h *Handle[S, E, C]) ID() InstanceID { return h.id }

// Start creates and registers a fresh durable instance: it casts the machine on
// input, persists a baseline checkpoint so the instance is loadable from the
// first step, and returns a live Handle. Cast options (for example
// state.WithInitialState) configure the initial configuration. Starting an
// InstanceID that already exists in the Store reports ErrInstanceExists rather
// than clobbering its recorded baseline.
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
	castOpts := append([]state.CastOption[S]{state.WithClock[S](recClock)}, opts...)
	inst := r.machine.Cast(input, castOpts...)

	// Persist a baseline checkpoint at baselineStep (below the first fired step)
	// so the instance is reconstructable from the Store before any event is fired.
	// The baseline is the cast snapshot; the tail then accumulates fired steps on
	// top of it.
	snap, err := state.MarshalSnapshot(inst.Snapshot())
	if err != nil {
		return nil, fmt.Errorf("durable: marshaling start snapshot for %q: %w", id, err)
	}
	if err := r.store.Checkpoint(ctx, id, snap, baselineStep); err != nil {
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
		sched:    state.NewScheduler(inst),
		clockBuf: &buf,
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
	return state.NewServiceRunner(inst, r.cfg.serviceReg)
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
// replaying it from the Store first, then recording the step. It is the
// stateless entry point (no Handle required); for a hot path that fires many
// events in sequence, hold a Handle from Start or Recover and use Handle.Fire to
// avoid reloading between steps.
func (r *Runner[S, E, C]) Fire(ctx context.Context, id InstanceID, event E, opts ...state.FireOption) (state.FireResult[S], error) {
	h, err := r.recover(ctx, id)
	if err != nil {
		return state.FireResult[S]{}, err
	}
	return h.Fire(ctx, event, opts...)
}

// Fire drives one event through the Handle's live instance and records the step:
// it Fires the kernel, appends a Record carrying the driving event at the
// produced Trace ordinal (write-ahead, before returning), and — when the
// checkpoint policy is due — persists a full Snapshot and compacts the tail. A
// kernel transition error is recorded as a no-op (no step was produced) and
// returned to the caller.
func (h *Handle[S, E, C]) Fire(ctx context.Context, event E, opts ...state.FireOption) (state.FireResult[S], error) {
	res := h.inst.Fire(ctx, event, opts...)
	if res.Err != nil {
		return res, res.Err
	}

	// Arm or cancel the step's delayed (`after`) timers, which reads the recording
	// clock; the readings land in clockBuf and are flushed into this step's Record.
	// Absorb only when the step actually scheduled or canceled a timer: the kernel
	// Scheduler reads the clock unconditionally, so skipping the call for an
	// effect-free step keeps a purely event-driven machine free of clock reads.
	if hasTimerEffect(res.Effects) {
		h.sched.Absorb(ctx, res.Effects)
	}

	// Arm or stop this step's invoked services so a service the step entered is in
	// flight for a subsequent RunService. The kernel emits StartService / StopService
	// as pure data; the host driver turns them into runnable services.
	if h.svc != nil {
		h.svc.Absorb(ctx, res.Effects)
	}

	step := h.nextStep
	// Stamp this step's domain effects with their deterministic ids and carry them
	// in the Record so the write-ahead append durably names every effect that must
	// be dispatched before any dispatch happens.
	des := dispatchableEffects(step, res.Effects)
	effEnvs, err := recordEffects(des)
	if err != nil {
		return res, err
	}
	rec := Record{Step: step, Event: []byte(res.Trace.EventPayload), Entries: h.drainClock(), Effects: effEnvs}
	if err := h.persistStep(ctx, step, &rec); err != nil {
		return res, err
	}
	h.nextStep++
	// Dispatch AFTER the append: a crash in this window leaves the effect recorded
	// but un-marked, so recovery redispatches it (at-least-once), deduped by id to
	// exactly-once once it lands.
	if err := h.dispatchEffects(ctx, des); err != nil {
		return res, err
	}
	return res, nil
}

// Tick advances the Handle's delayed-transition scheduler: it fires every timer
// whose recorded deadline is at or before the recording clock's current time, in
// due order, re-firing the delayed events through the durable instance and
// recording the clock readings the tick consumed. A host calls it from its own
// timer loop (with a real clock) or a test calls it after advancing a fake clock.
// It records one tick barrier — carrying the consumed clock readings and the
// count of timers fired — so recovery re-derives the same timers at their
// recorded instants. It returns the FireResults of the timers it fired, in order.
func (h *Handle[S, E, C]) Tick(ctx context.Context) ([]state.FireResult[S], error) {
	results := h.sched.Tick(ctx)

	step := h.nextStep
	rec := Record{Step: step, Tick: true, TickSteps: len(results), Entries: h.drainClock()}
	// A tick that read no clock and fired nothing records nothing: there is no
	// nondeterminism to journal and no step was produced.
	if len(rec.Entries) == 0 && rec.TickSteps == 0 {
		return results, nil
	}
	if err := h.persistStep(ctx, step, &rec); err != nil {
		return results, err
	}
	h.nextStep += len(results) + 1
	return results, nil
}

// persistStep checkpoints if the policy is due, then write-ahead appends rec
// before the step is acknowledged to the caller. The checkpoint is taken at the
// barrier's own step so a Load after it returns this Snapshot plus the later tail.
func (h *Handle[S, E, C]) persistStep(ctx context.Context, step int, rec *Record) error {
	due := h.runner.cfg.checkpointEvery > 0 && (step+1)%h.runner.cfg.checkpointEvery == 0
	if due {
		snap, err := state.MarshalSnapshot(h.inst.Snapshot())
		if err != nil {
			return fmt.Errorf("durable: marshaling checkpoint at step %d for %q: %w", step, h.id, err)
		}
		rec.Snapshot = snap
	}
	if _, err := h.runner.store.Append(ctx, h.id, *rec); err != nil {
		return fmt.Errorf("durable: recording step %d for %q: %w", step, h.id, err)
	}
	if due {
		if err := h.runner.store.Checkpoint(ctx, h.id, rec.Snapshot, step); err != nil {
			return fmt.Errorf("durable: checkpointing step %d for %q: %w", step, h.id, err)
		}
	}
	return nil
}

// Recover reconstructs a durable instance purely from the Store: it loads the
// latest checkpoint Snapshot and the journal/effect tail after it, Restores the
// snapshot (firing nothing, no IO), and replays the tail's recorded driving
// events through the kernel to reach the instance's live state. The returned
// Handle continues recording subsequent Fires. Recover reports ErrInstanceNotFound
// for an instance that was never started.
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

	snap, err := state.UnmarshalSnapshot[S, E, C](snapBytes)
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

	inst, err := r.machine.Restore(snap, state.WithRestoreClock[S](repClock))
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

	for i := range tail {
		rec := &tail[i]
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
			for _, entry := range serviceEntries(rec) {
				if err := replayService(ctx, svc, entry); err != nil {
					return nil, fmt.Errorf("durable: at step %d for %q: %w", rec.Step, id, err)
				}
			}
		case rec.Tick:
			// Re-derive the timers the live tick fired: ticking the replay clock
			// returns the recorded readings, so the same deadlines come due and the
			// same timers fire, at their recorded instants.
			sched.Tick(ctx)
		case len(rec.Event) == 0:
			continue // a checkpoint-only Record drives no event
		default:
			event, err := r.cfg.eventCodec.Decode(rec.Event)
			if err != nil {
				return nil, fmt.Errorf("durable: decoding recorded event at step %d for %q: %w", rec.Step, id, err)
			}
			res := inst.Fire(ctx, event)
			if res.Err != nil {
				return nil, fmt.Errorf("durable: replaying step %d for %q: %w", rec.Step, id, res.Err)
			}
			// Re-arm/cancel this step's timers exactly as the live Fire did, so the
			// scheduler's pending set is reconstructed with the recorded deadlines.
			if hasTimerEffect(res.Effects) {
				sched.Absorb(ctx, res.Effects)
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
	// readings): they belong to no new step and must not leak into the next Fire.
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
	// Record), no actor or tick step survives in it, so the restored Trace count —
	// one per pre-checkpoint step — gives the next ordinal directly.
	next := len(inst.History())
	if n := len(tail); n > 0 {
		last := &tail[n-1]
		next = last.Step + 1
		if last.Tick {
			next = last.Step + last.TickSteps + 1
		}
	}
	return &Handle[S, E, C]{
		runner:   r,
		id:       id,
		inst:     inst,
		nextStep: next,
		sched:    sched,
		clockBuf: &buf,
		svc:      svc,
		svcBuf:   &svcBuf,
		actors:   actors,
		actorBuf: &actorBuf,
	}, nil
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
