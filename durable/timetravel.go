package durable

import (
	"context"
	"errors"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// This file implements the read-only time-travel reader: reconstructing an
// instance's state AS OF any recorded step, without mutating the live instance or
// the Store and without re-running any service, actor, or domain effect. It is the
// audit counterpart to Recover — where Recover reconstructs the live tip and
// continues recording, StateAt reconstructs a historical point and stops, dispatching
// nothing.
//
// # How the reconstruction stays a pure, read-only replay
//
// The reader restores the start baseline snapshot (the cast state recorded at
// BaselineStep before any event fired) and replays the recorded driving events,
// service settlements, actor transitions, and clock reads forward UP TO the target
// step — exactly the replay Recover performs, bounded. Because every
// nondeterministic result is read back verbatim from the journal (the same record /
// replay model the live recovery uses), no service is re-invoked, no actor behavior
// is re-instantiated, and no clock is read against the wall clock. The reader also
// never dispatches a domain effect: it builds no effect handler and calls no
// DispatchStore, so a side effect a step emitted is observed in the reconstructed
// state's history but never re-applied. The Store is consulted only through reads
// (History / Load), so neither the recorded log nor the dispatched set is touched.
//
// # Why a history seam
//
// Reconstructing an arbitrary earlier step needs every Record from the baseline
// through that step. A live Store compacts the journal at each checkpoint to bound
// growth, so the latest checkpoint's tail alone cannot reach a step that was
// compacted away. A Store opts into time-travel by also implementing HistoryStore,
// retaining the baseline snapshot and the full ordered Record log (the in-tree
// MemStore does so under WithHistory). Against a Store that does not — or one that
// never compacted — the reader falls back to the latest checkpoint plus its tail,
// which still covers every step at or after that checkpoint.

// ErrStepOutOfRange is reported by StateAt when the requested step is below the
// start baseline or beyond the instance's last recorded step. Callers match it with
// errors.Is.
var ErrStepOutOfRange = errors.New("crucible/durable: time-travel step out of range")

// BaselineStep is the step that addresses an instance's start baseline — its state
// immediately after Start, before any event fired. StateAt(ctx, m, st, id,
// BaselineStep) reconstructs the freshly cast instance.
const BaselineStep = baselineStep

// HistoryStore is the optional time-travel seam: a Store that retains an instance's
// start baseline snapshot and its full ordered Record log, so a reader can
// reconstruct the state as of any recorded step even after checkpoint compaction
// discarded that step from the live tail. It is additive, like DispatchStore — the
// core Store interface is unchanged; a backend opts in by implementing this method,
// and StateAt uses it when present, falling back to Load otherwise. The in-tree
// MemStore implements it when constructed WithHistory.
type HistoryStore interface {
	// History returns the instance's start baseline snapshot bytes (the BaselineStep
	// checkpoint, nil if the instance was never started through a Runner) together
	// with the full ordered Record log — every Record ever appended, in Step order,
	// including those a checkpoint later compacted out of the live tail. It reports
	// ErrInstanceNotFound for an instance that was never written.
	History(ctx context.Context, id InstanceID) (baseline []byte, records []Record, err error)
}

// TimeTravelView is the read-only result of a StateAt reconstruction: the kernel
// Instance restored and replayed to the requested step, and the step it was
// reconstructed at. The Instance is detached — it shares no state with any live
// Handle and is safe to read (Snapshot, Current, Configuration) but not to drive
// (firing it records nothing and is not durable). Obtain one from StateAt.
type TimeTravelView[S comparable, E comparable, C any] struct {
	inst *state.Instance[S, E, C]
	step int
}

// Instance returns the reconstructed kernel Instance, for reads such as Snapshot,
// Current, or Configuration. It is a detached read-only view; drive a live instance
// through a Handle from Recover instead.
func (v *TimeTravelView[S, E, C]) Instance() *state.Instance[S, E, C] { return v.inst }

// Snapshot returns the reconstructed instance's kernel Snapshot at the target step,
// a convenience over Instance().Snapshot() for the common audit read.
func (v *TimeTravelView[S, E, C]) Snapshot() state.Snapshot[S, E, C] { return v.inst.Snapshot() }

// Step returns the recorded step the view was reconstructed at (BaselineStep for the
// start baseline).
func (v *TimeTravelView[S, E, C]) Step() int { return v.step }

// StateAt reconstructs a durable instance's state AS OF the recorded step, read-only:
// it restores the start baseline and replays the recorded driving events, service
// settlements, actor transitions, and clock reads forward up to step, returning a
// detached view of the instance at that point. It runs no service, re-instantiates
// no actor, reads no wall clock, and dispatches no domain effect — every result is a
// pure replay of recorded values — and it mutates neither the live instance nor the
// Store. The same functional options a Runner takes are accepted (the event codec,
// the service registry and actor palette needed to settle recorded results, the
// clock); a supplied effect handler is intentionally never invoked, since a
// historical read applies no side effect.
//
// step must be in [BaselineStep, lastRecordedStep]; a step outside that range
// reports ErrStepOutOfRange, and an unknown instance reports ErrInstanceNotFound.
func StateAt[S comparable, E comparable, C any](
	ctx context.Context,
	m *state.Machine[S, E, C],
	st Store,
	id InstanceID,
	step int,
	opts ...Option[S, E, C],
) (*TimeTravelView[S, E, C], error) {
	cfg := resolveRunner(opts...)

	baseline, records, err := loadHistory(ctx, st, id)
	if err != nil {
		return nil, err
	}
	if baseline == nil {
		return nil, fmt.Errorf("durable: instance %q has no baseline to reconstruct", id)
	}

	last := lastStep(records)
	if step < BaselineStep || step > last {
		return nil, fmt.Errorf("%w: step %d for %q (recorded range [%d, %d])",
			ErrStepOutOfRange, step, id, BaselineStep, last)
	}

	// Build the bounded record window and a replay clock over exactly its recorded
	// readings BEFORE restoring, so the restored instance's scheduler reads recorded
	// instants from the start rather than the wall clock.
	bounded := boundRecords(records, step)
	repClock := newReplayClock(clockReadings(bounded), cfg.clock)

	inst, err := restoreBaseline(m, baseline, repClock)
	if err != nil {
		return nil, fmt.Errorf("durable: reconstructing %q at step %d: %w", id, step, err)
	}

	if err := replayThrough(ctx, inst, &cfg, bounded); err != nil {
		return nil, fmt.Errorf("durable: reconstructing %q at step %d: %w", id, step, err)
	}
	return &TimeTravelView[S, E, C]{inst: inst, step: step}, nil
}

// Steps enumerates the recorded step ordinals of an instance, in order, so a caller
// can drive StateAt across the run (for example to render an audit timeline). It
// reports only externally fired and service/actor steps — the ordinals a StateAt
// target may name — and is read-only. An unknown instance reports
// ErrInstanceNotFound.
func Steps(ctx context.Context, st Store, id InstanceID) ([]int, error) {
	_, records, err := loadHistory(ctx, st, id)
	if err != nil {
		return nil, err
	}
	out := make([]int, 0, len(records))
	for i := range records {
		out = append(out, records[i].Step)
	}
	return out, nil
}

// loadHistory returns an instance's baseline snapshot and full ordered Record log,
// using the HistoryStore seam when the Store implements it (so a compacted step is
// still reachable) and falling back to Load's latest-checkpoint-plus-tail otherwise.
func loadHistory(ctx context.Context, st Store, id InstanceID) ([]byte, []Record, error) {
	if hs, ok := st.(HistoryStore); ok {
		baseline, records, err := hs.History(ctx, id)
		if err != nil {
			return nil, nil, fmt.Errorf("durable: loading history for %q: %w", id, err)
		}
		return baseline, records, nil
	}
	snap, tail, err := st.Load(ctx, id)
	if err != nil {
		return nil, nil, fmt.Errorf("durable: loading instance %q: %w", id, err)
	}
	return snap, tail, nil
}

// lastStep returns the highest step ordinal a Record log addresses, accounting for a
// tick barrier spanning several timer ordinals, or BaselineStep when the log is
// empty (only the baseline was recorded).
func lastStep(records []Record) int {
	if len(records) == 0 {
		return BaselineStep
	}
	last := &records[len(records)-1]
	if last.Tick {
		return last.Step + last.TickSteps
	}
	return last.Step
}

// restoreBaseline unwraps the baseline checkpoint envelope and restores a detached
// kernel instance from it under the bounded replay clock, so the reconstruction
// reads recorded instants rather than the wall clock. The baseline carries no armed
// timers (it is the pre-event cast), so its envelope's timer table is empty and only
// the snapshot matters here.
func restoreBaseline[S comparable, E comparable, C any](
	m *state.Machine[S, E, C],
	baseline []byte,
	repClock state.Clock,
) (*state.Instance[S, E, C], error) {
	kernelSnap, _, err := unmarshalCheckpoint(baseline)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling baseline envelope: %w", err)
	}
	snap, err := state.UnmarshalSnapshot[S, E, C](kernelSnap)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling baseline snapshot: %w", err)
	}
	inst, err := m.Restore(snap, state.WithRestoreClock[S](repClock))
	if err != nil {
		return nil, fmt.Errorf("restoring baseline snapshot: %w", err)
	}
	return inst, nil
}

// replayThrough replays the bounded record window forward into inst, reproducing the
// live driver sequence (service settle, actor re-fire, scheduler tick, or external
// Fire) for each Record — purely from recorded values, dispatching no domain effect.
// It is the bounded, read-only analog of Recover's replay loop: it builds its own
// service host driver (to settle recorded outcomes) and ticks a scheduler over the
// instance's recorded-instant clock, consuming exactly the records the caller bounded
// to the target step.
func replayThrough[S comparable, E comparable, C any](
	ctx context.Context,
	inst *state.Instance[S, E, C],
	cfg *runnerConfig[S, E, C],
	bounded []Record,
) error {
	sched := state.NewScheduler(inst)

	svc := newReplayServiceRunner(inst, cfg)
	if svc != nil {
		svc.Absorb(ctx, inst.StartEffects())
	}

	for i := range bounded {
		rec := &bounded[i]
		switch {
		case hasActorEntry(rec):
			for _, entry := range actorEntries(rec) {
				if err := replayActor(ctx, inst, cfg.eventCodec, entry); err != nil {
					return fmt.Errorf("at step %d: %w", rec.Step, err)
				}
			}
		case hasServiceEntry(rec):
			for _, entry := range serviceEntries(rec) {
				if err := replayService(ctx, svc, entry); err != nil {
					return fmt.Errorf("at step %d: %w", rec.Step, err)
				}
			}
		case rec.Tick:
			sched.Tick(ctx)
		case len(rec.Event) == 0:
			continue // a checkpoint-only Record drives no event
		default:
			event, err := cfg.eventCodec.Decode(rec.Event)
			if err != nil {
				return fmt.Errorf("decoding recorded event at step %d: %w", rec.Step, err)
			}
			res := inst.Fire(ctx, event)
			if res.Err != nil {
				return fmt.Errorf("replaying step %d: %w", rec.Step, res.Err)
			}
			if hasTimerEffect(res.Effects) {
				sched.Absorb(ctx, res.Effects)
			}
			if svc != nil {
				svc.Absorb(ctx, res.Effects)
			}
			// A domain effect is intentionally NOT dispatched here: a time-travel read
			// applies no side effect.
		}
	}
	return nil
}

// boundRecords returns the prefix of records whose first ordinal falls at or below
// target, the slice the bounded replay consumes. A Record's first ordinal is its
// Step (a tick barrier fires its timers at Step..Step+TickSteps-1, so it too is
// relevant whenever its Step is at or below the target).
func boundRecords(records []Record, target int) []Record {
	end := 0
	for i := range records {
		if records[i].Step > target {
			break
		}
		end = i + 1
	}
	return records[:end]
}

// newReplayServiceRunner builds the kernel service host driver for the read-only
// replay, or nil when no registry was wired. It is the reader's analog of the
// Runner's newServiceRunner, kept separate so the reader pulls in no live-path state.
func newReplayServiceRunner[S comparable, E comparable, C any](
	inst *state.Instance[S, E, C],
	cfg *runnerConfig[S, E, C],
) *state.ServiceRunner[S, E, C] {
	if cfg.serviceReg == nil {
		return nil
	}
	return state.NewServiceRunner(inst, cfg.serviceReg)
}
