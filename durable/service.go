package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// This file implements the invoked-service record/replay seam. An invoked service
// (`invoke`) is the second nondeterministic source the durable runtime records:
// its result is whatever the host's ServiceFn computes — an external fetch, a
// query, a clock- or randomness-dependent value — so it is NOT a pure function of
// (configuration, context, event payload, machine definition) and MUST be recorded
// to reproduce a run.
//
// # Run exactly once, replay the result
//
// A durable service runs exactly once, on the live path, and its result is
// recorded; on recovery the recorded result is replayed back through the same
// settle seam, and the service is NEVER re-invoked. This is the standard
// durable-execution guarantee: the external call (with its side effects and its
// nondeterministic output) happens once, and recovery is a pure replay against the
// kernel rather than a re-execution of the host's services.
//
// The Runner wraps the kernel's reusable host driver, state.ServiceRunner, which
// turns StartService / StopService effects into real ServiceFn executions and
// re-fires each result through the invocation's onDone / onError event. On the live
// path RunService delegates to ServiceRunner.Run (resolve + run + settle), reads
// the raw outcome the service produced, and journals it as a JournalServiceResult
// correlated by the invocation id. On recovery the recorded outcomes are replayed
// in order through ServiceRunner.SettleDone / SettleError — the same settle methods
// the live run drove — so the kernel re-fires the identical onDone / onError event
// with the identical data and re-derives byte-identical context, having run no
// service.
//
// # Correlation and ordering
//
// Each recorded entry's CorrelationID is the invocation id (state.InvokeID), the
// same id the kernel stamps on the StartService effect and the host settles by, so
// replay matches a recorded result to the exact invocation it stands in for.
// Entries are recorded in settle order and replayed in that order, interleaved with
// the clock reads and externally fired steps of the surrounding Records, so a
// chain of invokes — and a mix of services, timers, and external events in one
// instance — reproduces deterministically.

// serviceResultPayload is the wire form of a recorded service outcome: a service
// records EITHER a successful result (Result, the JSON of the ServiceFn's return)
// OR an error (Error, the failure's message). Replay reconstructs the outcome from
// whichever is present and settles the invocation through the matching seam.
type serviceResultPayload struct {
	// Result is the JSON-encoded successful return of the ServiceFn, present for a
	// SettleDone outcome. It is decoded to a Go value and re-fired through onDone so
	// the transition's Assign reads it from AssignCtx.Event exactly as on the live
	// run.
	Result json.RawMessage `json:"result,omitempty"`
	// Error is the failure message for a SettleError outcome. It is reconstructed
	// into an error and re-fired through onError so the transition's Assign reads it
	// from AssignCtx.Event.
	Error *string `json:"error,omitempty"`
}

// drainServices returns and clears the service outcomes recorded since the last
// drain, so each persisted step carries exactly the settlements that resolved
// during it, in settle order.
func (h *Handle[S, E, C]) drainServices() []state.JournalEntry {
	if h.svcBuf == nil || len(*h.svcBuf) == 0 {
		return nil
	}
	out := make([]state.JournalEntry, len(*h.svcBuf))
	copy(out, *h.svcBuf)
	*h.svcBuf = (*h.svcBuf)[:0]
	return out
}

// RunService runs the in-flight invoked service identified by id exactly once,
// records its outcome, and settles it — firing the invocation's onDone (on
// success) or onError (on failure) event through the durable instance and
// recording the produced step. The service is resolved and executed against the
// registry supplied with WithServiceRegistry; its raw result or error is journaled
// as a JournalServiceResult correlated by id so recovery replays it without
// re-invoking the service.
//
// It returns the routed FireResult and true, or the zero result and false when id
// names no in-flight service (already settled, stopped, or never started). A
// persistence failure is returned as the error; the kernel transition itself does
// not fail (a settled service always routes onDone or onError).
func (h *Handle[S, E, C]) RunService(ctx context.Context, id string) (state.FireResult[S], bool, error) {
	if h.svc == nil {
		return state.FireResult[S]{}, false, fmt.Errorf("durable: no service registry wired for %q (supply WithServiceRegistry)", h.id)
	}
	if !h.svc.HasPending(id) {
		return state.FireResult[S]{}, false, nil
	}

	res, ok := h.svc.Run(ctx, id)
	if !ok {
		return state.FireResult[S]{}, false, nil
	}

	entry, err := recordServiceOutcome(id, h.svc.LastResult, h.svc.LastError())
	if err != nil {
		return res, true, fmt.Errorf("durable: recording service %q result for %q: %w", id, h.id, err)
	}
	if h.svcBuf != nil {
		*h.svcBuf = append(*h.svcBuf, entry)
	}

	step := h.nextStep
	rec := Record{Step: step, Entries: h.drainServices()}
	if err := h.persistStep(ctx, step, &rec); err != nil {
		return res, true, err
	}
	h.nextStep++
	return res, true, nil
}

// recordServiceOutcome builds the JournalServiceResult entry for a just-settled
// service. It reads the success result through lastResult (valid only during the
// settle's synchronous window) and the error through lastErr, encoding whichever
// the service produced.
func recordServiceOutcome(id string, lastResult func() (any, bool), lastErr error) (state.JournalEntry, error) {
	var sp serviceResultPayload
	if lastErr != nil {
		msg := lastErr.Error()
		sp.Error = &msg
	} else if result, ok := lastResult(); ok {
		raw, err := json.Marshal(result)
		if err != nil {
			return state.JournalEntry{}, fmt.Errorf("marshaling service result: %w", err)
		}
		sp.Result = raw
	}
	payload, err := json.Marshal(sp)
	if err != nil {
		return state.JournalEntry{}, fmt.Errorf("marshaling service outcome: %w", err)
	}
	return state.JournalEntry{
		Kind:          state.JournalServiceResult,
		CorrelationID: id,
		Payload:       payload,
	}, nil
}

// replayService re-settles one recorded invoked-service outcome through the
// instance's ServiceRunner, in place of re-invoking the real service: it decodes
// the recorded payload and calls SettleDone (with the recorded result) or
// SettleError (with the reconstructed error), so the kernel re-fires the same
// onDone / onError event with the same data and re-derives identical context. It
// reports an error only when the recorded entry cannot be decoded or names no
// in-flight service (a corrupt or mis-ordered journal).
func replayService[S comparable, E comparable, C any](ctx context.Context, svc *state.ServiceRunner[S, E, C], entry state.JournalEntry) error {
	if svc == nil {
		return errors.New("durable: replaying a service result requires a service registry (supply WithServiceRegistry)")
	}
	var sp serviceResultPayload
	if err := json.Unmarshal(entry.Payload, &sp); err != nil {
		return fmt.Errorf("durable: decoding recorded service %q outcome: %w", entry.CorrelationID, err)
	}
	if sp.Error != nil {
		if _, ok := svc.SettleError(ctx, entry.CorrelationID, errors.New(*sp.Error)); !ok {
			return fmt.Errorf("durable: replaying service %q: no in-flight service to settle", entry.CorrelationID)
		}
		return nil
	}
	var result any
	if len(sp.Result) > 0 {
		if err := json.Unmarshal(sp.Result, &result); err != nil {
			return fmt.Errorf("durable: decoding recorded service %q result: %w", entry.CorrelationID, err)
		}
	}
	if _, ok := svc.SettleDone(ctx, entry.CorrelationID, result); !ok {
		return fmt.Errorf("durable: replaying service %q: no in-flight service to settle", entry.CorrelationID)
	}
	return nil
}

// serviceEntries returns the recorded service outcomes carried by a single
// Record, in recorded (settle) order, so replay re-settles them in the order the
// live run produced them.
func serviceEntries(rec *Record) []state.JournalEntry {
	var out []state.JournalEntry
	for _, e := range rec.Entries {
		if e.Kind == state.JournalServiceResult {
			out = append(out, e)
		}
	}
	return out
}

// hasServiceEntry reports whether a Record carries any recorded service outcome,
// so replay can route it to the service settle path rather than re-firing an event.
func hasServiceEntry(rec *Record) bool {
	for _, e := range rec.Entries {
		if e.Kind == state.JournalServiceResult {
			return true
		}
	}
	return false
}
