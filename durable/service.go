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
// # Run once per recorded result, replay the result
//
// A durable service runs on the live path and its result is recorded; on recovery the
// recorded result is replayed back through the same settle seam, and that service is
// NEVER re-invoked. This is the standard durable-execution guarantee: recovery is a pure
// replay against the kernel rather than a re-execution of the host's services. A result
// that wasn't recorded, after a context canceled during the service, a failed Append or a
// crash, has nothing to replay, so it stays in flight, runs again when the host calls
// RunService, and must tolerate a second run.
//
// The Runner wraps the kernel's reusable host driver, state.ServiceRunner, which
// turns StartService / StopService effects into real ServiceFn executions and
// re-fires each result through the invocation's onDone / onError event. On the live
// path RunService delegates to ServiceRunner.Tick (resolve + run + settle), reads
// the outcome in its JSON-decoded form, and journals it as a JournalServiceResult
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
	// run, which settles with the same decoded form (see replayableOutcome).
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

// RunService runs the in-flight invoked service identified by id,
// records its outcome, and settles it — firing the invocation's onDone (on
// success) or onError (on failure) event through the durable instance and
// recording the produced step. The service is resolved and executed against the
// registry supplied with WithServiceRegistry; its result or error is journaled
// as a JournalServiceResult correlated by id so recovery replays it without
// re-invoking the service. The outcome reaches the machine JSON-decoded, live as on
// replay, and the journal holds that form: a struct is a map, an integer a float64 that
// loses precision above 2^53, and an error an error with the same message, which
// errors.Is no longer matches.
//
// It returns the routed FireResult and true, or the zero result and false when id
// names no in-flight service (already settled, stopped, or never started). It returns
// false and an error when no registry is wired, the Handle has failed, or ctx ended
// before the service started.
//
// Once the service has run, it returns true with any error. A failed Append, a result
// or KindedEffect that won't encode, or a ctx that ended during the service returns
// ErrHandleFailed and records nothing: a recovered Handle has the service in flight again, and RunService
// runs it a second time. That error also matches the cause, so test ErrHandleFailed
// first: it alone means Recover is needed. A failed Checkpoint returns the plain error,
// and the result is recorded.
// A failed effect dispatch also returns ErrHandleFailed, but the result is recorded:
// Recover retries the effect and the service doesn't re-run.
func (h *Handle[S, E, C]) RunService(ctx context.Context, id string) (state.FireResult[S], bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.refuse(); err != nil {
		return state.FireResult[S]{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return state.FireResult[S]{}, false, err
	}
	if h.svc == nil {
		return state.FireResult[S]{}, false, fmt.Errorf("durable: no service registry wired for %q (supply WithServiceRegistry)", h.id)
	}
	if !h.svc.HasPending(id) {
		return state.FireResult[S]{}, false, nil
	}

	results := h.svc.Tick(ctx, id)
	if len(results) == 0 {
		return state.FireResult[S]{}, false, nil
	}
	res := results[0]

	// A context canceled during the service stops the settle's Fire before it applies,
	// so the instance never saw this result: journaling it would commit it on recovery.
	// The runner has already dropped the service, so this Handle can't run it again;
	// Recover re-arms it.
	if res.Err != nil && ctx.Err() != nil {
		return res, true, h.fail(fmt.Errorf("durable: settling service %q for %q: %w", id, h.id, res.Err))
	}

	// The settle re-fired the invocation's onDone / onError, which may enter a state
	// that arms a timer, starts another service, or spawns an actor. Absorb those
	// effects into every driver so the next seam method finds them armed — the service
	// seam composes with the timer and actor seams in one instance.
	h.absorbDrivers(ctx, res.Effects)

	entry, err := recordServiceOutcome(id, h.svc.LastResult, h.svc.LastError())
	if err != nil {
		return res, true, h.fail(fmt.Errorf("durable: recording service %q result for %q: %w", id, h.id, err))
	}
	if h.svcBuf != nil {
		*h.svcBuf = append(*h.svcBuf, entry)
	}

	rec := Record{Step: h.nextStep, Entries: h.drainServices()}
	if err := h.commitStep(ctx, &rec, res.Effects); err != nil {
		return res, true, err
	}
	return res, true, nil
}

// replayableOutcome gives a service's outcome the form a replay supplies, so the live
// settle and a replay can't differ: a result in its JSON-decoded form, an error as an
// error with the same message once any invalid UTF-8 is replaced, which is what the
// round trip through JSON does. An Error method that panics settles with a message
// saying so. A result with no JSON form is left as it is, and fails
// the Handle when recording it fails.
func replayableOutcome(result any, err error) (any, error) {
	if err != nil {
		msg, merr := errorMessage(err)
		if merr != nil {
			return nil, merr
		}
		b, jerr := marshalPayload(msg)
		if jerr != nil {
			return nil, errors.New(msg)
		}
		if jerr = json.Unmarshal(b, &msg); jerr != nil {
			return nil, errors.New(msg)
		}
		return nil, errors.New(msg)
	}
	b, jerr := marshalPayload(result)
	if jerr != nil {
		return result, nil
	}
	var decoded any
	if jerr = json.Unmarshal(b, &decoded); jerr != nil {
		return result, nil
	}
	return decoded, nil
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
		raw, err := marshalPayload(result)
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
// returns the effects the settle emitted, and reports an error only when the recorded entry cannot be decoded or names no
// in-flight service (a corrupt or mis-ordered journal).
func replayService[S comparable, E comparable, C any](ctx context.Context, svc *state.ServiceRunner[S, E, C], entry state.JournalEntry) ([]state.Effect, error) {
	if svc == nil {
		return nil, errors.New("durable: replaying a service result requires a service registry (supply WithServiceRegistry)")
	}
	var sp serviceResultPayload
	if err := json.Unmarshal(entry.Payload, &sp); err != nil {
		return nil, fmt.Errorf("durable: decoding recorded service %q outcome: %w", entry.CorrelationID, err)
	}
	if sp.Error != nil {
		res, ok := svc.SettleError(ctx, entry.CorrelationID, errors.New(*sp.Error))
		if !ok {
			return nil, fmt.Errorf("durable: replaying service %q: no in-flight service to settle", entry.CorrelationID)
		}
		return res.Effects, nil
	}
	var result any
	if len(sp.Result) > 0 {
		if err := json.Unmarshal(sp.Result, &result); err != nil {
			return nil, fmt.Errorf("durable: decoding recorded service %q result: %w", entry.CorrelationID, err)
		}
	}
	res, ok := svc.SettleDone(ctx, entry.CorrelationID, result)
	if !ok {
		return nil, fmt.Errorf("durable: replaying service %q: no in-flight service to settle", entry.CorrelationID)
	}
	return res.Effects, nil
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
