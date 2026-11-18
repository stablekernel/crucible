package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

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
// that wasn't recorded, after a context canceled during the service, a crash, or a failed
// Append whose write didn't land, has nothing to replay, so it stays in flight, runs again
// when the host calls RunService, and must tolerate a second run. A result that won't
// decode for its onDone event is recorded parked: nothing settles, replay parks it
// again, and a later RunService settles it, once the decoder decodes it, without
// running the service again.
//
// The Runner wraps the kernel's reusable host driver, state.ServiceRunner, which
// turns StartService / StopService effects into real ServiceFn executions and
// re-fires each result through the invocation's onDone / onError event. On the live
// path RunService delegates to ServiceRunner.Tick (resolve + run + settle), keeps the
// JSON the settle decoded the outcome from (see replayableOutcome), and journals it as
// a JournalServiceResult correlated by the invocation id. On recovery the recorded
// outcomes are replayed in order through ServiceRunner.SettleDone / SettleError — the
// same settle methods the live run drove — so the kernel re-fires the identical onDone
// / onError event with the identical data and re-derives byte-identical context,
// having run no service.
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
	// SettleDone outcome: the bytes the live settle decoded. Replay decodes them through
	// the Runner's EventDataDecoder for the onDone event and re-fires them through
	// onDone, so the transition's Assign reads from AssignCtx.Event the value the live
	// run's decode gave (see replayableOutcome). A Record of a format before
	// decodedResultsFormat replays them in JSON's generic form, as the version that
	// wrote it did.
	Result json.RawMessage `json:"result,omitempty"`
	// Error is the failure message for a SettleError outcome. It is reconstructed
	// into an error and re-fired through onError so the transition's Assign reads it
	// from AssignCtx.Event.
	Error *string `json:"error,omitempty"`
	// Parked marks a result that didn't decode for its onDone event: it is recorded so
	// the ServiceFn never runs again, but it didn't settle. Replay parks it again rather
	// than settling it, and a later RunService settles it once the decoder decodes it.
	// An older Record never sets it.
	Parked bool `json:"parked,omitempty"`
	// Type is a result's Go type, with its import path (see typeName): the type the
	// ServiceFn returned. A parked result records it so the RunService that settles it
	// holds the decoder to that type, as the live settle did, and every successful
	// settle records it, the one that settles a parked result too, so replay holds the
	// decoder to it as the settle did. It is empty for an error, and for a result
	// recorded before this field, which replays with no type check.
	Type string `json:"type,omitempty"`
}

// decodeServiceEntry decodes a recorded service outcome's payload, once for replay to
// park or settle it.
func decodeServiceEntry(e state.JournalEntry) (serviceResultPayload, error) {
	var sp serviceResultPayload
	if err := json.Unmarshal(e.Payload, &sp); err != nil {
		return sp, fmt.Errorf("durable: decoding recorded service %q outcome: %w", e.CorrelationID, err)
	}
	return sp, nil
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
// re-invoking the service. The outcome reaches the machine decoded from its JSON form,
// live as on replay, and the journal holds the JSON the live settle decoded, so replay
// decodes the same bytes. A result whose onDone event
// WithEventDataDecoder covers arrives as the decoder's type; otherwise it arrives in
// JSON's generic form: a struct is a map, an integer a float64 that loses precision
// above 2^53. A typed nil result, such as a nil pointer, goes through the decoder as
// JSON's null, so DecodeAs[*T] hands Assign a nil *T, live and on replay; with no
// decoder it arrives as an untyped nil, and an untyped nil result arrives as one
// without reaching the decoder. (A WithEventData payload that is a typed nil is refused
// instead, as EventDataDecoder says: a Fire can be retried with an untyped nil, but a
// service has already run.) An error is message-only: an error with the same message,
// which errors.Is and errors.As no longer match, live or on replay. So onError's Assign
// and the ServiceRunner's LastError see a plain error even for an unbound service's
// *state.UnboundRefError. The journal records the Go type the service returned,
// and replay holds the decoder to it, as the live settle did: a decoder that returns
// another type fails Recover and StateAt with ErrEventDataEncoding, though JSON's
// generic form and nil are always accepted. Register the decoder before results are
// recorded: changing the type a decoder returns for an event whose results are
// recorded fails Recover on them, unless it returns the generic form, and a decoder
// added later for the service's type changes what replay produces.
//
// It returns the routed FireResult and true, or the zero result and false when id
// names no in-flight service (already settled, stopped, or never started). The bool
// says whether the service was in flight and ran, not whether it settled: a parked
// result returns true, and so does a settle that finds the activation the ServiceFn
// ran for stopped, or stopped and armed again, while it ran, which returns the zero
// result and no error and records nothing, since nothing fired. The step lock keeps
// durable's own steps from doing that, so only code that drives the ServiceRunner past
// the Handle can. It returns false and an error when no registry is wired, the Handle
// has failed, or ctx ended before the service started.
//
// Once the service has run, it returns true with any error. A result the decoder for
// its onDone event can't decode, such as a number beyond float64, or decodes as a type
// other than the service returned, is parked: RunService records the result as the
// ServiceFn returned it, with its Go type, settles nothing, and returns
// ErrEventDataEncoding, not ErrHandleFailed, before any onDone or onError Assign runs.
// The Handle stays healthy and the service stays in flight, on this Handle and on a
// recovered one, but its ServiceFn never runs again. The decoder is part of the
// Runner's options, so this Handle never sees a fix: Recover with the fixed decoder,
// then call RunService, and it settles the parked result. The decoder must return the
// recorded type, by import path and name, or JSON's generic form: after the type is
// moved or renamed, only a decoder that returns the generic form settles it, so settle
// parked results before such a move. Until then each RunService returns
// ErrEventDataEncoding and records nothing more. The Handle that parked the result also
// holds the decoder to the type itself, as the live settle did; a recovered one has
// only the name, which two types of one package can share, such as two function-local
// types. A Fire or timer that leaves
// the state that declares the service stops it and discards the parked result; one that
// enters the state again starts a new activation, whose ServiceFn runs. A checkpoint
// keeps the parked result, and StateAt shows the instance as it was before the park.
//
// A result that won't encode, a ctx that ended during the service (whether or not the
// result decodes) or while its settle's effects were absorbed (even after a clean
// settle), or a Fire the settle drove refused because a context ended (ctx, or one
// middleware derived from it) returns ErrHandleFailed and records nothing: a recovered
// Handle has the service in flight again, and RunService runs it a second time. A
// result that won't encode runs no Assign, as one that won't decode doesn't. A KindedEffect the settle emits
// whose payload won't marshal doesn't fail it: it is recorded by id and kind, as
// recordEffects says, and dispatches as any effect does.
// A failed Append also returns ErrHandleFailed, but the write may have landed anyway:
// Recover finds out, and the service runs again only if the result isn't in the
// journal. If another goroutine failed the Handle while this step was recording, it
// returns the result, true and ErrHandleFailed saying the step was recorded. Every
// ErrHandleFailed also matches its cause, so test for ErrHandleFailed first, then
// ErrFenced, as the Handle doc says. A failed Checkpoint returns the plain error, and
// the result is recorded; on a step that parked its result, joined with the
// ErrEventDataEncoding. A failed effect dispatch also returns ErrHandleFailed, but the
// result is recorded: Recover retries the effect and the service doesn't run again.
func (h *Handle[S, E, C]) RunService(ctx context.Context, id string) (state.FireResult[S], bool, error) {
	if err := h.acquire(ctx); err != nil {
		return state.FireResult[S]{}, false, err
	}
	res, ran, rec, des, err := h.lockedServiceStep(ctx, id)
	if rec == nil {
		return res, ran, err
	}
	// Dispatch after releasing the step lock, as Fire does. A step that parked its
	// result is recorded and returns why it parked; it offers its checkpoint as any
	// step does.
	ferr := h.finishStep(ctx, rec, des)
	if err == nil {
		return res, true, ferr
	}
	if ferr != nil {
		return res, true, errors.Join(err, ferr)
	}
	return res, true, err
}

// lockedServiceStep runs serviceStep and releases the step lock however it returns. A
// panic in host code fails the Handle first, since the settle may have applied.
func (h *Handle[S, E, C]) lockedServiceStep(ctx context.Context, id string) (state.FireResult[S], bool, *Record, []dispatchableEffect, error) {
	defer h.release()
	defer h.failOnPanic()
	return h.serviceStep(ctx, id)
}

// serviceStep is the part of RunService that holds the step lock: it runs and settles
// the service and records the step, naming its domain effects, before any is
// dispatched. It returns a nil Record when nothing was recorded.
func (h *Handle[S, E, C]) serviceStep(ctx context.Context, id string) (state.FireResult[S], bool, *Record, []dispatchableEffect, error) {
	if err := h.refuse(ctx); err != nil {
		return state.FireResult[S]{}, false, nil, nil, err
	}
	if h.svc == nil {
		return state.FireResult[S]{}, false, nil, nil, fmt.Errorf("durable: no service registry wired for %q (supply WithServiceRegistry)", h.id)
	}
	if !h.svc.HasPending(id) {
		return state.FireResult[S]{}, false, nil, nil, nil
	}

	// A parked result settles in place of a run: the ServiceFn ran once, and the journal
	// holds what it returned.
	parked, isParked := h.parked[id]
	var res state.FireResult[S]
	routed := true
	if isParked {
		res, routed = h.svc.SettleDone(h.stepContext(ctx), id, parked)
	} else if results := h.svc.Tick(h.stepContext(ctx), id); len(results) > 0 {
		res = results[0]
	} else {
		// HasPending found the service in flight, so the ServiceFn ran: Tick returns
		// nothing only when its activation was stopped, or stopped and armed again,
		// before the settle.
		routed = false
	}
	// The JSON this settle decoded is the step's alone: take it, so no later step can
	// journal it and the Handle doesn't hold it.
	settled := h.svcJSON.take()
	if derr := (*undecodableResult)(nil); errors.As(res.Err, &derr) {
		// The result didn't decode for its onDone event, so the transform aborted the
		// settle: nothing fired and no Assign ran. Park it, unless a context that ended
		// during the service may have cut the result short: as for a result that
		// settles, that records nothing, and Recover runs the service again.
		if !isParked {
			if err := stepCanceled(ctx); err != nil {
				return state.FireResult[S]{}, true, nil, nil, h.fail(fmt.Errorf("durable: settling service %q for %q: %w", id, h.id, err))
			}
		}
		return h.parkResult(ctx, id, derr, isParked)
	}
	if !routed {
		// The activation the ServiceFn ran for was stopped, or stopped and armed again,
		// before the settle: nothing fired, so there is nothing to record, and a new
		// activation stays in flight. The step lock keeps any other step from doing that
		// while this one runs, so only code that drives the ServiceRunner past the Handle
		// can. It reports true: the service was in flight and its ServiceFn ran.
		return state.FireResult[S]{}, true, nil, nil, nil
	}
	// The service settled: unless the settle's own Fire leaves or re-enters the state
	// that declares it, which the absorb below notes, a checkpoint records it as settled.
	h.markSettled(id)

	// The settle re-fired the invocation's onDone / onError, which may enter a state
	// that arms a timer, starts another service, or spawns an actor. The ServiceRunner
	// already started and stopped the services those effects name as it settled, so
	// absorb them into the other drivers only, so the next seam method finds them armed
	// — the service seam composes with the timer and actor seams in one instance.
	h.absorbSettle(h.stepContext(ctx), res.Effects)

	// A context that ended during the service or the absorb can stop the settle's Fire,
	// or a Fire an actor spawn routes, before it applies, so the instance may not have
	// seen this result: journaling it would commit on recovery what live never applied.
	// The runner has already dropped the service, so this Handle can't run it again;
	// Recover re-arms it. This step rechecks ctx whatever its absorb handed the actor
	// system, unlike Fire and Tick, which fail only on a parent Fire a context refused:
	// the service itself ran under ctx, so its outcome can be one ctx's ending caused.
	fires := h.actorFires.take()
	if err := stepCanceled(ctx, append([]error{res.Err}, parentFireErrs(fires)...)...); err != nil {
		return res, true, nil, nil, h.fail(fmt.Errorf("durable: settling service %q for %q: %w", id, h.id, err))
	}
	// An actor the settle's absorb drove settled with an outcome that won't record: its
	// settle aborted, firing nothing, so nothing was applied for it. Fail the Handle and
	// record nothing, as for a context that ended, so a recovered Handle runs the actor
	// again. Unlike the service's own result, an actor outcome is never parked: the
	// service already ran, but the actor can run again.
	if abort := h.actorFires.takeAborts(); abort != nil {
		return res, true, nil, nil, h.fail(fmt.Errorf("durable: settling service %q for %q: %w", id, h.id, abort))
	}

	entry, err := recordServiceOutcome(id, settled, h.svc.LastError())
	if err != nil {
		return res, true, nil, nil, h.fail(fmt.Errorf("durable: recording service %q result for %q: %w", id, h.id, err))
	}
	if h.svcBuf != nil {
		*h.svcBuf = append(*h.svcBuf, entry)
	}

	// Every parent Fire the absorb drove (a message an actor sent as it spawned, a
	// settle, a failed spawn's onError) applied after the settle, so the step journals
	// them, and replay re-fires them after it.
	driven, err := recordParentFires(fires, false)
	if err != nil {
		return res, true, nil, nil, h.fail(fmt.Errorf("durable: recording %q's parent Fires: %w", h.id, err))
	}

	// Arming a timer the settle's state declares reads the clock, so the step carries
	// its reads.
	rec := &Record{Step: h.nextStep, Entries: append(append(h.drainClock(), h.drainServices()...), driven...)}
	des, err := h.recordStep(ctx, rec, res.Effects)
	if err != nil {
		return res, true, nil, nil, err
	}
	return res, true, rec, des, nil
}

// parkResult parks the result of service id that derr says didn't decode for its
// onDone event: it records the result's JSON and Go type in a Record of its own, so the
// ServiceFn never runs again, and keeps it for the next RunService of id to settle. The
// service stays in flight in the ServiceRunner and the Handle stays healthy. It returns
// the Record, which the caller finishes as any step's, and an error wrapping
// ErrEventDataEncoding that says the result is parked. A result already parked
// (already) is in the journal, so it records nothing. A result with no JSON form, which
// the transform aborted with no raw to record, fails the Handle, which leaves the
// service in flight for a recovered Handle to run again.
func (h *Handle[S, E, C]) parkResult(ctx context.Context, id string, derr *undecodableResult, already bool) (state.FireResult[S], bool, *Record, []dispatchableEffect, error) {
	parkErr := fmt.Errorf("durable: settling service %q for %q: its result is parked until RunService can decode it: %w", id, h.id, derr)
	if already {
		return state.FireResult[S]{}, true, nil, nil, parkErr
	}
	if derr.raw == nil {
		return state.FireResult[S]{}, true, nil, nil, h.fail(fmt.Errorf("durable: settling service %q for %q: %w", id, h.id, derr))
	}
	payload, err := json.Marshal(serviceResultPayload{Result: derr.raw, Parked: true, Type: derr.typ})
	if err != nil {
		return state.FireResult[S]{}, true, nil, nil, h.fail(fmt.Errorf("durable: recording service %q result for %q: %w", id, h.id, err))
	}
	// Keep it before the Record is appended, so a checkpoint the step makes due carries
	// it. A failed append fails the Handle, so a parked result it didn't record is never
	// settled.
	if h.parked == nil {
		h.parked = map[string]recordedResult{}
	}
	h.parked[id] = recordedResult{raw: derr.raw, typ: derr.typ, rtype: derr.rtype}
	entry := state.JournalEntry{Kind: state.JournalServiceResult, CorrelationID: id, Payload: payload}
	rec := &Record{Step: h.nextStep, Entries: append(h.drainClock(), entry)}
	if _, err := h.recordStep(ctx, rec, nil); err != nil {
		return state.FireResult[S]{}, true, nil, nil, err
	}
	return state.FireResult[S]{}, true, rec, nil, parkErr
}

// markSettled records that the service id settled in its state's current activation, so
// no result of it is parked. It and forgetServices are the only writers of settled
// alongside parked, so the two can't disagree.
func (h *Handle[S, E, C]) markSettled(id string) {
	if h.settled == nil {
		h.settled = map[string]bool{}
	}
	h.settled[id] = true
	delete(h.parked, id)
}

// replayableOutcome returns the transform a ServiceRunner or an ActorSystem applies
// to every outcome as it settles, so the live settle and a replay can't differ. A
// result is encoded to JSON and decoded through decode for the invocation's onDone
// event, as a WithEventData payload is: the live run passes the ServiceFn's or the
// behavior's value, a replay the recorded JSON as a recordedResult, and both get
// decode's value. With the default decoder that is JSON's generic form. Live, a
// decoder that returns a concrete type other than the result's is refused, though
// JSON's generic form, including a json.Number, is always accepted, so the default
// keeps working. A settle that succeeds leaves in settled the JSON it decoded and
// the result's Go type, which the driving step journals, so replay decodes the bytes
// live decoded and holds the decoder to the type live did; an error outcome leaves
// nothing there. what names the outcome in the errors an abort carries ("service
// result", "actor done-data"), and errWhat an error outcome in the message a
// panicking Error method settles with ("service error", "actor error").
//
// A result that won't decode, or is refused, aborts the settle with an
// *undecodableResult before it fires anything, so no onDone or onError Assign runs;
// serviceStep sees it in the settle's FireResult.Err and parks the result, an actor
// step sees it in the settle it aborted (WithSettleTransform) and fails the Handle,
// and a replay fails on it. A parked result settles as a recordedResult that
// carries its Go type, and the decoder must return that type, or JSON's generic
// form, as live. A result with no JSON form aborts the settle too, with no JSON to
// park, and serviceStep fails the Handle. A legacyResult, which replay passes for a
// Record of an older format, settles as it is, as that version replayed it.
//
// An error becomes an error with its message as a Store keeps it (see
// normalizedErrorMessage), live and on replay. An Error method that panics settles
// with a message saying so, normalized the same way.
func replayableOutcome[E comparable](decode EventDataDecoder[E], settled *settledJSON, what, errWhat string) func(E, any, error) (any, error, error) {
	return func(event E, result any, err error) (any, error, error) {
		settled.set(recordedResult{})
		if err != nil {
			msg, perr := normalizedErrorMessage(err, errWhat)
			if perr != nil {
				msg = validUTF8(perr.Error())
			}
			return nil, errors.New(msg), nil
		}
		if legacy, ok := result.(legacyResult); ok {
			return legacy.value, nil, nil
		}
		recorded, replay := result.(recordedResult)
		b := []byte(recorded.raw)
		if !replay {
			var jerr error
			if b, jerr = marshalPayload(result); jerr != nil {
				return nil, nil, &undecodableResult{err: fmt.Errorf("%s won't encode: %w", what, jerr)}
			}
		}
		var value any
		var derr error
		if typedNil(b, recorded, replay, result) {
			// A typed nil keeps its type through the decoder, live and on replay alike.
			value, derr = decodeData(event, b, decode)
		} else {
			value, derr = decodePayload(eventPayload{data: b}, event, decode)
		}
		if derr != nil {
			derr = fmt.Errorf("%w: %s won't decode for event %v: %w", ErrEventDataEncoding, what, event, derr)
		} else if !replay {
			derr = decodedAsPassed(value, result, what, true)
		} else {
			derr = decodedAsRecorded(value, recorded)
		}
		if derr != nil {
			rtype := recorded.rtype
			if !replay {
				rtype = reflect.TypeOf(result)
			}
			return nil, nil, &undecodableResult{raw: b, typ: resultType(recorded, replay, result), rtype: rtype, err: derr}
		}
		settled.set(recordedResult{raw: b, typ: resultType(recorded, replay, result)})
		return value, nil, nil
	}
}

// typedNil reports whether b, a service result's JSON, is the null of a typed nil,
// such as a nil pointer or map: the ServiceFn's own value live, or the Go type the
// journal recorded on replay. An untyped nil, or a result recorded with no type, isn't.
func typedNil(b []byte, recorded recordedResult, replay bool, result any) bool {
	if string(b) != "null" {
		return false
	}
	if replay {
		return recorded.typ != "" && recorded.typ != typeName(nil)
	}
	return result != nil
}

// settledJSON holds the JSON the last successful settle decoded and the result's Go
// type, for serviceStep to journal. The transform sets it during the settle, which runs
// under the Handle's step lock live and on replay's one goroutine, and serviceStep
// takes it once the settle returns, so it holds nothing between steps. A nil
// settledJSON keeps nothing.
type settledJSON struct{ recordedResult }

func (s *settledJSON) set(r recordedResult) {
	if s != nil {
		s.recordedResult = r
	}
}

// take returns what the last settle left and clears it.
func (s *settledJSON) take() recordedResult {
	if s == nil {
		return recordedResult{}
	}
	r := s.recordedResult
	s.recordedResult = recordedResult{}
	return r
}

// legacyResult is a service result recorded in a Record of a format before
// decodedResultsFormat, decoded as that version replayed it: into an any, with no
// decoder or type check. The transform settles it as it is.
type legacyResult struct{ value any }

// resultType returns the Go type a park, or a successful settle, records for result:
// the recorded one on replay, else the ServiceFn's value's.
func resultType(recorded recordedResult, replay bool, result any) string {
	if replay {
		return recorded.typ
	}
	return typeName(result)
}

// recordedResult is a journaled service result's JSON, which replayService settles so
// the transform decodes it as the live run decoded the ServiceFn's value, and which a
// RunService settles for a parked result.
type recordedResult struct {
	raw json.RawMessage
	// typ is the result's Go type, as typeName gives it, which a park and every
	// successful settle record; empty for a result recorded before settles recorded it,
	// which replay doesn't type-check.
	typ string
	// rtype is the result's Go type itself, which only a result this Handle parked
	// carries: typ can't tell apart two types of the same name and package, such as two
	// function-local types, so a parked settle on the Handle that parked it compares the
	// types, as the live settle did. A result read from the journal or a checkpoint has
	// only typ.
	rtype reflect.Type
}

// typeName returns the full name of v's type, the form a parked result records: a
// named type's import path and name, so two packages' model.Result differ, and an
// unnamed type spelled from those (see fullTypeName).
func typeName(v any) string {
	t := reflect.TypeOf(v)
	if t == nil {
		return "<nil>"
	}
	return fullTypeName(t)
}

// fullTypeName spells t as reflect does, but with each named type's import path in
// place of its package name, so two packages' model.Result differ wherever they
// appear: in a pointer, slice, array, map or chan, a func's parameters and results, an
// interface's methods, and a struct literal's fields, whose tags and unexported names'
// packages are part of the type too.
func fullTypeName(t reflect.Type) string {
	if name := t.Name(); name != "" {
		if pkg := t.PkgPath(); pkg != "" {
			return pkg + "." + name
		}
		return name
	}
	switch t.Kind() {
	case reflect.Pointer:
		return "*" + fullTypeName(t.Elem())
	case reflect.Slice:
		return "[]" + fullTypeName(t.Elem())
	case reflect.Array:
		return fmt.Sprintf("[%d]%s", t.Len(), fullTypeName(t.Elem()))
	case reflect.Map:
		return "map[" + fullTypeName(t.Key()) + "]" + fullTypeName(t.Elem())
	case reflect.Chan:
		return chanTypeName(t)
	case reflect.Func:
		return "func" + signatureName(t)
	case reflect.Struct:
		return structTypeName(t)
	case reflect.Interface:
		return interfaceTypeName(t)
	}
	return t.String()
}

// chanTypeName spells a chan type t, parenthesizing a receive-only element of a
// bidirectional chan, as reflect does.
func chanTypeName(t reflect.Type) string {
	elem := fullTypeName(t.Elem())
	switch t.ChanDir() {
	case reflect.RecvDir:
		return "<-chan " + elem
	case reflect.SendDir:
		return "chan<- " + elem
	}
	if t.Elem().Kind() == reflect.Chan && t.Elem().Name() == "" && t.Elem().ChanDir() == reflect.RecvDir {
		return "chan (" + elem + ")"
	}
	return "chan " + elem
}

// signatureName spells a func type t's parameters and results, without "func".
func signatureName(t reflect.Type) string {
	var b strings.Builder
	b.WriteByte('(')
	for i := range t.NumIn() {
		if i > 0 {
			b.WriteString(", ")
		}
		if t.IsVariadic() && i == t.NumIn()-1 {
			b.WriteString("..." + fullTypeName(t.In(i).Elem()))
			continue
		}
		b.WriteString(fullTypeName(t.In(i)))
	}
	b.WriteByte(')')
	switch n := t.NumOut(); n {
	case 0:
	case 1:
		b.WriteString(" " + fullTypeName(t.Out(0)))
	default:
		b.WriteString(" (")
		for i := range n {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(fullTypeName(t.Out(i)))
		}
		b.WriteByte(')')
	}
	return b.String()
}

// structTypeName spells a struct literal type t field by field: an unexported field's
// name with its package's import path, an embedded field by its type alone, and each
// tag quoted, as reflect does.
func structTypeName(t reflect.Type) string {
	if t.NumField() == 0 {
		return "struct {}"
	}
	var b strings.Builder
	b.WriteString("struct {")
	for i := range t.NumField() {
		if i > 0 {
			b.WriteByte(';')
		}
		f := t.Field(i)
		b.WriteByte(' ')
		if !f.Anonymous {
			b.WriteString(qualifiedName(f.PkgPath, f.Name) + " ")
		}
		b.WriteString(fullTypeName(f.Type))
		if f.Tag != "" {
			b.WriteString(" " + strconv.Quote(string(f.Tag)))
		}
	}
	b.WriteString(" }")
	return b.String()
}

// interfaceTypeName spells an interface literal type t method by method, an unexported
// method's name with its package's import path.
func interfaceTypeName(t reflect.Type) string {
	if t.NumMethod() == 0 {
		return "interface {}"
	}
	var b strings.Builder
	b.WriteString("interface {")
	for i := range t.NumMethod() {
		if i > 0 {
			b.WriteByte(';')
		}
		m := t.Method(i)
		b.WriteString(" " + qualifiedName(m.PkgPath, m.Name) + signatureName(m.Type))
	}
	b.WriteString(" }")
	return b.String()
}

// qualifiedName prefixes an unexported name with the import path of its package, pkg,
// which is empty for an exported one.
func qualifiedName(pkg, name string) string {
	if pkg == "" {
		return name
	}
	return pkg + "." + name
}

// decodedAsRecorded is decodedAsPassed for a recorded result, which records its type's
// name rather than a value: it reports, as an ErrEventDataEncoding error, a value the
// decoder returned as another type than the recorded one. As live, nil and JSON's
// generic form are always accepted. A result this Handle parked keeps its type, and is
// held to it as live held it; one read from the journal or a checkpoint is held to its
// name, which a type of the same name and package, such as another function-local type,
// also has. An empty name, a result recorded before settles recorded the type, accepts
// anything.
func decodedAsRecorded(value any, recorded recordedResult) error {
	typ := recorded.typ
	if typ == "" || value == nil || genericJSON(value) {
		return nil
	}
	if recorded.rtype != nil && reflect.TypeOf(value) != recorded.rtype {
		return fmt.Errorf("%w: a %s service result decodes as another type, %v, of the same name or not; decode it as the service's own %s with WithEventDataDecoder",
			ErrEventDataEncoding, typ, reflect.TypeOf(value), typ)
	}
	if got := typeName(value); got != typ {
		return fmt.Errorf("%w: a %s service result decodes as %s; decode it as %s with WithEventDataDecoder",
			ErrEventDataEncoding, typ, got, typ)
	}
	return nil
}

// undecodableResult is the abort replayableOutcome returns for a result that won't
// decode for its onDone event, or decodes as another type than the ServiceFn returned.
// The ServiceRunner fires no event for it and hands it back in the settle's
// FireResult.Err, where serviceStep finds it and parks the result, and replayService
// finds it and fails the replay. For a result that won't decode, or decodes as another
// type, err wraps ErrEventDataEncoding, as the park RunService returns does. For a
// result with no JSON form raw is nil, err says why it won't encode and wraps no
// sentinel, and serviceStep fails the Handle, so RunService returns ErrHandleFailed,
// not ErrEventDataEncoding, whose Handle stays usable.
type undecodableResult struct {
	err   error
	raw   json.RawMessage // the result's JSON, which a park records
	typ   string          // the result's Go type, as typeName gives it, for a park
	rtype reflect.Type    // the result's Go type itself, which the Handle keeps with a park
}

func (u *undecodableResult) Error() string { return u.err.Error() }
func (u *undecodableResult) Unwrap() error { return u.err }

// genericJSON reports whether v is a type JSON decodes into an any: what the default
// decoder gives, or a json.Number from a decoder that keeps numbers with UseNumber.
func genericJSON(v any) bool {
	switch v.(type) {
	case map[string]any, []any, float64, json.Number, string, bool:
		return true
	}
	return false
}

// decodedAsPassed reports, as an ErrEventDataEncoding error, a value the decoder
// returned as another type than passed, the value the host handed over; what names it.
// A Fire's payload must come back as the caller's type. A service result may also come
// back as nil or in JSON's generic form (generic), so the default decoder serves every
// result: the service has already run, and refusing its result would only run it again.
func decodedAsPassed(value, passed any, what string, generic bool) error {
	if generic && (value == nil || genericJSON(value)) {
		return nil
	}
	if got, want := reflect.TypeOf(value), reflect.TypeOf(passed); got != want {
		return fmt.Errorf("%w: a %v %s decodes as %v; decode it as %v with WithEventDataDecoder",
			ErrEventDataEncoding, want, what, got, want)
	}
	return nil
}

// recordServiceOutcome builds the JournalServiceResult entry for a just-settled
// service: the error lastErr, or, for a success, settled, the JSON the settle decoded
// and the result's Go type (see settledJSON), so replay decodes the bytes live decoded
// and holds the decoder to the type live did.
func recordServiceOutcome(id string, settled recordedResult, lastErr error) (state.JournalEntry, error) {
	var sp serviceResultPayload
	if lastErr != nil {
		msg := lastErr.Error()
		sp.Error = &msg
	} else {
		sp.Result, sp.Type = settled.raw, settled.typ
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
// returns the settle's effects, so the caller arms the timers its state declares, as
// the live RunService did. sp is entry's decoded payload. A result in a Record of a
// format before decodedResultsFormat (decoded false) replays as the version that wrote
// it replayed it, in JSON's generic form through no decoder: that version's live run
// saw the ServiceFn's own value, which no decoder of this one can be held to. It
// reports an error only when the recorded result cannot be decoded or names no
// in-flight service (a corrupt or mis-ordered journal).
func replayService[S comparable, E comparable, C any](ctx context.Context, svc *state.ServiceRunner[S, E, C], entry state.JournalEntry, sp serviceResultPayload, decoded bool) ([]state.Effect, error) {
	if svc == nil {
		return nil, errors.New("durable: replaying a service result requires a service registry (supply WithServiceRegistry)")
	}
	if sp.Error != nil {
		res, ok := svc.SettleError(ctx, entry.CorrelationID, errors.New(*sp.Error))
		if !ok {
			return nil, fmt.Errorf("durable: replaying service %q: no in-flight service to settle", entry.CorrelationID)
		}
		return res.Effects, nil
	}
	// The transform decodes the recorded JSON for the onDone event, as it did live. One
	// that won't decode aborts the settle before it fires anything.
	var result any
	switch {
	case !decoded:
		var v any
		if len(sp.Result) > 0 {
			if err := json.Unmarshal(sp.Result, &v); err != nil {
				return nil, fmt.Errorf("durable: decoding recorded service %q result: %w", entry.CorrelationID, err)
			}
		}
		result = legacyResult{value: v}
	case len(sp.Result) > 0:
		// The recorded type holds the decoder to it, as the settle did.
		result = recordedResult{raw: sp.Result, typ: sp.Type}
	}
	res, ok := svc.SettleDone(ctx, entry.CorrelationID, result)
	if derr := (*undecodableResult)(nil); errors.As(res.Err, &derr) {
		return nil, fmt.Errorf("durable: decoding recorded service %q result: %w", entry.CorrelationID, derr)
	}
	if !ok {
		return nil, fmt.Errorf("durable: replaying service %q: no in-flight service to settle", entry.CorrelationID)
	}
	return res.Effects, nil
}

// replayPark checks a recorded parked result against the replay: its service must be in
// flight, as it was when the live run parked it, and stays in flight. It settles
// nothing, so no Assign runs. It reports an error for a corrupt or mis-ordered journal.
func replayPark[S comparable, E comparable, C any](svc *state.ServiceRunner[S, E, C], entry state.JournalEntry) error {
	if svc == nil {
		return errors.New("durable: replaying a parked service result requires a service registry (supply WithServiceRegistry)")
	}
	if !svc.HasPending(entry.CorrelationID) {
		return fmt.Errorf("durable: replaying parked service %q: no in-flight service", entry.CorrelationID)
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
