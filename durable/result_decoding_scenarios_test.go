package durable_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These scenario tests pin what a host's Assign sees for a service result, live and
// after Recover: a result whose onDone event WithEventDataDecoder covers arrives as the
// decoder's type, any other arrives in JSON's generic form, an error is message-only,
// and a decoder that fails or returns the wrong type parks the result before any
// Assign runs.

type scnUser struct {
	ID   int64
	Name string
}

type scnAdmin struct {
	ID    int64
	Name  string
	Level int
}

var errScnNotFound = errors.New("user not found")

// scnAssigns records what the onDone and onError Assigns saw, in order, and how many
// times each ran.
type scnAssigns struct {
	seen        []any
	done, fails int
}

// scnMachine invokes fn on entering "loading": onDone "done" goes to ready, onError
// "fail" to failed, each through an Assign that notes what it saw in a.
func scnMachine(fn state.ServiceFn[svcCtx], a *scnAssigns) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("scn").
		Service("fetch", fn).
		Reducer("noteDone", func(in state.AssignCtx[svcCtx]) svcCtx {
			a.done++
			a.seen = append(a.seen, in.Event)
			return in.Entity
		}).
		Reducer("noteFail", func(in state.AssignCtx[svcCtx]) svcCtx {
			a.fails++
			a.seen = append(a.seen, in.Event)
			return in.Entity
		}).
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done"), state.WithInvokeOnError("fail")).
		State("ready").Final().
		State("failed").Final().
		Initial("loading").
		Transition("loading").On("done").GoTo("ready").Assign("noteDone").
		Transition("loading").On("fail").GoTo("failed").Assign("noteFail").
		Quench()
}

const scnInvoke = "scn" // the machine id InvokeID takes

// scnStart starts an instance of m in "loading" and returns its Handle.
func scnStart(t *testing.T, m *state.Machine[string, string, svcCtx], store durable.Store, opts ...durable.Option[string, string, svcCtx]) *durable.Handle[string, string, svcCtx] {
	t.Helper()
	h, err := durable.NewRunner(m, store, opts...).Start(context.Background(), "scn-1", svcCtx{}, state.WithInitialState("loading"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return h
}

// scnRun starts an instance in a state that invokes fn, runs the service live, then
// Recovers it, and returns what onDone's or onError's Assign saw each time (live
// first), with the RunService and Recover errors.
func scnRun(t *testing.T, fn state.ServiceFn[svcCtx], opts ...durable.Option[string, string, svcCtx]) (a *scnAssigns, runErr, recoverErr error) {
	t.Helper()
	ctx := context.Background()
	a = &scnAssigns{}
	m := scnMachine(fn, a)
	store := durable.NewMemStore()
	opts = append([]durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))}, opts...)
	h := scnStart(t, m, store, opts...)
	_, _, runErr = h.RunService(ctx, state.InvokeID(scnInvoke, "loading", 0))
	_, recoverErr = durable.Recover(ctx, m, store, "scn-1", opts...)
	return a, runErr, recoverErr
}

func describe(v any) string { return fmt.Sprintf("%T %+v", v, v) }

// decoderFor decodes event's payload with d and every other event's as JSON's own form.
func decoderFor(event string, d func([]byte) (any, error)) durable.Option[string, string, svcCtx] {
	return durable.WithEventDataDecoder[string, string, svcCtx](func(e string, data []byte) (any, error) {
		if e == event {
			return d(data)
		}
		return durable.DecodeAs[any](data)
	})
}

func returning(v any, err error) state.ServiceFn[svcCtx] {
	return func(context.Context, state.ServiceCtx[svcCtx]) (any, error) { return v, err }
}

// mustTwo fails unless the Assign ran exactly live and once on replay, and saw the
// same value both times.
func mustTwo(t *testing.T, a *scnAssigns, runErr, recErr error) {
	t.Helper()
	if runErr != nil || recErr != nil {
		t.Fatalf("RunService: %v, Recover: %v", runErr, recErr)
	}
	if len(a.seen) != 2 {
		t.Fatalf("Assign ran %d times, want 2 (live, replay): %v", len(a.seen), a.seen)
	}
	if describe(a.seen[0]) != describe(a.seen[1]) {
		t.Fatalf("live saw %s, replay %s; want the same", describe(a.seen[0]), describe(a.seen[1]))
	}
}

// TestServiceResult_DecoderForOnDone_ArrivesAsTheDecodersType: a struct result whose
// onDone event has a decoder reaches the Assign as that struct, live and on replay.
func TestServiceResult_DecoderForOnDone_ArrivesAsTheDecodersType(t *testing.T) {
	a, runErr, recErr := scnRun(t, returning(scnUser{ID: 7, Name: "ann"}, nil),
		decoderFor("done", durable.DecodeAs[scnUser]))
	mustTwo(t, a, runErr, recErr)
	for i, v := range a.seen {
		if u, ok := v.(scnUser); !ok || u != (scnUser{ID: 7, Name: "ann"}) {
			t.Errorf("seen[%d] = %s, want the scnUser", i, describe(v))
		}
	}
}

// TestServiceResult_Int64Above2to53_ExactWithADecoder: an int64 beyond float64's exact
// range survives when the decoder decodes it as an int64.
func TestServiceResult_Int64Above2to53_ExactWithADecoder(t *testing.T) {
	const want = int64(1<<53 + 1)
	a, runErr, recErr := scnRun(t, returning(want, nil), decoderFor("done", durable.DecodeAs[int64]))
	mustTwo(t, a, runErr, recErr)
	for i, v := range a.seen {
		if n, ok := v.(int64); !ok || n != want {
			t.Errorf("seen[%d] = %s, want int64 %d", i, describe(v), want)
		}
	}
}

// TestServiceResult_StructWithInt64Above2to53_ExactWithADecoder: the same id inside a
// struct survives too.
func TestServiceResult_StructWithInt64Above2to53_ExactWithADecoder(t *testing.T) {
	const want = int64(1<<53 + 1)
	a, runErr, recErr := scnRun(t, returning(scnUser{ID: want, Name: "big"}, nil),
		decoderFor("done", durable.DecodeAs[scnUser]))
	mustTwo(t, a, runErr, recErr)
	for i, v := range a.seen {
		if u, ok := v.(scnUser); !ok || u.ID != want {
			t.Errorf("seen[%d] = %s, want ID %d", i, describe(v), want)
		}
	}
}

// TestServiceResult_SentinelError_ErrorsIs would have errors.Is match a wrapped
// sentinel in onError. Errors are message-only by design, so it can't.
func TestServiceResult_SentinelError_ErrorsIs(t *testing.T) {
	t.Skip("service errors are message-only by design: onError gets an error with the same message, which errors.Is doesn't match")
	a, runErr, recErr := scnRun(t, returning(nil, fmt.Errorf("lookup 7: %w", errScnNotFound)))
	mustTwo(t, a, runErr, recErr)
	for i, v := range a.seen {
		if e, ok := v.(error); !ok || !errors.Is(e, errScnNotFound) {
			t.Errorf("seen[%d] = %s, want an error matching errScnNotFound", i, describe(v))
		}
	}
}

// TestServiceResult_ErrorIsMessageOnly: a service error reaches onError as an error
// with the same message, live and on replay, and doesn't match the sentinel it wraps.
func TestServiceResult_ErrorIsMessageOnly(t *testing.T) {
	a, runErr, recErr := scnRun(t, returning(nil, fmt.Errorf("lookup 7: %w", errScnNotFound)),
		decoderFor("fail", func([]byte) (any, error) { return nil, errors.New("an error must not reach the decoder") }))
	mustTwo(t, a, runErr, recErr)
	for i, v := range a.seen {
		e, ok := v.(error)
		if !ok || e.Error() != "lookup 7: user not found" || errors.Is(e, errScnNotFound) {
			t.Errorf("seen[%d] = %s, want an error with the message only", i, describe(v))
		}
	}
}

// TestServiceResult_NoDecoder_StructIsAMap: with no decoder for onDone a struct
// arrives in JSON's generic form.
func TestServiceResult_NoDecoder_StructIsAMap(t *testing.T) {
	a, runErr, recErr := scnRun(t, returning(scnUser{ID: 7, Name: "ann"}, nil))
	mustTwo(t, a, runErr, recErr)
	for i, v := range a.seen {
		if _, ok := v.(map[string]any); !ok {
			t.Errorf("seen[%d] = %T, want map[string]any", i, v)
		}
	}
}

// TestServiceResult_NoDecoder_Int64IsFloat64: with no decoder for onDone an int64
// arrives as a float64.
func TestServiceResult_NoDecoder_Int64IsFloat64(t *testing.T) {
	a, runErr, recErr := scnRun(t, returning(int64(42), nil))
	mustTwo(t, a, runErr, recErr)
	for i, v := range a.seen {
		if f, ok := v.(float64); !ok || f != 42 {
			t.Errorf("seen[%d] = %s, want float64 42", i, describe(v))
		}
	}
}

// TestServiceResult_NoJSONForm_FailsTheHandle: a result with no JSON form fails the
// Handle and records nothing, so the recovered instance has the service in flight
// again. No Assign runs: the settle aborts before it fires anything, as for a result
// that won't decode (see TestServiceResult_NoJSONForm_RunsNoAssign).
func TestServiceResult_NoJSONForm_FailsTheHandle(t *testing.T) {
	a, runErr, recErr := scnRun(t, returning(make(chan int), nil), decoderFor("done", durable.DecodeAs[scnUser]))
	if !errors.Is(runErr, durable.ErrHandleFailed) {
		t.Errorf("RunService err = %v, want ErrHandleFailed", runErr)
	}
	if recErr != nil {
		t.Errorf("Recover: %v", recErr)
	}
	if len(a.seen) != 0 {
		t.Errorf("Assign ran %d times, want 0", len(a.seen))
	}
}

// mustFailLoudly fails unless RunService returned ErrEventDataEncoding, parking the
// result with the Handle healthy, no Assign ran, and Recover found the service parked.
func mustFailLoudly(t *testing.T, a *scnAssigns, runErr, recErr error) {
	t.Helper()
	if errors.Is(runErr, durable.ErrHandleFailed) || !errors.Is(runErr, durable.ErrEventDataEncoding) {
		t.Errorf("RunService err = %v, want ErrEventDataEncoding without ErrHandleFailed", runErr)
	}
	if recErr != nil {
		t.Errorf("Recover: %v, want the instance recoverable", recErr)
	}
	if a.done != 0 || a.fails != 0 {
		t.Errorf("onDone's Assign ran %d times and onError's %d, want neither", a.done, a.fails)
	}
}

// TestServiceResult_WrongDecoderType_Compatible_FailsLoudly: a decoder for the wrong
// type (scnAdmin for a scnUser result), whose JSON still decodes, is refused live.
func TestServiceResult_WrongDecoderType_Compatible_FailsLoudly(t *testing.T) {
	a, runErr, recErr := scnRun(t, returning(scnUser{ID: 7, Name: "ann"}, nil),
		decoderFor("done", durable.DecodeAs[scnAdmin]))
	mustFailLoudly(t, a, runErr, recErr)
}

// TestServiceResult_WrongDecoderType_Incompatible_FailsLoudly: a decoder for a type
// the JSON won't decode as (int64 for an object) fails the step too.
func TestServiceResult_WrongDecoderType_Incompatible_FailsLoudly(t *testing.T) {
	a, runErr, recErr := scnRun(t, returning(scnUser{ID: 7, Name: "ann"}, nil),
		decoderFor("done", durable.DecodeAs[int64]))
	mustFailLoudly(t, a, runErr, recErr)
}

// TestServiceResult_NilResult_ArrivesNil: a nil result arrives as nil both ways,
// whatever the decoder.
func TestServiceResult_NilResult_ArrivesNil(t *testing.T) {
	a, runErr, recErr := scnRun(t, returning(nil, nil), decoderFor("done", durable.DecodeAs[scnUser]))
	mustTwo(t, a, runErr, recErr)
	if a.seen[0] != nil {
		t.Errorf("live saw %T, want nil", a.seen[0])
	}
}

// TestServiceResult_DecoderAddedAfterRecording_ChangesReplay pins the documented hazard
// that makes the docs say to register the decoder before recording: a result recorded
// with no decoder, then recovered by a Runner that has one for onDone, reaches the
// replayed Assign as the decoder's type, though live saw JSON's generic form. The
// journal is the same JSON either way; replay decodes it with the decoder it has.
func TestServiceResult_DecoderAddedAfterRecording_ChangesReplay(t *testing.T) {
	ctx := context.Background()
	fn := returning(scnUser{ID: 7, Name: "ann"}, nil)
	a := &scnAssigns{}
	m := scnMachine(fn, a)
	store := durable.NewMemStore()
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	h := scnStart(t, m, store, reg)
	mustRunService(t, h, state.InvokeID(scnInvoke, "loading", 0))
	if _, err := durable.Recover(ctx, m, store, "scn-1", reg, decoderFor("done", durable.DecodeAs[scnUser])); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(a.seen) != 2 {
		t.Fatalf("Assign ran %d times, want 2 (live, replay)", len(a.seen))
	}
	if _, ok := a.seen[0].(map[string]any); !ok {
		t.Errorf("live saw %s, want map[string]any: no decoder was registered", describe(a.seen[0]))
	}
	// The hazard: replay hands the Assign a different type than live did.
	if _, ok := a.seen[1].(scnUser); !ok {
		t.Errorf("replay saw %s, want the scnUser the added decoder gives", describe(a.seen[1]))
	}
}

// failingDecoder fails for onDone and decodes every other event as JSON's own form.
func failingDecoder() durable.Option[string, string, svcCtx] {
	return decoderFor("done", func([]byte) (any, error) { return nil, errors.New("decoder refused") })
}

// TestServiceResult_DecodeFailure_RunsNoOnErrorAssign proves a result that fails to
// decode, or decodes live as another type than the service returned, runs no Assign
// at all: neither onDone's nor onError's, not even once in memory. RunService returns
// ErrEventDataEncoding, the Handle stays healthy, the journal holds one Record that
// parks the result, and the instance stays where it was.
func TestServiceResult_DecodeFailure_RunsNoOnErrorAssign(t *testing.T) {
	cases := []struct {
		name string
		opt  durable.Option[string, string, svcCtx]
	}{
		{"decoder fails", failingDecoder()},
		{"type mismatch", decoderFor("done", durable.DecodeAs[scnAdmin])},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fn := returning(scnUser{ID: 7, Name: "ann"}, nil)
			a := &scnAssigns{}
			m := scnMachine(fn, a)
			store := durable.NewMemStore()
			opts := []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)), tc.opt}
			h := scnStart(t, m, store, opts...)
			_, ran, err := h.RunService(ctx, state.InvokeID(scnInvoke, "loading", 0))
			mustPark(t, "RunService", ran, err)
			if a.done != 0 || a.fails != 0 {
				t.Fatalf("onDone's Assign ran %d times and onError's %d, want neither", a.done, a.fails)
			}
			if got := h.Instance().Current(); got != "loading" {
				t.Fatalf("the live instance is in %q, want loading", got)
			}
			if _, recs, lerr := store.Load(ctx, "scn-1"); lerr != nil || len(recs) != 1 {
				t.Fatalf("journal holds %d Records (err %v), want the one that parks the result", len(recs), lerr)
			}
		})
	}
}

// TestServiceResult_RecordedResultThatWontDecode_RunsNoAssignOnReplay proves Recover and
// StateAt, replaying a recorded result their decoder can't decode, fail with
// ErrEventDataEncoding before any Assign runs.
func TestServiceResult_RecordedResultThatWontDecode_RunsNoAssignOnReplay(t *testing.T) {
	ctx := context.Background()
	fn := returning(scnUser{ID: 7, Name: "ann"}, nil)
	a := &scnAssigns{}
	m := scnMachine(fn, a)
	store := durable.NewMemStore()
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	h := scnStart(t, m, store, reg)
	mustRunService(t, h, state.InvokeID(scnInvoke, "loading", 0))
	if a.done != 1 || a.fails != 0 {
		t.Fatalf("live: onDone ran %d times and onError %d, want 1 and 0", a.done, a.fails)
	}

	if _, err := durable.Recover(ctx, m, store, "scn-1", reg, failingDecoder()); !errors.Is(err, durable.ErrEventDataEncoding) {
		t.Fatalf("Recover: %v, want ErrEventDataEncoding", err)
	}
	if a.done != 1 || a.fails != 0 {
		t.Fatalf("after Recover: onDone ran %d times and onError %d, want no more than live's 1 and 0", a.done, a.fails)
	}

	if _, err := durable.StateAt(ctx, m, store, "scn-1", 0, reg, failingDecoder()); !errors.Is(err, durable.ErrEventDataEncoding) {
		t.Fatalf("StateAt: %v, want ErrEventDataEncoding", err)
	}
	if a.done != 1 || a.fails != 0 {
		t.Fatalf("after StateAt: onDone ran %d times and onError %d, want no more than live's 1 and 0", a.done, a.fails)
	}
}

// TestRunService_ResultEncodingErrorsMatchTheirSentinels pins which sentinel each
// failed settle wraps. A result that won't decode is parked: RunService returns
// ErrEventDataEncoding with the Handle healthy. A result with no JSON form fails the
// Handle: RunService returns ErrHandleFailed, not ErrEventDataEncoding, whose Handle
// stays usable.
func TestRunService_ResultEncodingErrorsMatchTheirSentinels(t *testing.T) {
	t.Run("won't decode", func(t *testing.T) {
		_, runErr, _ := scnRun(t, returning(scnUser{ID: 7, Name: "ann"}, nil), failingDecoder())
		if !errors.Is(runErr, durable.ErrEventDataEncoding) || errors.Is(runErr, durable.ErrHandleFailed) {
			t.Errorf("RunService = %v, want ErrEventDataEncoding without ErrHandleFailed", runErr)
		}
	})
	t.Run("won't encode", func(t *testing.T) {
		_, runErr, _ := scnRun(t, returning(make(chan int), nil))
		if !errors.Is(runErr, durable.ErrHandleFailed) || errors.Is(runErr, durable.ErrEventDataEncoding) {
			t.Errorf("RunService = %v, want ErrHandleFailed without ErrEventDataEncoding", runErr)
		}
	})
}
