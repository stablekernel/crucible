package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	alpha "github.com/stablekernel/crucible/durable/internal/typefixture/alpha/model"
	beta "github.com/stablekernel/crucible/durable/internal/typefixture/beta/model"
	"github.com/stablekernel/crucible/state"
)

// These tests answer review findings on parked service results, the journal's bytes
// for a settled result, a result with no JSON form, and an effect whose MarshalJSON
// panics. Each fails if the defect it names is real.

// A ctx that ended during the ServiceFn, with a result the decoder refuses, parks
// nothing: as for a result that decodes, RunService fails the Handle with the ctx's
// error and records nothing, and a recovered Handle runs the service again. MemStore's
// Append doesn't check ctx, so only RunService's own check keeps the park out of the
// journal.
func TestParkedResult_CanceledDuringService_RecordsNothing(t *testing.T) {
	ctx := context.Background()
	turnCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var runs int64
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		atomic.AddInt64(&runs, 1)
		cancel() // the turn's deadline passes while the call is in flight
		return scnUser{ID: 7, Name: "ann"}, nil
	}
	a := &scnAssigns{}
	m := scnMachine(fn, a)
	store := durable.NewMemStore()
	opts := []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)), failingDecoder()}
	h := scnStart(t, m, store, opts...)
	id := state.InvokeID(scnInvoke, "loading", 0)

	_, _, err := h.RunService(turnCtx, id)
	if !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("RunService = %v, want ErrHandleFailed wrapping context.Canceled", err)
	}
	if got := tailLen(t, store, "scn-1"); got != 0 {
		t.Fatalf("the journal holds %d Records, want none: the canceled turn's result was parked", got)
	}
	if a.done != 0 || a.fails != 0 {
		t.Fatalf("onDone's Assign ran %d times and onError's %d, want neither", a.done, a.fails)
	}
	rh, err := durable.Recover(ctx, m, store, "scn-1", opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	_, ran, err := rh.RunService(ctx, id)
	mustPark(t, "RunService after Recover", ran, err)
	if got := atomic.LoadInt64(&runs); got != 2 {
		t.Fatalf("the ServiceFn ran %d times, want 2: the canceled run recorded nothing", got)
	}
}

// The decoder is part of the Runner's options, so fixing it reaches only a Handle a
// Runner with the fix recovers. On the Handle that parked the result, every RunService
// returns ErrEventDataEncoding; Recover with the fixed decoder, then RunService, settles
// it.
func TestParkedResult_OnlyARecoveredHandleSeesTheFix(t *testing.T) {
	r := newParkRun(t)
	mustPark(t, "the first RunService", true, r.err)
	h, err := durable.Recover(context.Background(), r.m, r.store, "scn-1", r.reg, failingDecoder())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	id := state.InvokeID(scnInvoke, "loading", 0)
	for i := 0; i < 3; i++ {
		_, ran, rerr := h.RunService(context.Background(), id)
		mustPark(t, fmt.Sprintf("RunService %d on the same Handle", i), ran, rerr)
	}
	fixed, runErr, err := r.recoverAndRun(decoderFor("done", durable.DecodeAs[scnUser]))
	if err != nil || runErr != nil {
		t.Fatalf("Recover: %v, RunService: %v, with the fixed decoder", err, runErr)
	}
	if got := fixed.Instance().Current(); got != "ready" {
		t.Fatalf("in %q, want ready", got)
	}
}

// A parked result records its Go type with its import path, so a decoder for another
// package's type of the same short name (model.Result) can't settle it. Live, the same
// decoder is refused, since the live check compares reflect types.
func TestParkedResult_TypeCheckKeepsTheImportPath(t *testing.T) {
	ctx := context.Background()
	var runs int64
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		atomic.AddInt64(&runs, 1)
		return alpha.Result{ID: 7, Name: "ann"}, nil
	}
	id := state.InvokeID(scnInvoke, "loading", 0)
	asBeta := decoderFor("done", durable.DecodeAs[beta.Result])

	t.Run("live", func(t *testing.T) {
		a := &scnAssigns{}
		h := scnStart(t, scnMachine(fn, a), durable.NewMemStore(), durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)), asBeta)
		_, ran, err := h.RunService(ctx, id)
		mustPark(t, "RunService with beta's decoder", ran, err)
	})
	t.Run("parked", func(t *testing.T) {
		a := &scnAssigns{}
		m := scnMachine(fn, a)
		store := durable.NewMemStore()
		reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
		h := scnStart(t, m, store, reg, failingDecoder())
		_, ran, err := h.RunService(ctx, id)
		mustPark(t, "RunService", ran, err)
		rh, err := durable.Recover(ctx, m, store, "scn-1", reg, asBeta)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		_, ran, err = rh.RunService(ctx, id)
		mustPark(t, "RunService with beta's decoder", ran, err)
		if a.done != 0 || a.fails != 0 {
			t.Fatalf("an Assign ran: onDone %d (saw %v), onError %d", a.done, a.seen, a.fails)
		}
		rh, err = durable.Recover(ctx, m, store, "scn-1", reg, decoderFor("done", durable.DecodeAs[alpha.Result]))
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if _, _, err = rh.RunService(ctx, id); err != nil {
			t.Fatalf("RunService with alpha's decoder: %v", err)
		}
		if a.done != 1 || describe(a.seen[0]) != describe(alpha.Result{ID: 7, Name: "ann"}) {
			t.Fatalf("onDone's Assign ran %d times and saw %v, want alpha's Result once", a.done, a.seen)
		}
	})
}

// A result with no JSON form runs no Assign: RunService fails the Handle before the
// settle fires anything, records nothing, and a recovered Handle has the service in
// flight again. This holds with the default decoder and with one for onDone.
func TestServiceResult_NoJSONForm_RunsNoAssign(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []durable.Option[string, string, svcCtx]
	}{
		{"default decoder", nil},
		{"decoder for onDone", []durable.Option[string, string, svcCtx]{decoderFor("done", durable.DecodeAs[scnUser])}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var runs int64
			fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
				atomic.AddInt64(&runs, 1)
				return make(chan int), nil
			}
			a := &scnAssigns{}
			m := scnMachine(fn, a)
			store := durable.NewMemStore()
			opts := append([]durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))}, tc.opts...)
			h := scnStart(t, m, store, opts...)
			id := state.InvokeID(scnInvoke, "loading", 0)
			if _, _, err := h.RunService(ctx, id); !errors.Is(err, durable.ErrHandleFailed) {
				t.Fatalf("RunService = %v, want ErrHandleFailed", err)
			}
			if a.done != 0 || a.fails != 0 {
				t.Fatalf("onDone's Assign ran %d times and onError's %d, want neither", a.done, a.fails)
			}
			if got := tailLen(t, store, "scn-1"); got != 0 {
				t.Fatalf("the journal holds %d Records, want none", got)
			}
			rh, err := durable.Recover(ctx, m, store, "scn-1", opts...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if _, ran, err := rh.RunService(ctx, id); !ran || !errors.Is(err, durable.ErrHandleFailed) {
				t.Fatalf("RunService after Recover = %v, %v; want the service run again, failing the Handle", ran, err)
			}
			if got := atomic.LoadInt64(&runs); got != 2 {
				t.Fatalf("the ServiceFn ran %d times, want 2", got)
			}
			if a.done != 0 || a.fails != 0 {
				t.Fatalf("onDone's Assign ran %d times and onError's %d, want neither", a.done, a.fails)
			}
		})
	}
}

// unitBug's MarshalJSON writes milliseconds and its UnmarshalJSON reads seconds: an
// asymmetric pair, so decoding its own encoding scales the value a thousandfold.
type unitBug struct{ Seconds int64 }

func (u unitBug) MarshalJSON() ([]byte, error) { return json.Marshal(u.Seconds * 1000) }

func (u *unitBug) UnmarshalJSON(b []byte) error { return json.Unmarshal(b, &u.Seconds) }

// sealed encodes as its Token until it is decoded; a decoded sealed won't encode again,
// as a type that keeps a handle it can't serialize won't.
type sealed struct {
	Token   string
	decoded bool
}

func (s sealed) MarshalJSON() ([]byte, error) {
	if s.decoded {
		return nil, errors.New("a decoded sealed won't encode again")
	}
	return json.Marshal(s.Token)
}

func (s *sealed) UnmarshalJSON(b []byte) error {
	s.decoded = true
	return json.Unmarshal(b, &s.Token)
}

// Replay decodes the bytes the live settle decoded, so the Assign sees the same value
// live and after Recover even when the decoder's type doesn't survive a round trip
// through its own encoding: an asymmetric MarshalJSON/UnmarshalJSON pair, or a decoded
// value that won't encode again. Journaling the decoded value's encoding instead
// replays the pair's value scaled again, and fails the Handle for the second, after
// its Assign ran.
func TestServiceResult_ReplayDecodesWhatLiveDecoded(t *testing.T) {
	t.Run("asymmetric codec", func(t *testing.T) {
		a, runErr, recErr := scnRun(t, returning(unitBug{Seconds: 2}, nil), decoderFor("done", durable.DecodeAs[unitBug]))
		mustTwo(t, a, runErr, recErr)
		if got := a.seen[0].(unitBug); got.Seconds != 2000 {
			t.Fatalf("live saw %+v, want the 2000 its own encoding decodes to", got)
		}
	})
	t.Run("decoded value won't encode", func(t *testing.T) {
		a, runErr, recErr := scnRun(t, returning(sealed{Token: "t"}, nil), decoderFor("done", durable.DecodeAs[sealed]))
		mustTwo(t, a, runErr, recErr)
	})
	t.Run("int64 above 2^53, no decoder", func(t *testing.T) {
		a, runErr, recErr := scnRun(t, returning(int64(1<<53+1), nil))
		mustTwo(t, a, runErr, recErr)
		if got, ok := a.seen[0].(float64); !ok || got != float64(1<<53) {
			t.Fatalf("live saw %s, want the float64 9007199254740992", describe(a.seen[0]))
		}
	})
}

// panicky is a KindedEffect whose MarshalJSON panics.
type panicky struct{}

func (panicky) Kind() string                 { return "demo.panicky" }
func (panicky) MarshalJSON() ([]byte, error) { panic("marshal blew up") }

// panickyKind is a KindedEffect whose Kind panics.
type panickyKind struct{}

func (panickyKind) Kind() string { panic("kind blew up") }

func emitting(eff state.Effect) func(state.ActionCtx[svcCtx]) (state.Effect, error) {
	return func(state.ActionCtx[svcCtx]) (state.Effect, error) { return eff, nil }
}

func panickyMachine(eff state.Effect) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("pnk").
		Action("emit", emitting(eff)).
		State("idle").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("done").Do("emit").
		Quench()
}

// fireCatching fires event on h and returns the panic that escaped, or the error.
func fireCatching(h *durable.Handle[string, string, svcCtx], event string) (panicked any, err error) {
	defer func() { panicked = recover() }()
	_, err = h.Fire(context.Background(), event)
	return nil, err
}

// A KindedEffect whose MarshalJSON panics is recorded as one whose marshal fails: by id
// and kind, with the panic's message in Meta under crucible.marshalError. The step
// doesn't panic or fail, and Recover replays it.
func TestUnmarshalableEffect_PanickingMarshal_RecordsByIDAndKind(t *testing.T) {
	ctx := context.Background()
	m := panickyMachine(panicky{})
	store := durable.NewMemStore()
	h, err := durable.NewRunner(m, store).Start(ctx, "pnk-1", svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	p, ferr := fireCatching(h, "go")
	if p != nil || ferr != nil {
		t.Fatalf("Fire = %v, panic %v; want neither", ferr, p)
	}
	_, recs, err := store.Load(ctx, "pnk-1")
	if err != nil || len(recs) != 1 || len(recs[0].Effects) != 1 {
		t.Fatalf("Load = %d Records (err %v), want one with one effect", len(recs), err)
	}
	env := recs[0].Effects[0]
	if env.Kind != "demo.panicky" || env.EffectID != "0#0#demo.panicky" || len(env.Payload) != 0 {
		t.Fatalf("recorded %+v, want kind demo.panicky, id 0#0#demo.panicky and no payload", env)
	}
	if msg, _ := env.Meta[metaMarshalError].(string); !strings.Contains(msg, "marshal blew up") {
		t.Fatalf("Meta = %v, want the panic's message under %q", env.Meta, metaMarshalError)
	}
	rh, err := durable.Recover(ctx, m, store, "pnk-1")
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := rh.Instance().Current(); got != "done" {
		t.Fatalf("recovered in %q, want done", got)
	}
}

// Pins current behavior: a KindedEffect whose Kind panics has no kind to record it by,
// so it can't take the id-and-kind fallback. The panic leaves the Fire, and the Handle
// is failed first, as for any panic in host code during a step, so the caller can
// Recover. The kernel applied the transition, but nothing was journaled.
func TestUnmarshalableEffect_PanickingKind_FailsTheHandle(t *testing.T) {
	ctx := context.Background()
	m := panickyMachine(panickyKind{})
	store := durable.NewMemStore()
	h, err := durable.NewRunner(m, store).Start(ctx, "pnk-1", svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if p, _ := fireCatching(h, "go"); p == nil || !strings.Contains(fmt.Sprint(p), "kind blew up") {
		t.Fatalf("Fire panicked with %v, want the Kind panic", p)
	}
	if _, err := h.Fire(ctx, "go"); !errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("Fire after the panic = %v, want ErrHandleFailed", err)
	}
	if got := tailLen(t, store, "pnk-1"); got != 0 {
		t.Fatalf("the journal holds %d Records, want none", got)
	}
}
