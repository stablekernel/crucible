package durable_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

type tnQuote struct{ Price int }

// TestRunService_ATypedNilResultKeepsItsType answers a review finding: a ServiceFn
// that returned a typed nil pointer reached onDone's Assign as an untyped nil, though a
// decoder for its onDone event decodes the pointer type, because a null result never
// reached the decoder. On main, durable passed the ServiceFn's own value live, so the
// Assign saw a (*tnQuote)(nil), and an untyped nil on replay. Now a typed nil result's
// null goes through the decoder, live and on replay alike, so DecodeAs[*tnQuote] hands
// the Assign a (*tnQuote)(nil) both times.
func TestRunService_ATypedNilResultKeepsItsType(t *testing.T) {
	a, runErr, recErr := scnRun(t, returning((*tnQuote)(nil), nil), decoderFor("done", durable.DecodeAs[*tnQuote]))
	mustTwo(t, a, runErr, recErr)
	for i, when := range []string{"live", "on replay"} {
		q, ok := a.seen[i].(*tnQuote)
		if !ok || q != nil {
			t.Fatalf("%s, onDone's Assign saw %s, want a nil *tnQuote", when, describe(a.seen[i]))
		}
	}
}

// TestRunService_ATypedNilResultWithNoDecoderIsNil pins the default: with no decoder
// for its onDone event, a typed nil result arrives in JSON's generic form for null, an
// untyped nil, live and on replay.
func TestRunService_ATypedNilResultWithNoDecoderIsNil(t *testing.T) {
	a, runErr, recErr := scnRun(t, returning((*tnQuote)(nil), nil))
	mustTwo(t, a, runErr, recErr)
	if a.seen[0] != nil {
		t.Fatalf("onDone's Assign saw %s, want an untyped nil", describe(a.seen[0]))
	}
}

// TestRunService_AnUntypedNilResultStaysNil pins that an untyped nil result arrives as
// an untyped nil, live and on replay, even with a decoder for its onDone event, which
// it doesn't reach.
func TestRunService_AnUntypedNilResultStaysNil(t *testing.T) {
	called := 0
	dec := func(data []byte) (any, error) {
		called++
		return durable.DecodeAs[*tnQuote](data)
	}
	a, runErr, recErr := scnRun(t, returning(nil, nil), decoderFor("done", dec))
	mustTwo(t, a, runErr, recErr)
	if a.seen[0] != nil || called != 0 {
		t.Fatalf("onDone's Assign saw %s, and the decoder ran %d times; want an untyped nil and no decode", describe(a.seen[0]), called)
	}
}

// TestRunService_AnUnboundServiceErrorIsMessageOnly pins a documented limit: errors are
// message-only, so onError's Assign sees a plain error with the message of an unbound
// service's *state.UnboundRefError, which errors.As doesn't match, live or on replay.
func TestRunService_AnUnboundServiceErrorIsMessageOnly(t *testing.T) {
	ctx := context.Background()
	a := &scnAssigns{}
	m := scnMachine(returning(nil, nil), a)
	store := durable.NewMemStore()
	// The registry binds no "fetch", so the service settles as unbound.
	opt := durable.WithServiceRegistry[string, string, svcCtx](state.NewRegistry[svcCtx]())
	h := scnStart(t, m, store, opt)
	if _, _, err := h.RunService(ctx, state.InvokeID(scnInvoke, "loading", 0)); err != nil {
		t.Fatalf("RunService: %v", err)
	}
	if _, err := durable.Recover(ctx, m, store, "scn-1", opt); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if a.fails != 2 || len(a.seen) != 2 {
		t.Fatalf("onError's Assign ran %d times (%v), want 2: live and on replay", a.fails, a.seen)
	}
	want := (&state.UnboundRefError{Kind: "service", Name: "fetch"}).Error()
	for i, when := range []string{"live", "on replay"} {
		err, ok := a.seen[i].(error)
		if !ok || err.Error() != want {
			t.Fatalf("%s, onError's Assign saw %s, want an error saying %q", when, describe(a.seen[i]), want)
		}
		var unbound *state.UnboundRefError
		if errors.As(err, &unbound) {
			t.Fatalf("%s, errors.As matched an *UnboundRefError, though errors are message-only", when)
		}
	}
}
