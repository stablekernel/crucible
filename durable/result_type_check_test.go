package durable_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests compare the type check a live service result passes with the one a
// Fire's WithEventData payload passes. Both decode through the Runner's
// EventDataDecoder; they differ in what they accept back.

type tcOrder struct {
	ID    int64
	Items []string
}

// useNumber decodes a payload as JSON's generic form, keeping numbers as json.Number
// so an integer keeps its precision.
func useNumber(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// fireDone fires "done" on a fresh scn instance with payload, and returns the error.
func fireDone(t *testing.T, payload any, opts ...durable.Option[string, string, svcCtx]) error {
	t.Helper()
	fn := returning(nil, nil)
	m := scnMachine(fn, &scnAssigns{})
	opts = append([]durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))}, opts...)
	h := scnStart(t, m, durable.NewMemStore(), opts...)
	_, err := h.Fire(context.Background(), "done", state.WithEventData(payload))
	return err
}

// TestTypeCheck_NoDecoder_ServiceResultIsAMap_FirePayloadIsRefused pins the documented
// asymmetry: with no decoder, a struct service result reaches Assign as JSON's generic
// form, a map, while a Fire carrying the same struct is refused. A Fire is refused
// before the kernel runs, so its caller can add a decoder and fire again; a service has
// already run, so refusing its result would only fail the step and run it again.
func TestTypeCheck_NoDecoder_ServiceResultIsAMap_FirePayloadIsRefused(t *testing.T) {
	order := tcOrder{ID: 7, Items: []string{"tea"}}
	a, runErr, recErr := scnRun(t, returning(order, nil))
	mustTwo(t, a, runErr, recErr)
	if _, ok := a.seen[0].(map[string]any); !ok {
		t.Fatalf("the service result reached Assign as %s, want map[string]any", describe(a.seen[0]))
	}
	if err := fireDone(t, order); !errors.Is(err, durable.ErrEventDataEncoding) {
		t.Fatalf("Fire with the same struct: %v, want ErrEventDataEncoding", err)
	}
}

// TestTypeCheck_UseNumberDecoder_ServiceResult proves a decoder that keeps numbers as
// json.Number, a generic form, is accepted for a service result whatever its shape: an
// int arrives as a json.Number, as the same decoder's struct result arrives as a map
// holding json.Numbers. Before the fix the int was refused and the struct accepted.
func TestTypeCheck_UseNumberDecoder_ServiceResult(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		a, runErr, recErr := scnRun(t, returning(42, nil), decoderFor("done", useNumber))
		mustTwo(t, a, runErr, recErr)
		if n, ok := a.seen[0].(json.Number); !ok || n.String() != "42" {
			t.Fatalf("Assign saw %s, want json.Number 42", describe(a.seen[0]))
		}
	})
	t.Run("struct", func(t *testing.T) {
		a, runErr, recErr := scnRun(t, returning(tcOrder{ID: 7}, nil), decoderFor("done", useNumber))
		mustTwo(t, a, runErr, recErr)
		m, ok := a.seen[0].(map[string]any)
		if !ok {
			t.Fatalf("Assign saw %s, want map[string]any", describe(a.seen[0]))
		}
		if _, ok := m["ID"].(json.Number); !ok {
			t.Fatalf("the map's ID is %s, want json.Number", describe(m["ID"]))
		}
	})
}

// TestTypeCheck_UseNumberDecoder_FirePayloadIsRefused pins that a Fire stays strict: an
// int payload decoded as a json.Number isn't the type its caller passed, so the Fire is
// refused, as one decoded as a map is.
func TestTypeCheck_UseNumberDecoder_FirePayloadIsRefused(t *testing.T) {
	if err := fireDone(t, 42, decoderFor("done", useNumber)); !errors.Is(err, durable.ErrEventDataEncoding) {
		t.Fatalf("Fire with an int and a json.Number decoder: %v, want ErrEventDataEncoding", err)
	}
	if err := fireDone(t, tcOrder{ID: 7}, decoderFor("done", useNumber)); !errors.Is(err, durable.ErrEventDataEncoding) {
		t.Fatalf("Fire with a struct and a json.Number decoder: %v, want ErrEventDataEncoding", err)
	}
}
