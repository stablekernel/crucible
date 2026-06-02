// SPDX-License-Identifier: Apache-2.0

package redis_test

import (
	"context"
	"errors"
	"testing"

	goredisp "github.com/redis/go-redis/v9"
	csink "github.com/stablekernel/crucible/sink"
	redissink "github.com/stablekernel/crucible/sink/redis"
	"github.com/stablekernel/crucible/sink/sinktest"
)

// fakeClient is a hand-rolled Client implementation — no live Redis, no
// mockery. It records calls and can inject a canned error.
type fakeClient struct {
	xaddCalls    []xaddCall
	publishCalls []publishCall
	err          error
}

type xaddCall struct {
	stream string
	values map[string]any
}

type publishCall struct {
	channel string
	message any
}

func (f *fakeClient) XAdd(_ context.Context, a *goredisp.XAddArgs) *goredisp.StringCmd {
	f.xaddCalls = append(f.xaddCalls, xaddCall{
		stream: a.Stream,
		values: asMap(a.Values),
	})
	return goredisp.NewStringResult("", f.err)
}

func (f *fakeClient) Publish(_ context.Context, channel string, message any) *goredisp.IntCmd {
	f.publishCalls = append(f.publishCalls, publishCall{channel: channel, message: message})
	return goredisp.NewIntResult(0, f.err)
}

// asMap asserts the values stored in the op are map[string]any, which is what
// XAdd constructs.
func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}

// ---- payload types used across tests ----------------------------------------

type eventOccurred struct {
	ID   string
	Kind string
}

type notificationSent struct {
	Channel string
	Body    string
}

// ---- helpers ----------------------------------------------------------------

func newOutletWithClient(c redissink.Client) csink.Outlet {
	reg := redissink.NewRegistry()
	csink.Register(reg, func(_ context.Context, e eventOccurred) csink.Op[redissink.Client] {
		return redissink.XAdd("events", map[string]any{"id": e.ID, "kind": e.Kind})
	})
	csink.Register(reg, func(_ context.Context, n notificationSent) csink.Op[redissink.Client] {
		return redissink.Publish(n.Channel, n.Body)
	})
	return redissink.New(c, reg)
}

// ---- XAdd tests -------------------------------------------------------------

func TestXAdd_WritesToStream(t *testing.T) {
	t.Parallel()

	fc := &fakeClient{}
	outlet := newOutletWithClient(fc)

	if err := outlet.Sink(context.Background(), eventOccurred{ID: "evt-1", Kind: "order.placed"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	if len(fc.xaddCalls) != 1 {
		t.Fatalf("XAdd call count = %d, want 1", len(fc.xaddCalls))
	}
	got := fc.xaddCalls[0]
	if got.stream != "events" {
		t.Errorf("stream = %q, want %q", got.stream, "events")
	}
	if got.values["id"] != "evt-1" || got.values["kind"] != "order.placed" {
		t.Errorf("values = %v, want id=evt-1 kind=order.placed", got.values)
	}
}

func TestXAdd_ErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("stream full")
	fc := &fakeClient{err: boom}
	outlet := newOutletWithClient(fc)

	err := outlet.Sink(context.Background(), eventOccurred{ID: "evt-2", Kind: "order.placed"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want to wrap %v", err, boom)
	}

	var se *csink.Error
	if !errors.As(err, &se) {
		t.Fatalf("Sink() error is not *csink.Error: %T", err)
	}
	if se.Phase != csink.PhaseApply {
		t.Errorf("Phase = %q, want %q", se.Phase, csink.PhaseApply)
	}
	if se.Outlet != "redis" {
		t.Errorf("Outlet = %q, want %q", se.Outlet, "redis")
	}
}

// ---- Publish tests ----------------------------------------------------------

func TestPublish_PostsToChannel(t *testing.T) {
	t.Parallel()

	fc := &fakeClient{}
	outlet := newOutletWithClient(fc)

	if err := outlet.Sink(context.Background(), notificationSent{Channel: "alerts", Body: "hello"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	if len(fc.publishCalls) != 1 {
		t.Fatalf("Publish call count = %d, want 1", len(fc.publishCalls))
	}
	got := fc.publishCalls[0]
	if got.channel != "alerts" {
		t.Errorf("channel = %q, want %q", got.channel, "alerts")
	}
	if got.message != "hello" {
		t.Errorf("message = %v, want %q", got.message, "hello")
	}
}

func TestPublish_ErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("connection refused")
	fc := &fakeClient{err: boom}
	outlet := newOutletWithClient(fc)

	err := outlet.Sink(context.Background(), notificationSent{Channel: "alerts", Body: "hi"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want to wrap %v", err, boom)
	}

	var se *csink.Error
	if !errors.As(err, &se) {
		t.Fatalf("Sink() error is not *csink.Error: %T", err)
	}
	if se.Phase != csink.PhaseApply {
		t.Errorf("Phase = %q, want %q", se.Phase, csink.PhaseApply)
	}
	if se.Outlet != "redis" {
		t.Errorf("Outlet = %q, want %q", se.Outlet, "redis")
	}
}

// ---- Unregistered skip ------------------------------------------------------

func TestUnregistered_Skips(t *testing.T) {
	t.Parallel()

	type unknownEvent struct{ Data string }
	fc := &fakeClient{}
	outlet := newOutletWithClient(fc)

	err := outlet.Sink(context.Background(), unknownEvent{Data: "ignored"})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

// ---- Conformance ------------------------------------------------------------

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet {
		return newOutletWithClient(&fakeClient{})
	})
}
