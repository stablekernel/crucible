// SPDX-License-Identifier: Apache-2.0

package sink_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/sink"
)

func TestEmitterAppliesRegisteredOp(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	reg := sink.NewRegistry[sink.Op[*fakeClient]]()
	sink.Register(reg, func(_ context.Context, p payloadA) sink.Op[*fakeClient] {
		return sink.OpFunc[*fakeClient](func(_ context.Context, client *fakeClient) error {
			client.applied = append(client.applied, "A")
			return nil
		})
	})
	e := sink.NewEmitter[*fakeClient](c, reg, sink.WithName("fake"))

	if err := e.Sink(context.Background(), payloadA{N: 1}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.applied) != 1 || c.applied[0] != "A" {
		t.Fatalf("client.applied = %v, want [A]", c.applied)
	}
}

func TestEmitterUnregisteredReturnsErrUnregistered(t *testing.T) {
	t.Parallel()

	reg := sink.NewRegistry[sink.Op[*fakeClient]]()
	e := sink.NewEmitter[*fakeClient](&fakeClient{}, reg)
	if err := e.Sink(context.Background(), payloadB{}); !errors.Is(err, sink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestEmitterWrapsApplyError(t *testing.T) {
	t.Parallel()

	boom := errors.New("write failed")
	reg := sink.NewRegistry[sink.Op[*fakeClient]]()
	sink.Register(reg, func(_ context.Context, p payloadA) sink.Op[*fakeClient] {
		return sink.OpFunc[*fakeClient](func(context.Context, *fakeClient) error { return boom })
	})
	e := sink.NewEmitter[*fakeClient](&fakeClient{}, reg, sink.WithName("fake"))

	err := e.Sink(context.Background(), payloadA{})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *sink.Error
	if !errors.As(err, &se) || se.Phase != sink.PhaseApply || se.Outlet != "fake" {
		t.Fatalf("recovered = %+v, want *sink.Error{Outlet:fake, Phase:apply}", se)
	}
}

func TestEmitterIsOutlet(t *testing.T) {
	t.Parallel()
	var _ sink.Outlet = sink.NewEmitter[*fakeClient](&fakeClient{}, sink.NewRegistry[sink.Op[*fakeClient]]())
}
