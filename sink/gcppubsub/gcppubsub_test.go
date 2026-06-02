// SPDX-License-Identifier: Apache-2.0

package gcppubsub_test

import (
	"context"
	"errors"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/gcppubsub"
	"github.com/stablekernel/crucible/sink/sinktest"
)

// fakePublisher is a hand-rolled Publisher — no live GCP, no emulator, no
// network, no mockery.
type fakePublisher struct {
	calls []publishCall
	id    string
	err   error
}

type publishCall struct {
	data  []byte
	attrs map[string]string
}

func (f *fakePublisher) Publish(_ context.Context, data []byte, attrs map[string]string) (string, error) {
	f.calls = append(f.calls, publishCall{data: data, attrs: attrs})
	if f.err != nil {
		return "", f.err
	}
	return f.id, nil
}

type orderPlaced struct{ ID string }

func newOutlet(pub gcppubsub.Publisher) csink.Outlet {
	reg := gcppubsub.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[gcppubsub.Publisher] {
		return gcppubsub.Publish([]byte(o.ID), map[string]string{"type": "orderPlaced"})
	})
	return gcppubsub.New(pub, reg)
}

func TestPublishSendsMessage(t *testing.T) {
	t.Parallel()

	pub := &fakePublisher{id: "srv-1"}
	if err := newOutlet(pub).Sink(context.Background(), orderPlaced{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(pub.calls) != 1 {
		t.Fatalf("Publish calls = %d, want 1", len(pub.calls))
	}
	if string(pub.calls[0].data) != "A-1" {
		t.Fatalf("data = %q, want A-1", pub.calls[0].data)
	}
	if pub.calls[0].attrs["type"] != "orderPlaced" {
		t.Fatalf("attrs = %v, want type=orderPlaced", pub.calls[0].attrs)
	}
}

func TestPublishNilAttrs(t *testing.T) {
	t.Parallel()

	pub := &fakePublisher{}
	reg := gcppubsub.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[gcppubsub.Publisher] {
		return gcppubsub.Publish([]byte(o.ID), nil)
	})
	if err := gcppubsub.New(pub, reg).Sink(context.Background(), orderPlaced{ID: "B-2"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(pub.calls) != 1 || pub.calls[0].attrs != nil {
		t.Fatalf("calls = %+v, want one call with nil attrs", pub.calls)
	}
}

func TestUnregisteredPayloadSkips(t *testing.T) {
	t.Parallel()

	type other struct{}
	err := newOutlet(&fakePublisher{}).Sink(context.Background(), other{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestPublishErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("topic not found")
	err := newOutlet(&fakePublisher{err: boom}).Sink(context.Background(), orderPlaced{ID: "A-3"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "gcppubsub" {
		t.Fatalf("recovered = %+v, want *sink.Error{Outlet:gcppubsub, Phase:apply}", se)
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(&fakePublisher{}) })
}
