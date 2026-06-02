// SPDX-License-Identifier: Apache-2.0

package nats_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	gonats "github.com/nats-io/nats.go"
	csink "github.com/stablekernel/crucible/sink"
	natssink "github.com/stablekernel/crucible/sink/nats"
	"github.com/stablekernel/crucible/sink/sinktest"
)

// fakeClient is a hand-rolled Client implementation — no live NATS server.
type fakeClient struct {
	published []publishCall
	msgs      []*gonats.Msg
	err       error
}

type publishCall struct {
	subject string
	data    []byte
}

func (f *fakeClient) Publish(subject string, data []byte) error {
	f.published = append(f.published, publishCall{subject: subject, data: data})
	return f.err
}

func (f *fakeClient) PublishMsg(m *gonats.Msg) error {
	f.msgs = append(f.msgs, m)
	return f.err
}

// orderShipped is a test payload type.
type orderShipped struct{ ID string }

func newOutlet(c natssink.Client) csink.Outlet {
	reg := natssink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderShipped) csink.Op[natssink.Client] {
		return natssink.Publish("orders.shipped", []byte(o.ID))
	})
	return natssink.New(c, reg)
}

func TestPublish_SendsToSubject(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	if err := newOutlet(c).Sink(context.Background(), orderShipped{ID: "B-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.published) != 1 {
		t.Fatalf("published count = %d, want 1", len(c.published))
	}
	if c.published[0].subject != "orders.shipped" {
		t.Fatalf("subject = %q, want %q", c.published[0].subject, "orders.shipped")
	}
	if !bytes.Equal(c.published[0].data, []byte("B-1")) {
		t.Fatalf("data = %q, want %q", c.published[0].data, "B-1")
	}
}

func TestPublishMsg_SendsMsg(t *testing.T) {
	t.Parallel()

	type msgPayload struct{ body string }
	c := &fakeClient{}
	reg := natssink.NewRegistry()
	csink.Register(reg, func(_ context.Context, p msgPayload) csink.Op[natssink.Client] {
		return natssink.PublishMsg(&gonats.Msg{
			Subject: "events.raw",
			Data:    []byte(p.body),
		})
	})
	outlet := natssink.New(c, reg)
	if err := outlet.Sink(context.Background(), msgPayload{body: "hello"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.msgs) != 1 {
		t.Fatalf("msgs count = %d, want 1", len(c.msgs))
	}
	if c.msgs[0].Subject != "events.raw" {
		t.Fatalf("msg.Subject = %q, want %q", c.msgs[0].Subject, "events.raw")
	}
	if !bytes.Equal(c.msgs[0].Data, []byte("hello")) {
		t.Fatalf("msg.Data = %q, want %q", c.msgs[0].Data, "hello")
	}
}

func TestUnregisteredPayloadSkips(t *testing.T) {
	t.Parallel()

	type other struct{}
	err := newOutlet(&fakeClient{}).Sink(context.Background(), other{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestPublishError_WrappedAsApplyError(t *testing.T) {
	t.Parallel()

	boom := errors.New("nats: connection closed")
	err := newOutlet(&fakeClient{err: boom}).Sink(context.Background(), orderShipped{ID: "B-2"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "nats" {
		t.Fatalf("recovered = %+v, want *sink.Error{Outlet:nats, Phase:apply}", se)
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(&fakeClient{}) })
}
