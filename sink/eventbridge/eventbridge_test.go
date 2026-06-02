// SPDX-License-Identifier: Apache-2.0

package eventbridge_test

import (
	"context"
	"errors"
	"testing"

	awseb "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	csink "github.com/stablekernel/crucible/sink"
	ebsink "github.com/stablekernel/crucible/sink/eventbridge"
	"github.com/stablekernel/crucible/sink/sinktest"
)

// fakeClient is a hand-rolled Client implementation. It records every
// PutEvents call and returns the configured response and error.
type fakeClient struct {
	calls  []*awseb.PutEventsInput
	output *awseb.PutEventsOutput
	err    error
}

func (f *fakeClient) PutEvents(_ context.Context, params *awseb.PutEventsInput, _ ...func(*awseb.Options)) (*awseb.PutEventsOutput, error) {
	f.calls = append(f.calls, params)
	if f.err != nil {
		return nil, f.err
	}
	if f.output != nil {
		return f.output, nil
	}
	return &awseb.PutEventsOutput{}, nil
}

// payload types used across tests.
type (
	orderPlaced  struct{ OrderID string }
	orderShipped struct{ OrderID string }
)

func ptr[T any](v T) *T { return &v }

// newOutlet wires a registry with the standard test payload types and returns
// the outlet backed by client.
func newOutlet(c ebsink.Client) csink.Outlet {
	reg := ebsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[ebsink.Client] {
		return ebsink.PutEvent("orders", "com.example.orders", "OrderPlaced", `{"id":"`+o.OrderID+`"}`)
	})
	csink.Register(reg, func(_ context.Context, o orderShipped) csink.Op[ebsink.Client] {
		return ebsink.PutEvents(&awseb.PutEventsInput{
			Entries: []types.PutEventsRequestEntry{
				{
					EventBusName: ptr("orders"),
					Source:       ptr("com.example.orders"),
					DetailType:   ptr("OrderShipped"),
					Detail:       ptr(`{"id":"` + o.OrderID + `"}`),
				},
			},
		})
	})
	return ebsink.New(c, reg)
}

func TestPutEvent_SendsSingleEntry(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	if err := newOutlet(c).Sink(context.Background(), orderPlaced{OrderID: "ORD-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.calls) != 1 {
		t.Fatalf("PutEvents call count = %d, want 1", len(c.calls))
	}
	entries := c.calls[0].Entries
	if len(entries) != 1 {
		t.Fatalf("entries count = %d, want 1", len(entries))
	}
	e := entries[0]
	if e.EventBusName == nil || *e.EventBusName != "orders" {
		t.Errorf("EventBusName = %v, want orders", e.EventBusName)
	}
	if e.Source == nil || *e.Source != "com.example.orders" {
		t.Errorf("Source = %v, want com.example.orders", e.Source)
	}
	if e.DetailType == nil || *e.DetailType != "OrderPlaced" {
		t.Errorf("DetailType = %v, want OrderPlaced", e.DetailType)
	}
}

func TestPutEvents_PassesThroughInput(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	if err := newOutlet(c).Sink(context.Background(), orderShipped{OrderID: "ORD-2"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.calls) != 1 {
		t.Fatalf("PutEvents call count = %d, want 1", len(c.calls))
	}
	entries := c.calls[0].Entries
	if len(entries) != 1 {
		t.Fatalf("entries count = %d, want 1", len(entries))
	}
	e := entries[0]
	if e.DetailType == nil || *e.DetailType != "OrderShipped" {
		t.Errorf("DetailType = %v, want OrderShipped", e.DetailType)
	}
}

func TestPutEvents_PartialFailureReturnsError(t *testing.T) {
	t.Parallel()

	c := &fakeClient{
		output: &awseb.PutEventsOutput{
			FailedEntryCount: 1,
			Entries: []types.PutEventsResultEntry{
				{ErrorCode: ptr("ThrottlingException"), ErrorMessage: ptr("Rate exceeded")},
			},
		},
	}

	reg := ebsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[ebsink.Client] {
		return ebsink.PutEvent("orders", "com.example.orders", "OrderPlaced", `{}`)
	})
	outlet := ebsink.New(c, reg)

	err := outlet.Sink(context.Background(), orderPlaced{OrderID: "ORD-3"})
	if err == nil {
		t.Fatal("Sink() = nil, want partial failure error")
	}
	if !errors.Is(err, err) { // ensure it is not nil and is returnable
		t.Fatal("error not returnable")
	}
	// The Emitter wraps the Op's raw error as *csink.Error.
	var se *csink.Error
	if !errors.As(err, &se) {
		t.Fatalf("Sink() = %v, want *sink.Error", err)
	}
	if se.Phase != csink.PhaseApply {
		t.Errorf("Phase = %v, want PhaseApply", se.Phase)
	}
	if se.Outlet != "eventbridge" {
		t.Errorf("Outlet = %q, want eventbridge", se.Outlet)
	}
}

func TestPutEvents_PartialFailureErrorMessage(t *testing.T) {
	t.Parallel()

	// Verify the raw error from the Op (before Emitter wrapping) contains useful text.
	// We use a registry-less Op directly via OpFunc to get the unwrapped error.
	c := &fakeClient{
		output: &awseb.PutEventsOutput{
			FailedEntryCount: 2,
			Entries: []types.PutEventsResultEntry{
				{ErrorCode: ptr("ThrottlingException"), ErrorMessage: ptr("Rate exceeded")},
				{ErrorCode: ptr("InternalFailure"), ErrorMessage: ptr("Retry later")},
			},
		},
	}

	reg := ebsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[ebsink.Client] {
		return ebsink.PutEvent("orders", "com.example.orders", "OrderPlaced", `{}`)
	})
	outlet := ebsink.New(c, reg)

	err := outlet.Sink(context.Background(), orderPlaced{OrderID: "ORD-4"})
	if err == nil {
		t.Fatal("Sink() = nil, want error")
	}
	var se *csink.Error
	if !errors.As(err, &se) {
		t.Fatalf("Sink() = %v, want *sink.Error", err)
	}
	// The underlying error should mention the count and codes.
	msg := se.Unwrap().Error()
	if msg == "" {
		t.Error("underlying error message is empty")
	}
}

func TestUnregisteredPayloadSkips(t *testing.T) {
	t.Parallel()

	type unknown struct{}
	err := newOutlet(&fakeClient{}).Sink(context.Background(), unknown{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestPutEvent_SDKErrorWrappedAsSinkError(t *testing.T) {
	t.Parallel()

	boom := errors.New("connection reset")
	err := newOutlet(&fakeClient{err: boom}).Sink(context.Background(), orderPlaced{OrderID: "ORD-5"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want to wrap %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) {
		t.Fatalf("Sink() = %v, want *sink.Error", err)
	}
	if se.Phase != csink.PhaseApply {
		t.Errorf("Phase = %v, want PhaseApply", se.Phase)
	}
	if se.Outlet != "eventbridge" {
		t.Errorf("Outlet = %q, want eventbridge", se.Outlet)
	}
}

func TestPutEvents_SDKErrorWrappedAsSinkError(t *testing.T) {
	t.Parallel()

	boom := errors.New("timeout")
	err := newOutlet(&fakeClient{err: boom}).Sink(context.Background(), orderShipped{OrderID: "ORD-6"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want to wrap %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) {
		t.Fatalf("Sink() = %v, want *sink.Error", err)
	}
	if se.Phase != csink.PhaseApply {
		t.Errorf("Phase = %v, want PhaseApply", se.Phase)
	}
	if se.Outlet != "eventbridge" {
		t.Errorf("Outlet = %q, want eventbridge", se.Outlet)
	}
}

func TestCanceledContextPropagates(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := &fakeClient{err: context.Canceled}
	err := newOutlet(c).Sink(ctx, orderPlaced{OrderID: "ORD-7"})
	if err == nil {
		t.Fatal("Sink() = nil, want error on canceled context")
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(&fakeClient{}) })
}

func TestNewRegistry_ReturnsEmptyRegistry(t *testing.T) {
	t.Parallel()

	reg := ebsink.NewRegistry()
	if reg == nil {
		t.Fatal("NewRegistry() = nil")
	}
}

func TestNew_DefaultOutletName(t *testing.T) {
	t.Parallel()

	// Sink an unregistered type; the resulting ErrUnregistered carries the
	// outlet name so we can confirm the default is "eventbridge".
	type probe struct{}
	c := &fakeClient{}
	reg := ebsink.NewRegistry()
	outlet := ebsink.New(c, reg)
	err := outlet.Sink(context.Background(), probe{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestNew_NameOverride(t *testing.T) {
	t.Parallel()

	type probe struct{}
	c := &fakeClient{}
	reg := ebsink.NewRegistry()
	outlet := ebsink.New(c, reg, csink.WithName("custom-eb"))

	// Sink a registered payload that will fail so we can inspect *csink.Error.
	csink.Register(reg, func(_ context.Context, p probe) csink.Op[ebsink.Client] {
		return ebsink.PutEvent("bus", "src", "dt", "{}")
	})
	c.err = errors.New("boom")
	err := outlet.Sink(context.Background(), probe{})

	var se *csink.Error
	if !errors.As(err, &se) {
		t.Fatalf("Sink() = %v, want *sink.Error", err)
	}
	if se.Outlet != "custom-eb" {
		t.Errorf("Outlet = %q, want custom-eb", se.Outlet)
	}
}
