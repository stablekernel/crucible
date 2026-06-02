// SPDX-License-Identifier: Apache-2.0

package sns_test

import (
	"context"
	"errors"
	"testing"

	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/sinktest"
	snssink "github.com/stablekernel/crucible/sink/sns"
)

// fakeClient is a hand-rolled Client implementation. It records every Publish
// and PublishBatch call and returns the configured error (nil by default).
type fakeClient struct {
	publishCalls      []*awssns.PublishInput
	publishBatchCalls []*awssns.PublishBatchInput
	err               error
}

func (f *fakeClient) Publish(_ context.Context, params *awssns.PublishInput, _ ...func(*awssns.Options)) (*awssns.PublishOutput, error) {
	f.publishCalls = append(f.publishCalls, params)
	if f.err != nil {
		return nil, f.err
	}
	return &awssns.PublishOutput{}, nil
}

func (f *fakeClient) PublishBatch(_ context.Context, params *awssns.PublishBatchInput, _ ...func(*awssns.Options)) (*awssns.PublishBatchOutput, error) {
	f.publishBatchCalls = append(f.publishBatchCalls, params)
	if f.err != nil {
		return nil, f.err
	}
	return &awssns.PublishBatchOutput{}, nil
}

// payload types used across tests.
type (
	orderShipped  struct{ OrderID string }
	orderCanceled struct{ OrderID string }
)

func ptr[T any](v T) *T { return &v }

// newOutlet wires a registry with both payload types and returns the outlet.
func newOutlet(c snssink.Client) csink.Outlet {
	reg := snssink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderShipped) csink.Op[snssink.Client] {
		return snssink.Publish("arn:aws:sns:us-east-1:123456789012:orders", "shipped:"+o.OrderID)
	})
	csink.Register(reg, func(_ context.Context, o orderCanceled) csink.Op[snssink.Client] {
		return snssink.PublishBatch(&awssns.PublishBatchInput{
			TopicArn: ptr("arn:aws:sns:us-east-1:123456789012:orders"),
			PublishBatchRequestEntries: []types.PublishBatchRequestEntry{
				{Id: ptr("1"), Message: ptr("canceled:" + o.OrderID)},
			},
		})
	})
	return snssink.New(c, reg)
}

func TestPublish_SendsMessageToTopic(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	if err := newOutlet(c).Sink(context.Background(), orderShipped{OrderID: "ORD-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.publishCalls) != 1 {
		t.Fatalf("Publish call count = %d, want 1", len(c.publishCalls))
	}
	got := c.publishCalls[0]
	if got.TopicArn == nil || *got.TopicArn != "arn:aws:sns:us-east-1:123456789012:orders" {
		t.Errorf("TopicArn = %v, want orders ARN", got.TopicArn)
	}
	if got.Message == nil || *got.Message != "shipped:ORD-1" {
		t.Errorf("Message = %v, want shipped:ORD-1", got.Message)
	}
}

func TestPublishInput_PassesThroughInput(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	reg := snssink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderShipped) csink.Op[snssink.Client] {
		return snssink.PublishInput(&awssns.PublishInput{
			TopicArn: ptr("arn:aws:sns:us-east-1:123456789012:events"),
			Message:  ptr("event:" + o.OrderID),
			Subject:  ptr("Order Shipped"),
		})
	})
	outlet := snssink.New(c, reg)

	if err := outlet.Sink(context.Background(), orderShipped{OrderID: "ORD-2"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.publishCalls) != 1 {
		t.Fatalf("Publish call count = %d, want 1", len(c.publishCalls))
	}
	got := c.publishCalls[0]
	if got.Subject == nil || *got.Subject != "Order Shipped" {
		t.Errorf("Subject = %v, want 'Order Shipped'", got.Subject)
	}
}

func TestPublishBatch_SendsBatchToTopic(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	if err := newOutlet(c).Sink(context.Background(), orderCanceled{OrderID: "ORD-3"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.publishBatchCalls) != 1 {
		t.Fatalf("PublishBatch call count = %d, want 1", len(c.publishBatchCalls))
	}
	got := c.publishBatchCalls[0]
	if got.TopicArn == nil || *got.TopicArn != "arn:aws:sns:us-east-1:123456789012:orders" {
		t.Errorf("TopicArn = %v, want orders ARN", got.TopicArn)
	}
	if len(got.PublishBatchRequestEntries) != 1 {
		t.Fatalf("entries count = %d, want 1", len(got.PublishBatchRequestEntries))
	}
	entry := got.PublishBatchRequestEntries[0]
	if entry.Message == nil || *entry.Message != "canceled:ORD-3" {
		t.Errorf("entry message = %v, want canceled:ORD-3", entry.Message)
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

func TestPublish_ErrorWrappedAsSinkError(t *testing.T) {
	t.Parallel()

	boom := errors.New("throttled")
	err := newOutlet(&fakeClient{err: boom}).Sink(context.Background(), orderShipped{OrderID: "ORD-4"})
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
	if se.Outlet != "sns" {
		t.Errorf("Outlet = %q, want sns", se.Outlet)
	}
}

func TestPublishBatch_ErrorWrappedAsSinkError(t *testing.T) {
	t.Parallel()

	boom := errors.New("batch throttled")
	err := newOutlet(&fakeClient{err: boom}).Sink(context.Background(), orderCanceled{OrderID: "ORD-5"})
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
	if se.Outlet != "sns" {
		t.Errorf("Outlet = %q, want sns", se.Outlet)
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(&fakeClient{}) })
}
