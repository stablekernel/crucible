// SPDX-License-Identifier: Apache-2.0

package sqs_test

import (
	"context"
	"errors"
	"testing"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/sinktest"
	sqssink "github.com/stablekernel/crucible/sink/sqs"
)

// fakeClient is a hand-rolled Client implementation — no AWS credentials, no
// mockery.
type fakeClient struct {
	sendCalls   []sendCall
	batchCalls  []batchCall
	sendErr     error
	batchErr    error
	batchFailed []types.BatchResultErrorEntry
}

type sendCall struct {
	queueURL string
	body     string
	input    *awssqs.SendMessageInput
}

type batchCall struct {
	queueURL string
	entries  []types.SendMessageBatchRequestEntry
}

func (f *fakeClient) SendMessage(
	_ context.Context,
	params *awssqs.SendMessageInput,
	_ ...func(*awssqs.Options),
) (*awssqs.SendMessageOutput, error) {
	call := sendCall{input: params}
	if params.QueueUrl != nil {
		call.queueURL = *params.QueueUrl
	}
	if params.MessageBody != nil {
		call.body = *params.MessageBody
	}
	f.sendCalls = append(f.sendCalls, call)
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	msgID := "msg-id-1"
	return &awssqs.SendMessageOutput{MessageId: &msgID}, nil
}

func (f *fakeClient) SendMessageBatch(
	_ context.Context,
	params *awssqs.SendMessageBatchInput,
	_ ...func(*awssqs.Options),
) (*awssqs.SendMessageBatchOutput, error) {
	call := batchCall{entries: params.Entries}
	if params.QueueUrl != nil {
		call.queueURL = *params.QueueUrl
	}
	f.batchCalls = append(f.batchCalls, call)
	if f.batchErr != nil {
		return nil, f.batchErr
	}
	return &awssqs.SendMessageBatchOutput{
		Failed: f.batchFailed,
	}, nil
}

// payloadA is a test payload type that maps to SendMessage.
type payloadA struct{ ID string }

// payloadB is a test payload type that maps to SendMessageFrom.
type payloadB struct{ Body string }

// payloadC is a test payload type that maps to SendMessageBatchOp.
type payloadC struct{ Items []string }

func str(s string) *string { return &s }

func newOutlet(c sqssink.Client) csink.Outlet {
	reg := sqssink.NewRegistry()
	csink.Register(reg, func(_ context.Context, p payloadA) csink.Op[sqssink.Client] {
		return sqssink.SendMessage("https://sqs.us-east-1.amazonaws.com/123/queue", p.ID)
	})
	csink.Register(reg, func(_ context.Context, p payloadB) csink.Op[sqssink.Client] {
		return sqssink.SendMessageFrom(&awssqs.SendMessageInput{
			QueueUrl:    str("https://sqs.us-east-1.amazonaws.com/123/queue"),
			MessageBody: &p.Body,
		})
	})
	csink.Register(reg, func(_ context.Context, p payloadC) csink.Op[sqssink.Client] {
		entries := make([]types.SendMessageBatchRequestEntry, len(p.Items))
		for i, item := range p.Items {
			id := str(item + "-id")
			body := str(item)
			entries[i] = types.SendMessageBatchRequestEntry{Id: id, MessageBody: body}
		}
		return sqssink.SendMessageBatchOp("https://sqs.us-east-1.amazonaws.com/123/queue", entries)
	})
	return sqssink.New(c, reg)
}

func TestSendMessage_PublishesMessage(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	if err := newOutlet(c).Sink(context.Background(), payloadA{ID: "event-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.sendCalls) != 1 {
		t.Fatalf("sendCalls = %d, want 1", len(c.sendCalls))
	}
	if c.sendCalls[0].body != "event-1" {
		t.Fatalf("body = %q, want %q", c.sendCalls[0].body, "event-1")
	}
}

func TestSendMessage_ErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("connection refused")
	c := &fakeClient{sendErr: boom}
	err := newOutlet(c).Sink(context.Background(), payloadA{ID: "x"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "sqs" {
		t.Fatalf("sink.Error = %+v, want Phase=apply Outlet=sqs", se)
	}
}

func TestSendMessageFrom_PublishesInput(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	if err := newOutlet(c).Sink(context.Background(), payloadB{Body: "hello"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.sendCalls) != 1 || c.sendCalls[0].body != "hello" {
		t.Fatalf("sendCalls = %+v, want one call with body=hello", c.sendCalls)
	}
}

func TestSendMessageBatchOp_PublishesBatch(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	if err := newOutlet(c).Sink(context.Background(), payloadC{Items: []string{"a", "b", "c"}}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.batchCalls) != 1 || len(c.batchCalls[0].entries) != 3 {
		t.Fatalf("batchCalls = %+v, want one call with 3 entries", c.batchCalls)
	}
}

func TestSendMessageBatchOp_SDKError(t *testing.T) {
	t.Parallel()

	boom := errors.New("throttled")
	c := &fakeClient{batchErr: boom}
	err := newOutlet(c).Sink(context.Background(), payloadC{Items: []string{"x"}})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "sqs" {
		t.Fatalf("sink.Error = %+v, want Phase=apply Outlet=sqs", se)
	}
}

func TestSendMessageBatchOp_PartialFailureError(t *testing.T) {
	t.Parallel()

	c := &fakeClient{
		batchFailed: []types.BatchResultErrorEntry{
			{Id: str("item-0"), Code: str("InvalidMessageContents"), SenderFault: true},
		},
	}
	err := newOutlet(c).Sink(context.Background(), payloadC{Items: []string{"bad"}})
	if err == nil {
		t.Fatal("Sink() = nil, want error for partial batch failure")
	}
	// The error originates from the Op (not a raw SDK error), so the Emitter
	// still wraps it in *csink.Error with PhaseApply.
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply {
		t.Fatalf("sink.Error = %+v, want PhaseApply", se)
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

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(&fakeClient{}) })
}
