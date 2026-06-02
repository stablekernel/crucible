// SPDX-License-Identifier: Apache-2.0

package firehose_test

import (
	"context"
	"errors"
	"testing"

	awsfirehose "github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"

	csink "github.com/stablekernel/crucible/sink"
	firehosesink "github.com/stablekernel/crucible/sink/firehose"
	"github.com/stablekernel/crucible/sink/sinktest"
)

// fakeClient is a hand-rolled Client implementation — no AWS credentials, no
// network, no mockery. It records every call and can inject a fixed error.
// When failCount > 0, PutRecordBatch reports that many records as failed
// (simulating a partial failure even when err is nil).
type fakeClient struct {
	putRecordCalls      []*awsfirehose.PutRecordInput
	putRecordBatchCalls []*awsfirehose.PutRecordBatchInput
	err                 error
	failCount           int32
}

func (f *fakeClient) PutRecord(_ context.Context, params *awsfirehose.PutRecordInput, _ ...func(*awsfirehose.Options)) (*awsfirehose.PutRecordOutput, error) {
	f.putRecordCalls = append(f.putRecordCalls, params)
	if f.err != nil {
		return nil, f.err
	}
	recordID := "record-id-1"
	return &awsfirehose.PutRecordOutput{RecordId: &recordID}, nil
}

func (f *fakeClient) PutRecordBatch(_ context.Context, params *awsfirehose.PutRecordBatchInput, _ ...func(*awsfirehose.Options)) (*awsfirehose.PutRecordBatchOutput, error) {
	f.putRecordBatchCalls = append(f.putRecordBatchCalls, params)
	if f.err != nil {
		return nil, f.err
	}
	failed := f.failCount
	results := make([]types.PutRecordBatchResponseEntry, len(params.Records))
	for i := range params.Records {
		recordID := "record-id-batch"
		results[i] = types.PutRecordBatchResponseEntry{RecordId: &recordID}
	}
	return &awsfirehose.PutRecordBatchOutput{
		FailedPutCount:   &failed,
		RequestResponses: results,
	}, nil
}

// deliveryStream is a shared stream name used across tests.
const deliveryStream = "test-stream"

// newOutlet wires a registry that maps eventPayload to PutRecordOf and returns
// a fresh Outlet backed by the given fake client.
func newOutlet(c firehosesink.Client) csink.Outlet {
	reg := firehosesink.NewRegistry()
	csink.Register(reg, func(_ context.Context, e eventPayload) csink.Op[firehosesink.Client] {
		return firehosesink.PutRecordOf(deliveryStream, []byte(e.Body))
	})
	return firehosesink.New(c, reg)
}

type eventPayload struct {
	Body string
}

// --- PutRecord tests ---------------------------------------------------------

func TestPutRecord_WritesRecord(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	stream := deliveryStream
	op := firehosesink.PutRecord(&awsfirehose.PutRecordInput{
		DeliveryStreamName: &stream,
		Record:             &types.Record{Data: []byte("hello")},
	})

	if err := op.Apply(context.Background(), client); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if len(client.putRecordCalls) != 1 {
		t.Fatalf("PutRecord called %d times, want 1", len(client.putRecordCalls))
	}
	got := client.putRecordCalls[0]
	if *got.DeliveryStreamName != deliveryStream {
		t.Errorf("DeliveryStreamName = %q, want %q", *got.DeliveryStreamName, deliveryStream)
	}
	if string(got.Record.Data) != "hello" {
		t.Errorf("Data = %q, want %q", got.Record.Data, "hello")
	}
}

func TestPutRecord_PropagatesError(t *testing.T) {
	t.Parallel()

	boom := errors.New("throttled")
	client := &fakeClient{err: boom}
	stream := deliveryStream
	op := firehosesink.PutRecord(&awsfirehose.PutRecordInput{
		DeliveryStreamName: &stream,
		Record:             &types.Record{Data: []byte("x")},
	})
	if err := op.Apply(context.Background(), client); !errors.Is(err, boom) {
		t.Fatalf("Apply() = %v, want %v", err, boom)
	}
}

// --- PutRecordOf tests -------------------------------------------------------

func TestPutRecordOf_WritesRecord(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	op := firehosesink.PutRecordOf(deliveryStream, []byte("world"))

	if err := op.Apply(context.Background(), client); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if len(client.putRecordCalls) != 1 {
		t.Fatalf("PutRecord called %d times, want 1", len(client.putRecordCalls))
	}
	got := client.putRecordCalls[0]
	if *got.DeliveryStreamName != deliveryStream {
		t.Errorf("DeliveryStreamName = %q, want %q", *got.DeliveryStreamName, deliveryStream)
	}
	if string(got.Record.Data) != "world" {
		t.Errorf("Data = %q, want %q", got.Record.Data, "world")
	}
}

func TestPutRecordOf_PropagatesError(t *testing.T) {
	t.Parallel()

	boom := errors.New("service unavailable")
	client := &fakeClient{err: boom}
	op := firehosesink.PutRecordOf(deliveryStream, []byte("data"))
	if err := op.Apply(context.Background(), client); !errors.Is(err, boom) {
		t.Fatalf("Apply() = %v, want %v", err, boom)
	}
}

// --- PutRecordBatch tests ----------------------------------------------------

func TestPutRecordBatch_WritesBatch(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	stream := deliveryStream
	op := firehosesink.PutRecordBatch(&awsfirehose.PutRecordBatchInput{
		DeliveryStreamName: &stream,
		Records: []types.Record{
			{Data: []byte("a")},
			{Data: []byte("b")},
		},
	})

	if err := op.Apply(context.Background(), client); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if len(client.putRecordBatchCalls) != 1 {
		t.Fatalf("PutRecordBatch called %d times, want 1", len(client.putRecordBatchCalls))
	}
	if len(client.putRecordBatchCalls[0].Records) != 2 {
		t.Errorf("Records len = %d, want 2", len(client.putRecordBatchCalls[0].Records))
	}
}

func TestPutRecordBatch_PropagatesSDKError(t *testing.T) {
	t.Parallel()

	boom := errors.New("access denied")
	client := &fakeClient{err: boom}
	stream := deliveryStream
	op := firehosesink.PutRecordBatch(&awsfirehose.PutRecordBatchInput{
		DeliveryStreamName: &stream,
		Records:            []types.Record{{Data: []byte("c")}},
	})
	if err := op.Apply(context.Background(), client); !errors.Is(err, boom) {
		t.Fatalf("Apply() = %v, want %v", err, boom)
	}
}

func TestPutRecordBatch_PartialFailureError(t *testing.T) {
	t.Parallel()

	client := &fakeClient{failCount: 1}
	stream := deliveryStream
	op := firehosesink.PutRecordBatch(&awsfirehose.PutRecordBatchInput{
		DeliveryStreamName: &stream,
		Records: []types.Record{
			{Data: []byte("good")},
			{Data: []byte("bad")},
		},
	})

	err := op.Apply(context.Background(), client)
	if err == nil {
		t.Fatal("Apply() returned nil, want ErrPartialFailure")
	}
	if !errors.Is(err, firehosesink.ErrPartialFailure) {
		t.Fatalf("Apply() = %v, want errors.Is(err, ErrPartialFailure)", err)
	}
}

// --- Outlet integration tests ------------------------------------------------

func TestOutlet_SinkRoutesToPutRecordOf(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	outlet := newOutlet(client)
	if err := outlet.Sink(context.Background(), eventPayload{Body: "payload"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(client.putRecordCalls) != 1 {
		t.Fatalf("PutRecord called %d times, want 1", len(client.putRecordCalls))
	}
}

func TestOutlet_UnregisteredPayloadSkips(t *testing.T) {
	t.Parallel()

	type other struct{}
	err := newOutlet(&fakeClient{}).Sink(context.Background(), other{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestOutlet_ApplyErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("request limit exceeded")
	client := &fakeClient{err: boom}
	err := newOutlet(client).Sink(context.Background(), eventPayload{Body: "data"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "firehose" {
		t.Fatalf("error = %+v, want *sink.Error{Outlet:firehose, Phase:apply}", se)
	}
}

// --- Conformance -------------------------------------------------------------

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(&fakeClient{}) })
}
