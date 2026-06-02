// SPDX-License-Identifier: Apache-2.0

package kinesis_test

import (
	"context"
	"errors"
	"testing"

	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	csink "github.com/stablekernel/crucible/sink"
	kinesissink "github.com/stablekernel/crucible/sink/kinesis"
	"github.com/stablekernel/crucible/sink/sinktest"
)

// fakeClient is a hand-rolled Client implementation — no AWS credentials, no
// network, no mockery. It records every call and can inject a fixed error.
type fakeClient struct {
	putRecordCalls  []*awskinesis.PutRecordInput
	putRecordsCalls []*awskinesis.PutRecordsInput
	err             error
}

func (f *fakeClient) PutRecord(_ context.Context, params *awskinesis.PutRecordInput, _ ...func(*awskinesis.Options)) (*awskinesis.PutRecordOutput, error) {
	f.putRecordCalls = append(f.putRecordCalls, params)
	if f.err != nil {
		return nil, f.err
	}
	seq := "1"
	shardID := "shardId-000000000000"
	return &awskinesis.PutRecordOutput{
		SequenceNumber: &seq,
		ShardId:        &shardID,
	}, nil
}

func (f *fakeClient) PutRecords(_ context.Context, params *awskinesis.PutRecordsInput, _ ...func(*awskinesis.Options)) (*awskinesis.PutRecordsOutput, error) {
	f.putRecordsCalls = append(f.putRecordsCalls, params)
	if f.err != nil {
		return nil, f.err
	}
	failedCount := int32(0)
	resultEntries := make([]types.PutRecordsResultEntry, len(params.Records))
	for i := range params.Records {
		seq := "1"
		shardID := "shardId-000000000000"
		resultEntries[i] = types.PutRecordsResultEntry{
			SequenceNumber: &seq,
			ShardId:        &shardID,
		}
	}
	return &awskinesis.PutRecordsOutput{
		FailedRecordCount: &failedCount,
		Records:           resultEntries,
	}, nil
}

// streamName is a shared stream name used across tests.
const streamName = "test-stream"

// newOutlet wires a registry that maps eventPayload to PutRecordOf and returns
// a fresh Outlet backed by the given fake client.
func newOutlet(c kinesissink.Client) csink.Outlet {
	reg := kinesissink.NewRegistry()
	csink.Register(reg, func(_ context.Context, e eventPayload) csink.Op[kinesissink.Client] {
		return kinesissink.PutRecordOf(kinesissink.PutRecordParams{
			StreamName:   streamName,
			PartitionKey: e.ID,
			Data:         []byte(e.Body),
		})
	})
	return kinesissink.New(c, reg)
}

type eventPayload struct {
	ID   string
	Body string
}

// --- PutRecord tests ---------------------------------------------------------

func TestPutRecord_WritesRecord(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	stream := streamName
	pk := "pk-1"
	data := []byte("hello")
	op := kinesissink.PutRecord(&awskinesis.PutRecordInput{
		StreamName:   &stream,
		PartitionKey: &pk,
		Data:         data,
	})

	if err := op.Apply(context.Background(), client); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if len(client.putRecordCalls) != 1 {
		t.Fatalf("PutRecord called %d times, want 1", len(client.putRecordCalls))
	}
	got := client.putRecordCalls[0]
	if *got.PartitionKey != pk {
		t.Errorf("PartitionKey = %q, want %q", *got.PartitionKey, pk)
	}
	if string(got.Data) != "hello" {
		t.Errorf("Data = %q, want %q", got.Data, "hello")
	}
}

func TestPutRecord_PropagatesError(t *testing.T) {
	t.Parallel()

	boom := errors.New("throttled")
	client := &fakeClient{err: boom}
	stream := streamName
	pk := "pk-err"
	op := kinesissink.PutRecord(&awskinesis.PutRecordInput{
		StreamName:   &stream,
		PartitionKey: &pk,
		Data:         []byte("x"),
	})
	if err := op.Apply(context.Background(), client); !errors.Is(err, boom) {
		t.Fatalf("Apply() = %v, want %v", err, boom)
	}
}

// --- PutRecordOf tests -------------------------------------------------------

func TestPutRecordOf_WritesRecord(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	op := kinesissink.PutRecordOf(kinesissink.PutRecordParams{
		StreamName:   streamName,
		PartitionKey: "pk-2",
		Data:         []byte("world"),
	})

	if err := op.Apply(context.Background(), client); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if len(client.putRecordCalls) != 1 {
		t.Fatalf("PutRecord called %d times, want 1", len(client.putRecordCalls))
	}
	got := client.putRecordCalls[0]
	if *got.StreamName != streamName {
		t.Errorf("StreamName = %q, want %q", *got.StreamName, streamName)
	}
	if *got.PartitionKey != "pk-2" {
		t.Errorf("PartitionKey = %q, want %q", *got.PartitionKey, "pk-2")
	}
}

func TestPutRecordOf_StreamARN(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	arn := "arn:aws:kinesis:us-east-1:123456789012:stream/test"
	op := kinesissink.PutRecordOf(kinesissink.PutRecordParams{
		StreamARN:    arn,
		PartitionKey: "pk-3",
		Data:         []byte("arn-data"),
	})

	if err := op.Apply(context.Background(), client); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	got := client.putRecordCalls[0]
	if got.StreamName != nil {
		t.Errorf("StreamName should be nil when only ARN set, got %q", *got.StreamName)
	}
	if *got.StreamARN != arn {
		t.Errorf("StreamARN = %q, want %q", *got.StreamARN, arn)
	}
}

// --- PutRecords tests --------------------------------------------------------

func TestPutRecords_WritesBatch(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	stream := streamName
	pk1, pk2 := "pk-a", "pk-b"
	op := kinesissink.PutRecords(&awskinesis.PutRecordsInput{
		StreamName: &stream,
		Records: []types.PutRecordsRequestEntry{
			{PartitionKey: &pk1, Data: []byte("a")},
			{PartitionKey: &pk2, Data: []byte("b")},
		},
	})

	if err := op.Apply(context.Background(), client); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if len(client.putRecordsCalls) != 1 {
		t.Fatalf("PutRecords called %d times, want 1", len(client.putRecordsCalls))
	}
	if len(client.putRecordsCalls[0].Records) != 2 {
		t.Errorf("Records len = %d, want 2", len(client.putRecordsCalls[0].Records))
	}
}

func TestPutRecords_PropagatesError(t *testing.T) {
	t.Parallel()

	boom := errors.New("service unavailable")
	client := &fakeClient{err: boom}
	stream := streamName
	pk := "pk-c"
	op := kinesissink.PutRecords(&awskinesis.PutRecordsInput{
		StreamName: &stream,
		Records:    []types.PutRecordsRequestEntry{{PartitionKey: &pk, Data: []byte("c")}},
	})
	if err := op.Apply(context.Background(), client); !errors.Is(err, boom) {
		t.Fatalf("Apply() = %v, want %v", err, boom)
	}
}

// --- PutRecordsOf tests ------------------------------------------------------

func TestPutRecordsOf_WritesBatch(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	entries := []kinesissink.PutRecordsEntry{
		{PartitionKey: "pk-x", Data: []byte("x")},
		{PartitionKey: "pk-y", Data: []byte("y")},
	}
	op := kinesissink.PutRecordsOf(streamName, "", entries)

	if err := op.Apply(context.Background(), client); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	calls := client.putRecordsCalls
	if len(calls) != 1 {
		t.Fatalf("PutRecords called %d times, want 1", len(calls))
	}
	if len(calls[0].Records) != 2 {
		t.Errorf("Records len = %d, want 2", len(calls[0].Records))
	}
	if *calls[0].Records[0].PartitionKey != "pk-x" {
		t.Errorf("Records[0].PartitionKey = %q, want pk-x", *calls[0].Records[0].PartitionKey)
	}
}

// --- Outlet integration tests ------------------------------------------------

func TestOutlet_SinkRoutesToPutRecordOf(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	outlet := newOutlet(client)
	if err := outlet.Sink(context.Background(), eventPayload{ID: "e-1", Body: "payload"}); err != nil {
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
	err := newOutlet(client).Sink(context.Background(), eventPayload{ID: "e-2", Body: "data"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "kinesis" {
		t.Fatalf("error = %+v, want *sink.Error{Outlet:kinesis, Phase:apply}", se)
	}
}

// --- Conformance -------------------------------------------------------------

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(&fakeClient{}) })
}
