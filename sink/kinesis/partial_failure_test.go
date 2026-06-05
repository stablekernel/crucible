// SPDX-License-Identifier: Apache-2.0

package kinesis_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	kinesissink "github.com/stablekernel/crucible/sink/kinesis"
)

// failingClient returns a PutRecords response with a non-zero FailedRecordCount
// so the partial-failure path is exercised. Kinesis surfaces such a batch as
// HTTP 200, so only inspecting the response detects it.
type failingClient struct {
	failedCount int32
	records     []types.PutRecordsResultEntry
}

func (f *failingClient) PutRecord(context.Context, *awskinesis.PutRecordInput, ...func(*awskinesis.Options)) (*awskinesis.PutRecordOutput, error) {
	return &awskinesis.PutRecordOutput{}, nil
}

func (f *failingClient) PutRecords(_ context.Context, _ *awskinesis.PutRecordsInput, _ ...func(*awskinesis.Options)) (*awskinesis.PutRecordsOutput, error) {
	count := f.failedCount
	return &awskinesis.PutRecordsOutput{FailedRecordCount: &count, Records: f.records}, nil
}

func TestPutRecords_PartialFailureReturnsSentinel(t *testing.T) {
	t.Parallel()

	c := &failingClient{
		failedCount: 1,
		records: []types.PutRecordsResultEntry{
			{SequenceNumber: ptrStr("1"), ShardId: ptrStr("shardId-0")},
			{ErrorCode: ptrStr("ProvisionedThroughputExceededException"), ErrorMessage: ptrStr("rate exceeded")},
		},
	}
	stream := streamName
	pk := "pk-1"
	op := kinesissink.PutRecords(&awskinesis.PutRecordsInput{
		StreamName: &stream,
		Records:    []types.PutRecordsRequestEntry{{PartitionKey: &pk, Data: []byte("a")}},
	})

	err := op.Apply(context.Background(), c)
	if !errors.Is(err, kinesissink.ErrPartialFailure) {
		t.Fatalf("Apply() = %v, want to wrap ErrPartialFailure", err)
	}
	if !strings.Contains(err.Error(), "ProvisionedThroughputExceededException") {
		t.Errorf("error = %v, want the per-record error code in the message", err)
	}
	if !strings.Contains(err.Error(), "1 failed") {
		t.Errorf("error = %v, want the failed count in the message", err)
	}
}

func TestPutRecords_PartialFailureWithoutErrorCodes(t *testing.T) {
	t.Parallel()

	// FailedRecordCount is non-zero but no entry carries an ErrorCode; the Op
	// still reports the sentinel rather than silently succeeding.
	c := &failingClient{failedCount: 2, records: []types.PutRecordsResultEntry{{}, {}}}
	stream := streamName
	pk := "pk-2"
	op := kinesissink.PutRecords(&awskinesis.PutRecordsInput{
		StreamName: &stream,
		Records:    []types.PutRecordsRequestEntry{{PartitionKey: &pk, Data: []byte("b")}},
	})

	err := op.Apply(context.Background(), c)
	if !errors.Is(err, kinesissink.ErrPartialFailure) {
		t.Fatalf("Apply() = %v, want ErrPartialFailure", err)
	}
	if !strings.Contains(err.Error(), "2 failed") {
		t.Errorf("error = %v, want count 2", err)
	}
}

func TestPutRecordsOf_StreamARNPath(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	arn := "arn:aws:kinesis:us-east-1:123456789012:stream/test"
	op := kinesissink.PutRecordsOf("", arn, []kinesissink.PutRecordsEntry{
		{PartitionKey: "pk-x", Data: []byte("x")},
	})

	if err := op.Apply(context.Background(), c); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	got := c.putRecordsCalls[0]
	if got.StreamName != nil {
		t.Errorf("StreamName should be nil when only ARN is set, got %q", *got.StreamName)
	}
	if got.StreamARN == nil || *got.StreamARN != arn {
		t.Errorf("StreamARN = %v, want %q", got.StreamARN, arn)
	}
}

func ptrStr(s string) *string { return &s }
