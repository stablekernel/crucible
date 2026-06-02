// SPDX-License-Identifier: Apache-2.0

package kinesis_test

import (
	"context"
	"fmt"

	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	csink "github.com/stablekernel/crucible/sink"
	kinesissink "github.com/stablekernel/crucible/sink/kinesis"
)

// recordingClient records every PutRecord call for use in examples.
type recordingClient struct {
	partitionKeys []string
}

func (r *recordingClient) PutRecord(_ context.Context, params *awskinesis.PutRecordInput, _ ...func(*awskinesis.Options)) (*awskinesis.PutRecordOutput, error) {
	r.partitionKeys = append(r.partitionKeys, *params.PartitionKey)
	seq := "1"
	shardID := "shardId-000000000000"
	return &awskinesis.PutRecordOutput{SequenceNumber: &seq, ShardId: &shardID}, nil
}

func (r *recordingClient) PutRecords(_ context.Context, params *awskinesis.PutRecordsInput, _ ...func(*awskinesis.Options)) (*awskinesis.PutRecordsOutput, error) {
	for _, e := range params.Records {
		r.partitionKeys = append(r.partitionKeys, *e.PartitionKey)
	}
	failedCount := int32(0)
	resultEntries := make([]types.PutRecordsResultEntry, len(params.Records))
	for i := range params.Records {
		seq := "1"
		shardID := "shardId-000000000000"
		resultEntries[i] = types.PutRecordsResultEntry{SequenceNumber: &seq, ShardId: &shardID}
	}
	return &awskinesis.PutRecordsOutput{FailedRecordCount: &failedCount, Records: resultEntries}, nil
}

type orderShipped struct{ OrderID string }

func ExampleNew() {
	client := &recordingClient{}
	reg := kinesissink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderShipped) csink.Op[kinesissink.Client] {
		return kinesissink.PutRecordOf(kinesissink.PutRecordParams{
			StreamName:   "orders",
			PartitionKey: o.OrderID,
			Data:         []byte(o.OrderID),
		})
	})

	outlet := kinesissink.New(client, reg)
	_ = outlet.Sink(context.Background(), orderShipped{OrderID: "ord-42"})

	fmt.Println(client.partitionKeys[0])
	// Output: ord-42
}
