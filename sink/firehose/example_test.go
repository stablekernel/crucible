// SPDX-License-Identifier: Apache-2.0

package firehose_test

import (
	"context"
	"fmt"

	awsfirehose "github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"

	csink "github.com/stablekernel/crucible/sink"
	firehosesink "github.com/stablekernel/crucible/sink/firehose"
)

// recordingClient records every PutRecord call for use in examples.
type recordingClient struct {
	streams []string
}

func (r *recordingClient) PutRecord(_ context.Context, params *awsfirehose.PutRecordInput, _ ...func(*awsfirehose.Options)) (*awsfirehose.PutRecordOutput, error) {
	r.streams = append(r.streams, *params.DeliveryStreamName)
	recordID := "record-id-1"
	return &awsfirehose.PutRecordOutput{RecordId: &recordID}, nil
}

func (r *recordingClient) PutRecordBatch(_ context.Context, params *awsfirehose.PutRecordBatchInput, _ ...func(*awsfirehose.Options)) (*awsfirehose.PutRecordBatchOutput, error) {
	r.streams = append(r.streams, *params.DeliveryStreamName)
	failed := int32(0)
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

type orderShipped struct{ OrderID string }

func ExampleNew() {
	client := &recordingClient{}
	reg := firehosesink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderShipped) csink.Op[firehosesink.Client] {
		return firehosesink.PutRecordOf("orders", []byte(o.OrderID))
	})

	outlet := firehosesink.New(client, reg)
	_ = outlet.Sink(context.Background(), orderShipped{OrderID: "ord-42"})

	fmt.Println(client.streams[0])
	// Output: orders
}
