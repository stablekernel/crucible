// SPDX-License-Identifier: Apache-2.0

package sns_test

import (
	"context"
	"fmt"

	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	csink "github.com/stablekernel/crucible/sink"
	snssink "github.com/stablekernel/crucible/sink/sns"
)

// recordingClient records the topic ARN of each Publish call.
type recordingClient struct{ topics []string }

func (r *recordingClient) Publish(_ context.Context, params *awssns.PublishInput, _ ...func(*awssns.Options)) (*awssns.PublishOutput, error) {
	if params.TopicArn != nil {
		r.topics = append(r.topics, *params.TopicArn)
	}
	return &awssns.PublishOutput{}, nil
}

func (r *recordingClient) PublishBatch(_ context.Context, _ *awssns.PublishBatchInput, _ ...func(*awssns.Options)) (*awssns.PublishBatchOutput, error) {
	return &awssns.PublishBatchOutput{}, nil
}

type itemPurchased struct{ ItemID string }

func ExampleNew() {
	const topicARN = "arn:aws:sns:us-east-1:123456789012:purchases"
	c := &recordingClient{}
	reg := snssink.NewRegistry()
	csink.Register(reg, func(_ context.Context, e itemPurchased) csink.Op[snssink.Client] {
		return snssink.Publish(topicARN, "purchased:"+e.ItemID)
	})

	outlet := snssink.New(c, reg)
	_ = outlet.Sink(context.Background(), itemPurchased{ItemID: "ITEM-42"})

	fmt.Println(c.topics[0])
	// Output: arn:aws:sns:us-east-1:123456789012:purchases
}
