// SPDX-License-Identifier: Apache-2.0

package sqs_test

import (
	"context"
	"fmt"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	csink "github.com/stablekernel/crucible/sink"
	sqssink "github.com/stablekernel/crucible/sink/sqs"
)

// recordingClient is a stand-in Client that records the messages it sends.
type recordingClient struct{ bodies []string }

func (r *recordingClient) SendMessage(
	_ context.Context,
	params *awssqs.SendMessageInput,
	_ ...func(*awssqs.Options),
) (*awssqs.SendMessageOutput, error) {
	if params.MessageBody != nil {
		r.bodies = append(r.bodies, *params.MessageBody)
	}
	return &awssqs.SendMessageOutput{}, nil
}

func (r *recordingClient) SendMessageBatch(
	_ context.Context,
	_ *awssqs.SendMessageBatchInput,
	_ ...func(*awssqs.Options),
) (*awssqs.SendMessageBatchOutput, error) {
	return &awssqs.SendMessageBatchOutput{}, nil
}

type orderShipped struct{ OrderID string }

func ExampleNew() {
	c := &recordingClient{}
	reg := sqssink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderShipped) csink.Op[sqssink.Client] {
		return sqssink.SendMessage("https://sqs.us-east-1.amazonaws.com/123/orders", o.OrderID)
	})

	outlet := sqssink.New(c, reg)
	_ = outlet.Sink(context.Background(), orderShipped{OrderID: "ORD-42"})

	fmt.Println(c.bodies[0])
	// Output: ORD-42
}
