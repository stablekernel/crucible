// SPDX-License-Identifier: Apache-2.0

// Package sqs is a sink destination that publishes payloads to Amazon SQS. It
// depends on github.com/aws/aws-sdk-go-v2/service/sqs and crucible/sink.
// Register a transformer that turns each payload type into a [SendMessage] or
// [SendMessageBatch] operation, then attach the result of [New] to a
// sink.Manifold.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package sqs

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	csink "github.com/stablekernel/crucible/sink"
)

// Client is the narrow SQS surface this destination needs. It is satisfied
// structurally by *sqs.Client from github.com/aws/aws-sdk-go-v2/service/sqs,
// so tests use hand-rolled fakes without a live AWS connection.
type Client interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}

// SendMessage returns an Op that publishes a single message to queueURL with
// the given body. It is the most common constructor: register one per payload
// type that maps to a single SQS message.
func SendMessage(queueURL, body string) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    &queueURL,
			MessageBody: &body,
		})
		return err
	})
}

// SendMessageFrom returns an Op that publishes the message described by input
// to SQS. Use this when you need to set message attributes, delay, or other
// fields beyond queue URL and body.
func SendMessageFrom(input *sqs.SendMessageInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.SendMessage(ctx, input)
		return err
	})
}

// SendMessageBatchOp returns an Op that sends a batch of entries in a single
// SendMessageBatch call. The entries slice must contain at least one item and
// at most ten (the SQS batch limit). If any entry in the response is reported
// as failed by SQS, the Op returns an error listing the failed IDs.
func SendMessageBatchOp(queueURL string, entries []types.SendMessageBatchRequestEntry) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		out, err := c.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: &queueURL,
			Entries:  entries,
		})
		if err != nil {
			return err
		}
		if len(out.Failed) > 0 {
			return batchFailureError(out.Failed)
		}
		return nil
	})
}

// batchFailureError builds a descriptive error from partial-batch failures
// returned by SQS. SQS returns HTTP 200 for partial failures, so callers must
// inspect out.Failed explicitly.
func batchFailureError(failed []types.BatchResultErrorEntry) error {
	ids := make([]string, 0, len(failed))
	for _, f := range failed {
		id := "<nil>"
		if f.Id != nil {
			id = *f.Id
		}
		code := "<nil>"
		if f.Code != nil {
			code = *f.Code
		}
		ids = append(ids, fmt.Sprintf("%s(%s)", id, code))
	}
	return fmt.Errorf(
		"sqs: %d batch entr%s failed: %s",
		len(failed),
		pluralSuffix(len(failed)),
		strings.Join(ids, ", "),
	)
}

func pluralSuffix(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

// NewRegistry returns an empty registry of Op[Client] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Client]] {
	return csink.NewRegistry[csink.Op[Client]]()
}

// New builds an Outlet that applies each payload's registered Op[Client] to
// client. The outlet is named "sqs" unless overridden with sink.WithName.
func New(client Client, reg *csink.Registry[csink.Op[Client]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Client](client, reg, append([]csink.EmitterOption{csink.WithName("sqs")}, opts...)...)
}
