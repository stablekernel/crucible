// SPDX-License-Identifier: Apache-2.0

// Package sns is a sink destination that publishes payloads to Amazon SNS. It
// wraps the AWS SDK v2 SNS client behind a narrow [Client] interface so that
// unit tests work with hand-rolled fakes and no network is required. Register a
// transformer per payload type that returns a [Publish] or [PublishBatch] Op,
// then pass the result of [New] to a sink.Manifold.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package sns

import (
	"context"
	"fmt"
	"strings"

	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	csink "github.com/stablekernel/crucible/sink"
)

// Client is the narrow Amazon SNS surface this destination needs. It is
// satisfied structurally by *sns.Client from the AWS SDK so no casting or
// wrapper is required in production code.
type Client interface {
	Publish(ctx context.Context, params *awssns.PublishInput, optFns ...func(*awssns.Options)) (*awssns.PublishOutput, error)
	PublishBatch(ctx context.Context, params *awssns.PublishBatchInput, optFns ...func(*awssns.Options)) (*awssns.PublishBatchOutput, error)
}

// Publish returns an Op that sends a single message to topicARN. The message
// body is set to the provided message string. Callers that need fine-grained
// control over all PublishInput fields (subject, message attributes, FIFO
// deduplication, etc.) should use [PublishInput] instead.
func Publish(topicARN, message string) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.Publish(ctx, &awssns.PublishInput{
			TopicArn: &topicARN,
			Message:  &message,
		})
		return err
	})
}

// PublishInput returns an Op that calls sns.Publish with the supplied input
// exactly as given. It is the escape hatch for callers that need to control
// every field of the request.
func PublishInput(input *awssns.PublishInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.Publish(ctx, input)
		return err
	})
}

// PublishBatch returns an Op that calls sns.PublishBatch with the supplied
// input. The SDK accepts up to ten entries per batch request; callers are
// responsible for chunking larger slices before building the input.
//
// SNS returns HTTP 200 for a batch in which some entries were rejected, so the
// Op inspects the response's Failed list. When any entry failed the Op returns
// an error listing the failed IDs and codes, rather than reporting success.
func PublishBatch(input *awssns.PublishBatchInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		out, err := c.PublishBatch(ctx, input)
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
// returned by SNS. SNS returns HTTP 200 for partial failures, so callers must
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
	suffix := "y"
	if len(failed) != 1 {
		suffix = "ies"
	}
	return fmt.Errorf(
		"sns: %d batch entr%s failed: %s",
		len(failed),
		suffix,
		strings.Join(ids, ", "),
	)
}

// NewRegistry returns an empty registry of Op[Client] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Client]] {
	return csink.NewRegistry[csink.Op[Client]]()
}

// New builds an Outlet that applies each payload's registered Op[Client] to
// client. The outlet is named "sns" unless overridden with sink.WithName.
func New(client Client, reg *csink.Registry[csink.Op[Client]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Client](client, reg, append([]csink.EmitterOption{csink.WithName("sns")}, opts...)...)
}
