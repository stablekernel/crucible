// SPDX-License-Identifier: Apache-2.0

// Package cloudwatch is a sink destination that writes log events to Amazon
// CloudWatch Logs. It wraps the AWS SDK v2 CloudWatch Logs client behind a
// narrow [Client] interface so that unit tests work with hand-rolled fakes and
// no network is required. Register a transformer per payload type that returns
// a [PutLogEvents] or [PutLogEvent] Op, then pass the result of [New] to a
// sink.Manifold.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package cloudwatch

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	csink "github.com/stablekernel/crucible/sink"
)

// Client is the narrow Amazon CloudWatch Logs surface this destination needs.
// It is satisfied structurally by *cloudwatchlogs.Client from the AWS SDK so
// no casting or wrapper is required in production code.
type Client interface {
	PutLogEvents(ctx context.Context, params *cloudwatchlogs.PutLogEventsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error)
}

// PutLogEvents returns an Op that sends the supplied input to CloudWatch Logs
// exactly as given. It is the full-control constructor for callers that need to
// set every field of the request (log group, log stream, sequence token, etc.).
func PutLogEvents(input *cloudwatchlogs.PutLogEventsInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.PutLogEvents(ctx, input)
		return err
	})
}

// PutLogEvent returns an Op that sends a single log event to the named log
// group and stream. The event timestamp is set to the current time in
// milliseconds since the Unix epoch, as required by the CloudWatch Logs API.
// Callers that need to control the timestamp, sequence token, or multiple
// events in one request should use [PutLogEvents] instead.
func PutLogEvent(logGroup, logStream, message string) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		ts := time.Now().UnixMilli()
		_, err := c.PutLogEvents(ctx, &cloudwatchlogs.PutLogEventsInput{
			LogGroupName:  &logGroup,
			LogStreamName: &logStream,
			LogEvents: []types.InputLogEvent{
				{
					Message:   &message,
					Timestamp: &ts,
				},
			},
		})
		return err
	})
}

// NewRegistry returns an empty registry of Op[Client] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Client]] {
	return csink.NewRegistry[csink.Op[Client]]()
}

// New builds an Outlet that applies each payload's registered Op[Client] to
// client. The outlet is named "cloudwatch" unless overridden with sink.WithName.
func New(client Client, reg *csink.Registry[csink.Op[Client]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Client](client, reg, append([]csink.EmitterOption{csink.WithName("cloudwatch")}, opts...)...)
}
