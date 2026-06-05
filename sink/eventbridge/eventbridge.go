// SPDX-License-Identifier: Apache-2.0

// Package eventbridge is a sink destination that puts events onto Amazon
// EventBridge. It wraps the AWS SDK v2 EventBridge client behind a narrow
// [Client] interface so that unit tests work with hand-rolled fakes and no
// network is required. Register a transformer per payload type that returns a
// [PutEvents] or [PutEvent] Op, then pass the result of [New] to a
// sink.Manifold.
//
// # Partial failures
//
// EventBridge PutEvents returns HTTP 200 even when some entries fail. If
// PutEventsOutput.FailedEntryCount is greater than zero, the Op returns an
// error describing the failed entries. The Emitter wraps that error as a
// *sink.Error with Phase == sink.PhaseApply and Outlet == "eventbridge".
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package eventbridge

import (
	"context"
	"fmt"
	"strings"

	awseb "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	csink "github.com/stablekernel/crucible/sink"
)

// Client is the narrow Amazon EventBridge surface this destination needs. It
// is satisfied structurally by *eventbridge.Client from the AWS SDK v2 so no
// casting or wrapper is required in production code.
type Client interface {
	PutEvents(ctx context.Context, params *awseb.PutEventsInput, optFns ...func(*awseb.Options)) (*awseb.PutEventsOutput, error)
}

// PutEvents returns an Op that calls eventbridge.PutEvents with the supplied
// input exactly as given. It is the full-control constructor for callers that
// need to set every field of the request (resources, trace header, endpoint
// ID, etc.). If the response reports any failed entries, the Op returns a
// descriptive error.
func PutEvents(input *awseb.PutEventsInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		out, err := c.PutEvents(ctx, input)
		if err != nil {
			return err
		}
		if out.FailedEntryCount > 0 {
			return partialFailureError(out.FailedEntryCount, out.Entries)
		}
		return nil
	})
}

// PutEvent returns an Op that puts a single event onto eventBusName. source,
// detailType, and detail correspond to the EventBridge PutEventsRequestEntry
// fields Source, DetailType, and Detail. If the entry is rejected by
// EventBridge, the Op returns a descriptive error.
func PutEvent(eventBusName, source, detailType, detail string) csink.Op[Client] {
	return PutEvents(&awseb.PutEventsInput{
		Entries: []types.PutEventsRequestEntry{
			{
				EventBusName: &eventBusName,
				Source:       &source,
				DetailType:   &detailType,
				Detail:       &detail,
			},
		},
	})
}

// partialFailureError builds a descriptive error from the failed entries
// returned in a PutEvents response. EventBridge returns HTTP 200 for partial
// failures, so the Op inspects FailedEntryCount explicitly and passes it here as
// the authoritative count. The reported number is FailedEntryCount, not the
// number of entries that happened to carry an ErrorCode, so the count and its
// plural stay correct even when an entry omits a code.
func partialFailureError(failed int32, entries []types.PutEventsResultEntry) error {
	var parts []string
	for _, e := range entries {
		if e.ErrorCode == nil {
			continue
		}
		code := *e.ErrorCode
		msg := "<no message>"
		if e.ErrorMessage != nil {
			msg = *e.ErrorMessage
		}
		parts = append(parts, fmt.Sprintf("%s: %s", code, msg))
	}
	suffix := "y"
	if failed != 1 {
		suffix = "ies"
	}
	return fmt.Errorf(
		"eventbridge: %d entr%s failed: %s",
		failed,
		suffix,
		strings.Join(parts, "; "),
	)
}

// NewRegistry returns an empty registry of Op[Client] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Client]] {
	return csink.NewRegistry[csink.Op[Client]]()
}

// New builds an Outlet that applies each payload's registered Op[Client] to
// client. The outlet is named "eventbridge" unless overridden with
// sink.WithName.
func New(client Client, reg *csink.Registry[csink.Op[Client]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Client](client, reg, append([]csink.EmitterOption{csink.WithName("eventbridge")}, opts...)...)
}
