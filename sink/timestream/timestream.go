// SPDX-License-Identifier: Apache-2.0

// Package timestream is a sink destination that writes time-series records to
// Amazon Timestream via the AWS SDK v2. It depends on
// github.com/aws/aws-sdk-go-v2/service/timestreamwrite and crucible/sink. Register
// a transformer that turns each payload type into a [WriteRecords] operation,
// then attach the result of [New] to a sink.Manifold.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package timestream

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	csink "github.com/stablekernel/crucible/sink"
)

// Client is the narrow Timestream surface this destination needs. It is
// satisfied structurally by *timestreamwrite.Client, so tests use hand-rolled
// fakes without importing the real SDK client.
type Client interface {
	WriteRecords(
		ctx context.Context,
		params *timestreamwrite.WriteRecordsInput,
		optFns ...func(*timestreamwrite.Options),
	) (*timestreamwrite.WriteRecordsOutput, error)
}

// WriteRecords returns an Op that calls WriteRecords with the provided input.
// The raw SDK error is returned; the Emitter wraps it in a *sink.Error with
// PhaseApply and Outlet "timestream".
func WriteRecords(input *timestreamwrite.WriteRecordsInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.WriteRecords(ctx, input)
		return err
	})
}

// NewRegistry returns an empty registry of Op[Client] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Client]] {
	return csink.NewRegistry[csink.Op[Client]]()
}

// New builds an Outlet that applies each payload's registered Op[Client] to c.
// The outlet is named "timestream" unless overridden with sink.WithName.
func New(c Client, reg *csink.Registry[csink.Op[Client]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Client](c, reg, append([]csink.EmitterOption{csink.WithName("timestream")}, opts...)...)
}
