// SPDX-License-Identifier: Apache-2.0

// Package firehose is a sink destination that writes payloads to Amazon Data
// Firehose via the AWS SDK v2. Register a transformer per payload type that
// produces a [PutRecord] or [PutRecordBatch] operation, then attach the result
// of [New] to a sink.Manifold.
//
// The narrow [Client] interface is satisfied by the real
// *github.com/aws/aws-sdk-go-v2/service/firehose.Client, so no casting is
// needed in production code. Tests use a hand-rolled fake.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package firehose

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"

	csink "github.com/stablekernel/crucible/sink"
)

// Client is the narrow Firehose surface this destination needs. It is satisfied
// structurally by *firehose.Client from the AWS SDK v2, keeping the package
// free of a hard dependency on the concrete SDK type in production callers.
type Client interface {
	PutRecord(ctx context.Context, params *firehose.PutRecordInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordOutput, error)
	PutRecordBatch(ctx context.Context, params *firehose.PutRecordBatchInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error)
}

// PutRecord returns an Op that writes a single record to a Firehose delivery
// stream using the supplied PutRecordInput. The DeliveryStreamName field in
// input must identify the target stream. The caller owns the input struct and
// must not mutate it concurrently after calling PutRecord.
func PutRecord(input *firehose.PutRecordInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.PutRecord(ctx, input)
		return err
	})
}

// PutRecordOf returns an Op that writes a single record to the named Firehose
// delivery stream using the provided data bytes. It is a convenience
// alternative to [PutRecord] when the caller does not need fine-grained SDK
// input control. deliveryStream must be non-empty.
func PutRecordOf(deliveryStream string, data []byte) csink.Op[Client] {
	input := &firehose.PutRecordInput{
		DeliveryStreamName: &deliveryStream,
		Record:             &types.Record{Data: data},
	}
	return PutRecord(input)
}

// ErrPartialFailure is returned by a [PutRecordBatch] Op when the SDK call
// succeeds but reports one or more failed records via FailedPutCount > 0. The
// caller should inspect the batch output and retry the failed records.
var ErrPartialFailure = fmt.Errorf("firehose: PutRecordBatch reported partial failure")

// PutRecordBatch returns an Op that writes a batch of records to a Firehose
// delivery stream in a single PutRecordBatch call. The DeliveryStreamName field
// in input must identify the target stream. The caller owns the input struct and
// must not mutate it concurrently after calling PutRecordBatch.
//
// If the SDK call succeeds but PutRecordBatchOutput.FailedPutCount is greater
// than zero, the Op returns [ErrPartialFailure] so the failure is surfaced
// rather than silently dropped.
func PutRecordBatch(input *firehose.PutRecordBatchInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		out, err := c.PutRecordBatch(ctx, input)
		if err != nil {
			return err
		}
		if out != nil && out.FailedPutCount != nil && *out.FailedPutCount > 0 {
			return fmt.Errorf("%w: %d of %d records failed",
				ErrPartialFailure, *out.FailedPutCount, len(input.Records))
		}
		return nil
	})
}

// NewRegistry returns an empty registry of Op[Client] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Client]] {
	return csink.NewRegistry[csink.Op[Client]]()
}

// New builds an Outlet that applies each payload's registered Op[Client] to
// client. The outlet is named "firehose" unless overridden with sink.WithName.
func New(client Client, reg *csink.Registry[csink.Op[Client]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Client](client, reg, append([]csink.EmitterOption{csink.WithName("firehose")}, opts...)...)
}
