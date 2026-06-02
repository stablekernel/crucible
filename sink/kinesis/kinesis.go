// SPDX-License-Identifier: Apache-2.0

// Package kinesis is a sink destination that writes payloads to Amazon Kinesis
// Data Streams via the AWS SDK v2. Register a transformer per payload type that
// produces a [PutRecord] or [PutRecords] operation, then attach the result of
// [New] to a sink.Manifold.
//
// The narrow [Client] interface is satisfied by the real
// *github.com/aws/aws-sdk-go-v2/service/kinesis.Client, so no casting is
// needed in production code. Tests use a hand-rolled fake.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package kinesis

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	csink "github.com/stablekernel/crucible/sink"
)

// Client is the narrow Kinesis surface this destination needs. It is satisfied
// structurally by *kinesis.Client from the AWS SDK v2, keeping the package free
// of a hard dependency on the concrete SDK type in production callers.
type Client interface {
	PutRecord(ctx context.Context, params *kinesis.PutRecordInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error)
	PutRecords(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error)
}

// PutRecord returns an Op that writes a single record to Kinesis using the
// supplied PutRecordInput. The StreamName or StreamARN field (or both) in input
// must identify the target stream. The caller owns the input struct and must
// not mutate it concurrently after calling PutRecord.
func PutRecord(input *kinesis.PutRecordInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.PutRecord(ctx, input)
		return err
	})
}

// PutRecordParams bundles the three logical fields of a single-record write for
// use with [PutRecordOf]. It keeps Op constructors free of SDK import noise at
// the call site when the caller only wants to supply stream, partition key, and
// raw bytes.
type PutRecordParams struct {
	// StreamName is the name of the target stream. Either StreamName or StreamARN
	// must be set.
	StreamName string
	// StreamARN is the ARN of the target stream. Either StreamName or StreamARN
	// must be set.
	StreamARN string
	// PartitionKey determines which shard the record is routed to.
	PartitionKey string
	// Data is the raw payload bytes to write.
	Data []byte
}

// PutRecordOf returns an Op that writes a single record using the provided
// stream name, partition key, and data bytes. It is a convenience alternative
// to [PutRecord] when the caller does not need fine-grained SDK input control.
// At least one of params.StreamName or params.StreamARN must be non-empty.
func PutRecordOf(params PutRecordParams) csink.Op[Client] {
	input := &kinesis.PutRecordInput{
		Data:         params.Data,
		PartitionKey: &params.PartitionKey,
	}
	if params.StreamName != "" {
		input.StreamName = &params.StreamName
	}
	if params.StreamARN != "" {
		input.StreamARN = &params.StreamARN
	}
	return PutRecord(input)
}

// PutRecords returns an Op that writes a batch of records to Kinesis in a
// single PutRecords call. The StreamName or StreamARN field (or both) in input
// must identify the target stream. The caller owns the input struct and must
// not mutate it concurrently after calling PutRecords.
func PutRecords(input *kinesis.PutRecordsInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.PutRecords(ctx, input)
		return err
	})
}

// PutRecordsEntry is a single record within a batch write. It mirrors the
// fields of types.PutRecordsRequestEntry that callers typically set, allowing
// [PutRecordsOf] to be used without importing the SDK types package directly.
type PutRecordsEntry struct {
	// PartitionKey determines which shard the record is routed to.
	PartitionKey string
	// Data is the raw payload bytes to write.
	Data []byte
}

// PutRecordsOf returns an Op that writes a batch of records to the named
// stream using a single PutRecords call. At least one of streamName or
// streamARN must be non-empty. An empty entries slice is allowed; the SDK will
// return a validation error in that case.
func PutRecordsOf(streamName, streamARN string, entries []PutRecordsEntry) csink.Op[Client] {
	sdkEntries := make([]types.PutRecordsRequestEntry, len(entries))
	for i, e := range entries {
		pk := e.PartitionKey
		sdkEntries[i] = types.PutRecordsRequestEntry{
			Data:         e.Data,
			PartitionKey: &pk,
		}
	}
	input := &kinesis.PutRecordsInput{Records: sdkEntries}
	if streamName != "" {
		input.StreamName = &streamName
	}
	if streamARN != "" {
		input.StreamARN = &streamARN
	}
	return PutRecords(input)
}

// NewRegistry returns an empty registry of Op[Client] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Client]] {
	return csink.NewRegistry[csink.Op[Client]]()
}

// New builds an Outlet that applies each payload's registered Op[Client] to
// client. The outlet is named "kinesis" unless overridden with sink.WithName.
func New(client Client, reg *csink.Registry[csink.Op[Client]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Client](client, reg, append([]csink.EmitterOption{csink.WithName("kinesis")}, opts...)...)
}
