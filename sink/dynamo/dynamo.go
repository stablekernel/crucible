// SPDX-License-Identifier: Apache-2.0

// Package dynamo is a sink destination that persists payloads to Amazon
// DynamoDB through the AWS SDK for Go v2. It depends only on the DynamoDB
// service client and crucible/sink. Register a transformer that turns each
// payload type into one of the write operations exposed here (PutItem,
// UpdateItem, DeleteItem, TransactWrite, BatchWrite), then attach the result of
// [New] to a sink.Manifold.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package dynamo

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	csink "github.com/stablekernel/crucible/sink"
)

// ErrUnprocessedItems reports that a BatchWriteItem call returned without a
// request-level error but left one or more items unprocessed (DynamoDB returns
// these via UnprocessedItems on the output, typically under throttling).
// [BatchWriteChecked] returns an error wrapping this sentinel so a caller can
// match the class with errors.Is and retry the unprocessed items.
var ErrUnprocessedItems = errors.New("dynamo: BatchWriteItem left items unprocessed")

// Client is the narrow DynamoDB surface this destination needs. It declares
// only the write operations the package issues, so consumers wire the real
// *dynamodb.Client (which satisfies it structurally) while tests use a
// hand-rolled fake.
type Client interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

// PutItem returns an Op that creates or replaces a single item. The input
// carries the table name, the item attributes, and any condition expression.
func PutItem(in *dynamodb.PutItemInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.PutItem(ctx, in)
		return err
	})
}

// UpdateItem returns an Op that applies an update expression to a single item
// identified by the input's key.
func UpdateItem(in *dynamodb.UpdateItemInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.UpdateItem(ctx, in)
		return err
	})
}

// DeleteItem returns an Op that removes a single item identified by the input's
// key.
func DeleteItem(in *dynamodb.DeleteItemInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.DeleteItem(ctx, in)
		return err
	})
}

// TransactWrite returns an Op that applies up to the service-limited set of
// writes as a single all-or-nothing transaction.
func TransactWrite(in *dynamodb.TransactWriteItemsInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.TransactWriteItems(ctx, in)
		return err
	})
}

// BatchWrite returns an Op that issues a batch of put and delete requests. The
// SDK reports per-item failures via UnprocessedItems on the output; this Op
// returns only the request-level error and ignores unprocessed items. Use
// [BatchWriteChecked] when a left-behind item must surface as an error.
func BatchWrite(in *dynamodb.BatchWriteItemInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.BatchWriteItem(ctx, in)
		return err
	})
}

// BatchWriteChecked returns an Op that issues a batch of put and delete requests
// and treats unprocessed items as a failure. DynamoDB returns HTTP 200 with a
// populated UnprocessedItems map when it could not write every item (commonly
// under throttling), so the request-level error is nil even though work was
// dropped. The Op inspects the response and, when any item is unprocessed,
// returns an error wrapping [ErrUnprocessedItems] reporting how many tables and
// items were left behind. Pair it with sink.Reservoir or a retry middleware to
// resubmit the unprocessed items.
func BatchWriteChecked(in *dynamodb.BatchWriteItemInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		out, err := c.BatchWriteItem(ctx, in)
		if err != nil {
			return err
		}
		var items int
		for _, reqs := range out.UnprocessedItems {
			items += len(reqs)
		}
		if items > 0 {
			return fmt.Errorf("%w: %d item(s) across %d table(s)", ErrUnprocessedItems, items, len(out.UnprocessedItems))
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
// client. The outlet is named "dynamo" unless overridden with sink.WithName.
func New(client Client, reg *csink.Registry[csink.Op[Client]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Client](client, reg, append([]csink.EmitterOption{csink.WithName("dynamo")}, opts...)...)
}
