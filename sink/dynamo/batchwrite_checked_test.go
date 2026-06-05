// SPDX-License-Identifier: Apache-2.0

package dynamo_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/stablekernel/crucible/sink/dynamo"
)

// batchClient is a hand-rolled Client that returns a configurable
// BatchWriteItem response, so the UnprocessedItems path can be driven without a
// live table.
type batchClient struct {
	unprocessed map[string][]types.WriteRequest
	err         error
}

func (b *batchClient) PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}

func (b *batchClient) UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

func (b *batchClient) DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
}

func (b *batchClient) TransactWriteItems(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func (b *batchClient) BatchWriteItem(_ context.Context, _ *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if b.err != nil {
		return nil, b.err
	}
	return &dynamodb.BatchWriteItemOutput{UnprocessedItems: b.unprocessed}, nil
}

func writeReq(id string) types.WriteRequest {
	return types.WriteRequest{
		PutRequest: &types.PutRequest{Item: item(id)},
	}
}

func TestBatchWriteChecked_NoUnprocessedSucceeds(t *testing.T) {
	t.Parallel()

	c := &batchClient{}
	op := dynamo.BatchWriteChecked(&dynamodb.BatchWriteItemInput{})
	if err := op.Apply(context.Background(), c); err != nil {
		t.Fatalf("Apply() error = %v, want nil when no items are unprocessed", err)
	}
}

func TestBatchWriteChecked_UnprocessedReturnsSentinel(t *testing.T) {
	t.Parallel()

	c := &batchClient{
		unprocessed: map[string][]types.WriteRequest{
			"orders":    {writeReq("A-1"), writeReq("A-2")},
			"shipments": {writeReq("S-1")},
		},
	}
	op := dynamo.BatchWriteChecked(&dynamodb.BatchWriteItemInput{})
	err := op.Apply(context.Background(), c)
	if !errors.Is(err, dynamo.ErrUnprocessedItems) {
		t.Fatalf("Apply() = %v, want to wrap ErrUnprocessedItems", err)
	}
	if !strings.Contains(err.Error(), "3 item(s)") || !strings.Contains(err.Error(), "2 table(s)") {
		t.Errorf("error = %v, want it to report 3 items across 2 tables", err)
	}
}

func TestBatchWriteChecked_RequestErrorPropagates(t *testing.T) {
	t.Parallel()

	boom := errors.New("throttled")
	c := &batchClient{err: boom}
	op := dynamo.BatchWriteChecked(&dynamodb.BatchWriteItemInput{})
	if err := op.Apply(context.Background(), c); !errors.Is(err, boom) {
		t.Fatalf("Apply() = %v, want wrapped %v", err, boom)
	}
}

// TestBatchWrite_IgnoresUnprocessed documents that the unchecked BatchWrite
// still reports success when items are left unprocessed, so callers who need the
// stronger guarantee reach for BatchWriteChecked.
func TestBatchWrite_IgnoresUnprocessed(t *testing.T) {
	t.Parallel()

	c := &batchClient{
		unprocessed: map[string][]types.WriteRequest{"orders": {writeReq("A-1")}},
	}
	op := dynamo.BatchWrite(&dynamodb.BatchWriteItemInput{})
	if err := op.Apply(context.Background(), c); err != nil {
		t.Fatalf("BatchWrite Apply() = %v, want nil (unchecked variant ignores unprocessed)", err)
	}
}
