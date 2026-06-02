// SPDX-License-Identifier: Apache-2.0

package dynamo_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/dynamo"
	"github.com/stablekernel/crucible/sink/sinktest"
)

// fakeClient is a hand-rolled dynamo.Client with no SDK transport and no
// mockery. It records which method ran and the table it targeted, and can
// inject an error.
type fakeClient struct {
	calls []call
	err   error
}

type call struct {
	op    string
	table string
}

func (f *fakeClient) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.calls = append(f.calls, call{op: "PutItem", table: aws.ToString(in.TableName)})
	if f.err != nil {
		return nil, f.err
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakeClient) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	f.calls = append(f.calls, call{op: "UpdateItem", table: aws.ToString(in.TableName)})
	if f.err != nil {
		return nil, f.err
	}
	return &dynamodb.UpdateItemOutput{}, nil
}

func (f *fakeClient) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	f.calls = append(f.calls, call{op: "DeleteItem", table: aws.ToString(in.TableName)})
	if f.err != nil {
		return nil, f.err
	}
	return &dynamodb.DeleteItemOutput{}, nil
}

func (f *fakeClient) TransactWriteItems(_ context.Context, _ *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	f.calls = append(f.calls, call{op: "TransactWriteItems"})
	if f.err != nil {
		return nil, f.err
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func (f *fakeClient) BatchWriteItem(_ context.Context, _ *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	f.calls = append(f.calls, call{op: "BatchWriteItem"})
	if f.err != nil {
		return nil, f.err
	}
	return &dynamodb.BatchWriteItemOutput{}, nil
}

type orderPlaced struct{ ID string }

func item(id string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: id}}
}

func TestOpConstructors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      func() csink.Op[dynamo.Client]
		wantOp  string
		wantTab string
	}{
		{
			name: "PutItem",
			op: func() csink.Op[dynamo.Client] {
				return dynamo.PutItem(&dynamodb.PutItemInput{TableName: aws.String("orders"), Item: item("A-1")})
			},
			wantOp:  "PutItem",
			wantTab: "orders",
		},
		{
			name: "UpdateItem",
			op: func() csink.Op[dynamo.Client] {
				return dynamo.UpdateItem(&dynamodb.UpdateItemInput{TableName: aws.String("orders"), Key: item("A-1")})
			},
			wantOp:  "UpdateItem",
			wantTab: "orders",
		},
		{
			name: "DeleteItem",
			op: func() csink.Op[dynamo.Client] {
				return dynamo.DeleteItem(&dynamodb.DeleteItemInput{TableName: aws.String("orders"), Key: item("A-1")})
			},
			wantOp:  "DeleteItem",
			wantTab: "orders",
		},
		{
			name:    "TransactWrite",
			op:      func() csink.Op[dynamo.Client] { return dynamo.TransactWrite(&dynamodb.TransactWriteItemsInput{}) },
			wantOp:  "TransactWriteItems",
			wantTab: "",
		},
		{
			name:    "BatchWrite",
			op:      func() csink.Op[dynamo.Client] { return dynamo.BatchWrite(&dynamodb.BatchWriteItemInput{}) },
			wantOp:  "BatchWriteItem",
			wantTab: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := &fakeClient{}
			if err := tc.op().Apply(context.Background(), c); err != nil {
				t.Fatalf("Apply() error = %v", err)
			}
			if len(c.calls) != 1 {
				t.Fatalf("calls = %+v, want exactly one", c.calls)
			}
			if c.calls[0].op != tc.wantOp {
				t.Fatalf("op = %q, want %q", c.calls[0].op, tc.wantOp)
			}
			if c.calls[0].table != tc.wantTab {
				t.Fatalf("table = %q, want %q", c.calls[0].table, tc.wantTab)
			}
		})
	}
}

func newOutlet(c dynamo.Client) csink.Outlet {
	reg := dynamo.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[dynamo.Client] {
		return dynamo.PutItem(&dynamodb.PutItemInput{TableName: aws.String("orders"), Item: item(o.ID)})
	})
	return dynamo.New(c, reg)
}

func TestOutletPersistsRegisteredPayload(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	if err := newOutlet(c).Sink(context.Background(), orderPlaced{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.calls) != 1 || c.calls[0].op != "PutItem" || c.calls[0].table != "orders" {
		t.Fatalf("calls = %+v, want one PutItem on orders", c.calls)
	}
}

func TestUnregisteredPayloadSkips(t *testing.T) {
	t.Parallel()

	type other struct{}
	err := newOutlet(&fakeClient{}).Sink(context.Background(), other{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestApplyErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("ConditionalCheckFailedException")
	err := newOutlet(&fakeClient{err: boom}).Sink(context.Background(), orderPlaced{ID: "A-2"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "dynamo" {
		t.Fatalf("recovered = %+v, want *sink.Error{Outlet:dynamo, Phase:apply}", se)
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(&fakeClient{}) })
}
