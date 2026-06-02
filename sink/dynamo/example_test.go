// SPDX-License-Identifier: Apache-2.0

package dynamo_test

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/dynamo"
)

// recordingClient is a stand-in dynamo.Client that records the tables it writes.
type recordingClient struct{ tables []string }

func (r *recordingClient) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	r.tables = append(r.tables, aws.ToString(in.TableName))
	return &dynamodb.PutItemOutput{}, nil
}

func (r *recordingClient) UpdateItem(_ context.Context, _ *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

func (r *recordingClient) DeleteItem(_ context.Context, _ *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
}

func (r *recordingClient) TransactWriteItems(_ context.Context, _ *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func (r *recordingClient) BatchWriteItem(_ context.Context, _ *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	return &dynamodb.BatchWriteItemOutput{}, nil
}

type userRegistered struct{ Email string }

func ExampleNew() {
	client := &recordingClient{}
	reg := dynamo.NewRegistry()
	csink.Register(reg, func(_ context.Context, u userRegistered) csink.Op[dynamo.Client] {
		return dynamo.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String("users"),
			Item:      map[string]types.AttributeValue{"email": &types.AttributeValueMemberS{Value: u.Email}},
		})
	})

	outlet := dynamo.New(client, reg)
	_ = outlet.Sink(context.Background(), userRegistered{Email: "a@example.com"})

	fmt.Println(client.tables[0])
	// Output: users
}
