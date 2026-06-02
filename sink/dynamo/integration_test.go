// SPDX-License-Identifier: Apache-2.0

//go:build integration

package dynamo_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

	csink "github.com/stablekernel/crucible/sink"
	dynamosink "github.com/stablekernel/crucible/sink/dynamo"
)

// orderPlacedIT is the payload the integration test sinks through the outlet.
type orderPlacedIT struct {
	ID string
}

// TestIntegrationSinkPutsItemInLocalStack starts a LocalStack DynamoDB, drives
// the real AWS SDK PutItem path through the Outlet, then reads the item back to
// prove the write landed. It skips cleanly when Docker is not reachable.
func TestIntegrationSinkPutsItemInLocalStack(t *testing.T) {
	t.Parallel()
	skipWithoutDocker(t)

	ctx := context.Background()
	endpoint := startLocalStack(t, ctx)
	client := newDynamoClient(t, ctx, endpoint)

	const table = "orders"
	createTable(t, ctx, client, table)

	reg := dynamosink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlacedIT) csink.Op[dynamosink.Client] {
		return dynamosink.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item: map[string]ddbtypes.AttributeValue{
				"id": &ddbtypes.AttributeValueMemberS{Value: o.ID},
			},
		})
	})

	outlet := dynamosink.New(client, reg)
	if err := outlet.Sink(ctx, orderPlacedIT{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key:       map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "A-1"}},
	})
	if err != nil {
		t.Fatalf("GetItem() error = %v", err)
	}
	got, ok := out.Item["id"].(*ddbtypes.AttributeValueMemberS)
	if !ok || got.Value != "A-1" {
		t.Fatalf("persisted item id = %#v, want A-1", out.Item["id"])
	}
}

func newDynamoClient(t *testing.T, ctx context.Context, endpoint string) *dynamodb.Client {
	t.Helper()
	cfg := localStackConfig(t, ctx)
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func createTable(t *testing.T, ctx context.Context, client *dynamodb.Client, table string) {
	t.Helper()
	if _, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
	}); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}
	waiter := dynamodb.NewTableExistsWaiter(client)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)}, 30*time.Second); err != nil {
		t.Fatalf("table did not become active: %v", err)
	}
}

// localStackConfig builds an AWS config wired to LocalStack with static
// credentials, shared by every LocalStack-backed sink integration test.
func localStackConfig(t *testing.T, ctx context.Context) aws.Config {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig() error = %v", err)
	}
	return cfg
}

// startLocalStack starts a LocalStack container and returns its edge endpoint.
func startLocalStack(t *testing.T, ctx context.Context) string {
	t.Helper()
	container, err := localstack.Run(ctx, "localstack/localstack:3.8.1")
	if err != nil {
		t.Fatalf("localstack.Run() error = %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container.Host() error = %v", err)
	}
	port, err := container.MappedPort(ctx, "4566/tcp")
	if err != nil {
		t.Fatalf("container.MappedPort() error = %v", err)
	}
	return "http://" + host + ":" + port.Port()
}

// skipWithoutDocker skips the test when no Docker daemon is reachable, so the
// integration leg stays green on hosts without Docker and runs for real on the
// Docker-enabled CI leg.
func skipWithoutDocker(t *testing.T) {
	t.Helper()
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	defer func() { _ = provider.Close() }()
	if err := provider.Health(context.Background()); err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
}
