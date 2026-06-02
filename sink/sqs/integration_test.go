// SPDX-License-Identifier: Apache-2.0

//go:build integration

package sqs_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

	csink "github.com/stablekernel/crucible/sink"
	sqssink "github.com/stablekernel/crucible/sink/sqs"
)

// orderPlacedIT is the payload the integration test sinks through the outlet.
type orderPlacedIT struct {
	ID string
}

// TestIntegrationSinkSendsMessageInLocalStack starts a LocalStack SQS, drives
// the real AWS SDK SendMessage path through the Outlet, then receives the
// message to prove it landed. It skips cleanly when Docker is not reachable.
func TestIntegrationSinkSendsMessageInLocalStack(t *testing.T) {
	t.Parallel()
	skipWithoutDocker(t)

	ctx := context.Background()
	endpoint := startLocalStack(t, ctx)
	client := newSQSClient(t, ctx, endpoint)

	created, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String("orders")})
	if err != nil {
		t.Fatalf("CreateQueue() error = %v", err)
	}
	queueURL := aws.ToString(created.QueueUrl)

	reg := sqssink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlacedIT) csink.Op[sqssink.Client] {
		return sqssink.SendMessage(queueURL, o.ID)
	})

	outlet := sqssink.New(client, reg)
	if err = outlet.Sink(ctx, orderPlacedIT{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	out, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     5,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage() error = %v", err)
	}
	if len(out.Messages) != 1 || aws.ToString(out.Messages[0].Body) != "A-1" {
		t.Fatalf("received messages = %#v, want one body A-1", out.Messages)
	}
}

func newSQSClient(t *testing.T, ctx context.Context, endpoint string) *awssqs.Client {
	t.Helper()
	cfg := localStackConfig(t, ctx)
	return awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

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
