// SPDX-License-Identifier: Apache-2.0

//go:build integration

package sns_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

	csink "github.com/stablekernel/crucible/sink"
	snssink "github.com/stablekernel/crucible/sink/sns"
)

// alertRaisedIT is the payload the integration test sinks through the outlet.
type alertRaisedIT struct {
	Message string
}

// TestIntegrationSinkPublishesInLocalStack starts a LocalStack SNS+SQS, wires
// an SQS subscription to a topic, drives the real AWS SDK Publish path through
// the Outlet, then receives the fanned-out message to prove it landed. It skips
// cleanly when Docker is not reachable.
func TestIntegrationSinkPublishesInLocalStack(t *testing.T) {
	t.Parallel()
	skipWithoutDocker(t)

	ctx := context.Background()
	endpoint := startLocalStack(t, ctx)
	cfg := localStackConfig(t, ctx)

	snsClient := awssns.NewFromConfig(cfg, func(o *awssns.Options) { o.BaseEndpoint = aws.String(endpoint) })
	sqsClient := awssqs.NewFromConfig(cfg, func(o *awssqs.Options) { o.BaseEndpoint = aws.String(endpoint) })

	topic, err := snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{Name: aws.String("alerts")})
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	queue, err := sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String("alerts-q")})
	if err != nil {
		t.Fatalf("CreateQueue() error = %v", err)
	}
	attrs, err := sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       queue.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes() error = %v", err)
	}
	queueARN := attrs.Attributes["QueueArn"]
	if _, err = snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn:              topic.TopicArn,
		Protocol:              aws.String("sqs"),
		Endpoint:              aws.String(queueARN),
		Attributes:            map[string]string{"RawMessageDelivery": "true"},
		ReturnSubscriptionArn: true,
	}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	reg := snssink.NewRegistry()
	csink.Register(reg, func(_ context.Context, a alertRaisedIT) csink.Op[snssink.Client] {
		return snssink.Publish(aws.ToString(topic.TopicArn), a.Message)
	})

	outlet := snssink.New(snsClient, reg)
	if err = outlet.Sink(ctx, alertRaisedIT{Message: "disk full"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	out, err := sqsClient.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queue.QueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     5,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage() error = %v", err)
	}
	if len(out.Messages) != 1 || aws.ToString(out.Messages[0].Body) != "disk full" {
		t.Fatalf("received messages = %#v, want one body \"disk full\"", out.Messages)
	}
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
