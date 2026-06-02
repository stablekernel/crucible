// SPDX-License-Identifier: Apache-2.0

//go:build integration

package eventbridge_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awseb "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

	csink "github.com/stablekernel/crucible/sink"
	eventbridgesink "github.com/stablekernel/crucible/sink/eventbridge"
)

// orderPlacedIT is the payload the integration test sinks through the outlet.
type orderPlacedIT struct {
	ID string
}

// TestIntegrationSinkPutsEventInLocalStack starts a LocalStack EventBridge+SQS,
// wires a rule that routes a matched event to an SQS target, drives the real
// AWS SDK PutEvents path through the Outlet, then receives the routed message
// to prove the event landed. It skips cleanly when Docker is not reachable.
func TestIntegrationSinkPutsEventInLocalStack(t *testing.T) {
	t.Parallel()
	skipWithoutDocker(t)

	ctx := context.Background()
	endpoint := startLocalStack(t, ctx)
	cfg := localStackConfig(t, ctx)

	ebClient := awseb.NewFromConfig(cfg, func(o *awseb.Options) { o.BaseEndpoint = aws.String(endpoint) })
	sqsClient := awssqs.NewFromConfig(cfg, func(o *awssqs.Options) { o.BaseEndpoint = aws.String(endpoint) })

	const source = "crucible.orders"
	queue, err := sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String("eb-target")})
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

	rule, err := ebClient.PutRule(ctx, &awseb.PutRuleInput{
		Name:         aws.String("orders-rule"),
		EventPattern: aws.String(`{"source":["` + source + `"]}`),
	})
	if err != nil {
		t.Fatalf("PutRule() error = %v", err)
	}
	if _, err = ebClient.PutTargets(ctx, &awseb.PutTargetsInput{
		Rule: aws.String("orders-rule"),
		Targets: []ebtypes.Target{
			{Id: aws.String("sqs-target"), Arn: aws.String(queueARN)},
		},
	}); err != nil {
		t.Fatalf("PutTargets() error = %v", err)
	}
	_ = rule

	reg := eventbridgesink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlacedIT) csink.Op[eventbridgesink.Client] {
		return eventbridgesink.PutEvent("default", source, "OrderPlaced", `{"id":"`+o.ID+`"}`)
	})

	outlet := eventbridgesink.New(ebClient, reg)
	if err = outlet.Sink(ctx, orderPlacedIT{ID: "A-1"}); err != nil {
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
	if len(out.Messages) != 1 || !strings.Contains(aws.ToString(out.Messages[0].Body), `"id":"A-1"`) {
		t.Fatalf("routed messages = %#v, want one carrying id A-1", out.Messages)
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
