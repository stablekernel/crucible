// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cloudwatch_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awscwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

	csink "github.com/stablekernel/crucible/sink"
	cloudwatchsink "github.com/stablekernel/crucible/sink/cloudwatch"
)

// logLineIT is the payload the integration test sinks through the outlet.
type logLineIT struct {
	Message string
}

// TestIntegrationSinkPutsLogEventInLocalStack starts a LocalStack CloudWatch
// Logs, drives the real AWS SDK PutLogEvents path through the Outlet, then
// reads the log stream back to prove the event landed. It skips cleanly when
// Docker is not reachable.
func TestIntegrationSinkPutsLogEventInLocalStack(t *testing.T) {
	t.Parallel()
	skipWithoutDocker(t)

	ctx := context.Background()
	endpoint := startLocalStack(t, ctx)
	client := newLogsClient(t, ctx, endpoint)

	const (
		group  = "orders"
		stream = "ingest"
	)
	if _, err := client.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{LogGroupName: aws.String(group)}); err != nil {
		t.Fatalf("CreateLogGroup() error = %v", err)
	}
	if _, err := client.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	}); err != nil {
		t.Fatalf("CreateLogStream() error = %v", err)
	}

	reg := cloudwatchsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, l logLineIT) csink.Op[cloudwatchsink.Client] {
		return cloudwatchsink.PutLogEvent(group, stream, l.Message)
	})

	outlet := cloudwatchsink.New(client, reg)
	if err := outlet.Sink(ctx, logLineIT{Message: "order A-1 placed"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	out, err := client.GetLogEvents(ctx, &awscwl.GetLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		StartFromHead: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("GetLogEvents() error = %v", err)
	}
	if len(out.Events) != 1 || aws.ToString(out.Events[0].Message) != "order A-1 placed" {
		t.Fatalf("log events = %#v, want one \"order A-1 placed\"", out.Events)
	}
}

func newLogsClient(t *testing.T, ctx context.Context, endpoint string) *awscwl.Client {
	t.Helper()
	cfg := localStackConfig(t, ctx)
	return awscwl.NewFromConfig(cfg, func(o *awscwl.Options) {
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
		t.Skipf("localstack.Run() unavailable (image pull or startup failed); skipping integration test: %v", err)
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
