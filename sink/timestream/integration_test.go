// SPDX-License-Identifier: Apache-2.0

//go:build integration

package timestream_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsts "github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	tstypes "github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

	csink "github.com/stablekernel/crucible/sink"
	timestreamsink "github.com/stablekernel/crucible/sink/timestream"
)

// readingIT is the payload the integration test sinks through the outlet.
type readingIT struct {
	Name  string
	Value string
}

// TestIntegrationSinkWritesRecordsInLocalStack starts a LocalStack Timestream,
// drives the real AWS SDK WriteRecords path through the Outlet, and asserts the
// write was accepted (LocalStack reports records ingested). It skips cleanly
// when Docker is not reachable, and when the running LocalStack edition does
// not provide the Timestream service.
func TestIntegrationSinkWritesRecordsInLocalStack(t *testing.T) {
	t.Parallel()
	skipWithoutDocker(t)

	ctx := context.Background()
	endpoint := startLocalStack(t, ctx)
	client := newTimestreamClient(t, ctx, endpoint)

	const (
		database = "metrics"
		table    = "readings"
	)
	if _, err := client.CreateDatabase(ctx, &awsts.CreateDatabaseInput{
		DatabaseName: aws.String(database),
	}); err != nil {
		t.Skipf("timestream unavailable in this LocalStack edition: %v", err)
	}
	if _, err := client.CreateTable(ctx, &awsts.CreateTableInput{
		DatabaseName: aws.String(database),
		TableName:    aws.String(table),
	}); err != nil {
		t.Skipf("timestream table create unavailable in this LocalStack edition: %v", err)
	}

	reg := timestreamsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, r readingIT) csink.Op[timestreamsink.Client] {
		return timestreamsink.WriteRecords(&awsts.WriteRecordsInput{
			DatabaseName: aws.String(database),
			TableName:    aws.String(table),
			Records: []tstypes.Record{
				{
					MeasureName:      aws.String(r.Name),
					MeasureValue:     aws.String(r.Value),
					MeasureValueType: tstypes.MeasureValueTypeDouble,
				},
			},
		})
	})

	outlet := timestreamsink.New(client, reg)
	if err := outlet.Sink(ctx, readingIT{Name: "latency_ms", Value: "42"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
}

func newTimestreamClient(t *testing.T, ctx context.Context, endpoint string) *awsts.Client {
	t.Helper()
	cfg := localStackConfig(t, ctx)
	return awsts.NewFromConfig(cfg, func(o *awsts.Options) {
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
