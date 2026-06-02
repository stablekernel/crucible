// SPDX-License-Identifier: Apache-2.0

//go:build integration

package kinesis_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

	csink "github.com/stablekernel/crucible/sink"
	kinesissink "github.com/stablekernel/crucible/sink/kinesis"
)

// eventIT is the payload the integration test sinks through the outlet.
type eventIT struct {
	Data string
}

// TestIntegrationSinkPutsRecordInLocalStack starts a LocalStack Kinesis, drives
// the real AWS SDK PutRecord path through the Outlet, then reads the shard back
// to prove the record landed. It skips cleanly when Docker is not reachable.
func TestIntegrationSinkPutsRecordInLocalStack(t *testing.T) {
	t.Parallel()
	skipWithoutDocker(t)

	ctx := context.Background()
	endpoint := startLocalStack(t, ctx)
	client := newKinesisClient(t, ctx, endpoint)

	const stream = "events"
	if _, err := client.CreateStream(ctx, &awskinesis.CreateStreamInput{
		StreamName: aws.String(stream),
		ShardCount: aws.Int32(1),
	}); err != nil {
		t.Fatalf("CreateStream() error = %v", err)
	}
	if err := awskinesis.NewStreamExistsWaiter(client).Wait(ctx,
		&awskinesis.DescribeStreamInput{StreamName: aws.String(stream)}, 30*time.Second); err != nil {
		t.Fatalf("stream did not become active: %v", err)
	}

	reg := kinesissink.NewRegistry()
	csink.Register(reg, func(_ context.Context, e eventIT) csink.Op[kinesissink.Client] {
		return kinesissink.PutRecordOf(kinesissink.PutRecordParams{
			StreamName:   stream,
			PartitionKey: "pk-1",
			Data:         []byte(e.Data),
		})
	})

	outlet := kinesissink.New(client, reg)
	if err := outlet.Sink(ctx, eventIT{Data: "hello"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	if got := readFirstRecord(t, ctx, client, stream); string(got) != "hello" {
		t.Fatalf("read record = %q, want hello", got)
	}
}

func readFirstRecord(t *testing.T, ctx context.Context, client *awskinesis.Client, stream string) []byte {
	t.Helper()
	desc, err := client.DescribeStream(ctx, &awskinesis.DescribeStreamInput{StreamName: aws.String(stream)})
	if err != nil {
		t.Fatalf("DescribeStream() error = %v", err)
	}
	shardID := desc.StreamDescription.Shards[0].ShardId

	it, err := client.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{
		StreamName:        aws.String(stream),
		ShardId:           shardID,
		ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatalf("GetShardIterator() error = %v", err)
	}

	iterator := it.ShardIterator
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		out, gerr := client.GetRecords(ctx, &awskinesis.GetRecordsInput{ShardIterator: iterator})
		if gerr != nil {
			t.Fatalf("GetRecords() error = %v", gerr)
		}
		if len(out.Records) > 0 {
			return out.Records[0].Data
		}
		iterator = out.NextShardIterator
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no records read from stream %q before deadline", stream)
	return nil
}

func newKinesisClient(t *testing.T, ctx context.Context, endpoint string) *awskinesis.Client {
	t.Helper()
	cfg := localStackConfig(t, ctx)
	return awskinesis.NewFromConfig(cfg, func(o *awskinesis.Options) {
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
