// SPDX-License-Identifier: Apache-2.0

//go:build integration

package firehose_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsfirehose "github.com/aws/aws-sdk-go-v2/service/firehose"
	fhtypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

	csink "github.com/stablekernel/crucible/sink"
	firehosesink "github.com/stablekernel/crucible/sink/firehose"
)

// recordIT is the payload the integration test sinks through the outlet.
type recordIT struct {
	Data string
}

// TestIntegrationSinkPutsRecordInLocalStack starts a LocalStack Firehose with
// an S3-backed delivery stream, drives the real AWS SDK PutRecord path through
// the Outlet, then polls the destination bucket to prove the record was
// delivered. It skips cleanly when Docker is not reachable.
func TestIntegrationSinkPutsRecordInLocalStack(t *testing.T) {
	t.Parallel()
	skipWithoutDocker(t)

	ctx := context.Background()
	endpoint := startLocalStack(t, ctx)
	cfg := localStackConfig(t, ctx)

	fhClient := awsfirehose.NewFromConfig(cfg, func(o *awsfirehose.Options) { o.BaseEndpoint = aws.String(endpoint) })
	s3Client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	const (
		bucket = "firehose-out"
		stream = "events"
	)
	if _, err := s3Client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}
	if _, err := fhClient.CreateDeliveryStream(ctx, &awsfirehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String(stream),
		DeliveryStreamType: fhtypes.DeliveryStreamTypeDirectPut,
		S3DestinationConfiguration: &fhtypes.S3DestinationConfiguration{
			BucketARN: aws.String("arn:aws:s3:::" + bucket),
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/firehose-role"),
			BufferingHints: &fhtypes.BufferingHints{
				IntervalInSeconds: aws.Int32(60),
				SizeInMBs:         aws.Int32(1),
			},
		},
	}); err != nil {
		t.Fatalf("CreateDeliveryStream() error = %v", err)
	}
	waitStreamActive(t, ctx, fhClient, stream)

	reg := firehosesink.NewRegistry()
	csink.Register(reg, func(_ context.Context, r recordIT) csink.Op[firehosesink.Client] {
		return firehosesink.PutRecord(&awsfirehose.PutRecordInput{
			DeliveryStreamName: aws.String(stream),
			Record:             &fhtypes.Record{Data: []byte(r.Data)},
		})
	})

	outlet := firehosesink.New(fhClient, reg)
	if err := outlet.Sink(ctx, recordIT{Data: "payload-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	if got := waitForObject(t, ctx, s3Client, bucket); !strings.Contains(got, "payload-1") {
		t.Fatalf("delivered object body = %q, want it to contain payload-1", got)
	}
}

func waitStreamActive(t *testing.T, ctx context.Context, client *awsfirehose.Client, stream string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		out, err := client.DescribeDeliveryStream(ctx, &awsfirehose.DescribeDeliveryStreamInput{
			DeliveryStreamName: aws.String(stream),
		})
		if err != nil {
			t.Fatalf("DescribeDeliveryStream() error = %v", err)
		}
		if out.DeliveryStreamDescription.DeliveryStreamStatus == fhtypes.DeliveryStreamStatusActive {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("delivery stream %q did not become active", stream)
}

func waitForObject(t *testing.T, ctx context.Context, client *awss3.Client, bucket string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		list, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if err != nil {
			t.Fatalf("ListObjectsV2() error = %v", err)
		}
		if len(list.Contents) > 0 {
			obj, gerr := client.GetObject(ctx, &awss3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    list.Contents[0].Key,
			})
			if gerr != nil {
				t.Fatalf("GetObject() error = %v", gerr)
			}
			body, rerr := io.ReadAll(obj.Body)
			_ = obj.Body.Close()
			if rerr != nil {
				t.Fatalf("read delivered object error = %v", rerr)
			}
			return string(body)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("no object delivered to bucket %q before deadline", bucket)
	return ""
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
