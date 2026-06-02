// SPDX-License-Identifier: Apache-2.0

//go:build integration

package s3_test

import (
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

	csink "github.com/stablekernel/crucible/sink"
	s3sink "github.com/stablekernel/crucible/sink/s3"
)

// reportReadyIT is the payload the integration test sinks through the outlet.
type reportReadyIT struct {
	Key  string
	Body string
}

// TestIntegrationSinkPutsObjectInLocalStack starts a LocalStack S3, drives the
// real AWS SDK PutObject path through the Outlet, then reads the object back to
// prove the write landed. It skips cleanly when Docker is not reachable.
func TestIntegrationSinkPutsObjectInLocalStack(t *testing.T) {
	t.Parallel()
	skipWithoutDocker(t)

	ctx := context.Background()
	endpoint := startLocalStack(t, ctx)
	client := newS3Client(t, ctx, endpoint)

	const bucket = "reports"
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	reg := s3sink.NewRegistry()
	csink.Register(reg, func(_ context.Context, r reportReadyIT) csink.Op[s3sink.Client] {
		return s3sink.PutObject(bucket, r.Key, []byte(r.Body))
	})

	outlet := s3sink.New(client, reg)
	if err := outlet.Sink(ctx, reportReadyIT{Key: "daily/2026-06-02.json", Body: `{"ok":true}`}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("daily/2026-06-02.json"),
	})
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	defer func() { _ = out.Body.Close() }()
	got, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read object body error = %v", err)
	}
	if string(got) != `{"ok":true}` {
		t.Fatalf("persisted object body = %q, want %q", got, `{"ok":true}`)
	}
}

func newS3Client(t *testing.T, ctx context.Context, endpoint string) *awss3.Client {
	t.Helper()
	cfg := localStackConfig(t, ctx)
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
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
