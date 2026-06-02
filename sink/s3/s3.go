// SPDX-License-Identifier: Apache-2.0

// Package s3 is a sink destination that persists payloads to Amazon S3 via
// the AWS SDK v2. Register a transformer that turns each payload type into a
// [PutObject] or [DeleteObject] operation, then attach the result of [New] to
// a sink.Manifold.
//
// The [Client] interface is narrow: it declares only the two S3 methods this
// destination actually calls. The real [*s3.Client] satisfies it structurally,
// so no adapter is required in production and hand-rolled fakes are trivial in
// tests.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package s3

import (
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	csink "github.com/stablekernel/crucible/sink"
)

// Client is the narrow S3 surface this destination needs. It is satisfied by
// the real [*s3.Client] without any adapter.
type Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// PutObject returns an Op that uploads body to the given bucket and key.
// ContentType is left unset; callers that need it should use [PutObjectWith]
// and populate the input directly.
func PutObject(bucket, key string, body []byte) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   bytes.NewReader(body),
		})
		return err
	})
}

// PutObjectWith returns an Op that calls PutObject with a pre-built
// [*s3.PutObjectInput]. The input is used as-is; callers own its fields.
func PutObjectWith(input *s3.PutObjectInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.PutObject(ctx, input)
		return err
	})
}

// DeleteObject returns an Op that removes the object at bucket/key.
func DeleteObject(bucket, key string) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		return err
	})
}

// DeleteObjectWith returns an Op that calls DeleteObject with a pre-built
// [*s3.DeleteObjectInput]. The input is used as-is; callers own its fields.
func DeleteObjectWith(input *s3.DeleteObjectInput) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		_, err := c.DeleteObject(ctx, input)
		return err
	})
}

// NewRegistry returns an empty registry of Op[Client] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Client]] {
	return csink.NewRegistry[csink.Op[Client]]()
}

// New builds an Outlet that applies each payload's registered Op[Client] to
// client. The outlet is named "s3" unless overridden with sink.WithName.
func New(client Client, reg *csink.Registry[csink.Op[Client]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Client](client, reg, append([]csink.EmitterOption{csink.WithName("s3")}, opts...)...)
}
