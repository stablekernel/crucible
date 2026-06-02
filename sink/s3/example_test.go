// SPDX-License-Identifier: Apache-2.0

package s3_test

import (
	"context"
	"fmt"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	csink "github.com/stablekernel/crucible/sink"
	s3sink "github.com/stablekernel/crucible/sink/s3"
)

// recordingClient records the keys passed to PutObject and DeleteObject.
type recordingClient struct {
	putKeys    []string
	deleteKeys []string
}

func (r *recordingClient) PutObject(_ context.Context, params *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	if params.Key != nil {
		r.putKeys = append(r.putKeys, *params.Key)
	}
	return &awss3.PutObjectOutput{}, nil
}

func (r *recordingClient) DeleteObject(_ context.Context, params *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	if params.Key != nil {
		r.deleteKeys = append(r.deleteKeys, *params.Key)
	}
	return &awss3.DeleteObjectOutput{}, nil
}

type documentUploaded struct {
	Bucket  string
	Key     string
	Content []byte
}

func ExampleNew() {
	client := &recordingClient{}
	reg := s3sink.NewRegistry()
	csink.Register(reg, func(_ context.Context, d documentUploaded) csink.Op[s3sink.Client] {
		return s3sink.PutObject(d.Bucket, d.Key, d.Content)
	})

	outlet := s3sink.New(client, reg)
	_ = outlet.Sink(context.Background(), documentUploaded{
		Bucket:  "my-bucket",
		Key:     "docs/report.pdf",
		Content: []byte("%PDF-1.4"),
	})

	fmt.Println(client.putKeys[0])
	// Output: docs/report.pdf
}
