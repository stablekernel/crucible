// SPDX-License-Identifier: Apache-2.0

package s3_test

import (
	"context"
	"errors"
	"io"
	"testing"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	csink "github.com/stablekernel/crucible/sink"
	s3sink "github.com/stablekernel/crucible/sink/s3"
	"github.com/stablekernel/crucible/sink/sinktest"
)

// fakeClient is a hand-rolled Client implementation — no AWS credentials,
// no network, no mockery.
type fakeClient struct {
	puts    []putCall
	deletes []deleteCall
	err     error
}

type putCall struct {
	bucket string
	key    string
	body   []byte
}

type deleteCall struct {
	bucket string
	key    string
}

func (f *fakeClient) PutObject(_ context.Context, params *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	var body []byte
	if params.Body != nil {
		body, _ = io.ReadAll(params.Body)
	}
	f.puts = append(f.puts, putCall{
		bucket: deref(params.Bucket),
		key:    deref(params.Key),
		body:   body,
	})
	return &awss3.PutObjectOutput{}, nil
}

func (f *fakeClient) DeleteObject(_ context.Context, params *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.deletes = append(f.deletes, deleteCall{
		bucket: deref(params.Bucket),
		key:    deref(params.Key),
	})
	return &awss3.DeleteObjectOutput{}, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// payloadPut and payloadDelete are domain types used in tests.
type payloadPut struct {
	Bucket  string
	Key     string
	Content []byte
}

type payloadDelete struct {
	Bucket string
	Key    string
}

func newPutOutlet(c s3sink.Client) csink.Outlet {
	reg := s3sink.NewRegistry()
	csink.Register(reg, func(_ context.Context, p payloadPut) csink.Op[s3sink.Client] {
		return s3sink.PutObject(p.Bucket, p.Key, p.Content)
	})
	return s3sink.New(c, reg)
}

func newDeleteOutlet(c s3sink.Client) csink.Outlet {
	reg := s3sink.NewRegistry()
	csink.Register(reg, func(_ context.Context, p payloadDelete) csink.Op[s3sink.Client] {
		return s3sink.DeleteObject(p.Bucket, p.Key)
	})
	return s3sink.New(c, reg)
}

func newCombinedOutlet(c s3sink.Client) csink.Outlet {
	reg := s3sink.NewRegistry()
	csink.Register(reg, func(_ context.Context, p payloadPut) csink.Op[s3sink.Client] {
		return s3sink.PutObject(p.Bucket, p.Key, p.Content)
	})
	csink.Register(reg, func(_ context.Context, p payloadDelete) csink.Op[s3sink.Client] {
		return s3sink.DeleteObject(p.Bucket, p.Key)
	})
	return s3sink.New(c, reg)
}

func TestPutObjectUploadsToCorrectBucketAndKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload payloadPut
	}{
		{
			name:    "simple object",
			payload: payloadPut{Bucket: "my-bucket", Key: "uploads/file.txt", Content: []byte("hello")},
		},
		{
			name:    "empty body",
			payload: payloadPut{Bucket: "other-bucket", Key: "empty.bin", Content: nil},
		},
		{
			name:    "nested key",
			payload: payloadPut{Bucket: "b", Key: "a/b/c/d.json", Content: []byte(`{}`)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := &fakeClient{}
			outlet := newPutOutlet(c)
			if err := outlet.Sink(context.Background(), tc.payload); err != nil {
				t.Fatalf("Sink() error = %v", err)
			}
			if len(c.puts) != 1 {
				t.Fatalf("PutObject calls = %d, want 1", len(c.puts))
			}
			got := c.puts[0]
			if got.bucket != tc.payload.Bucket {
				t.Errorf("bucket = %q, want %q", got.bucket, tc.payload.Bucket)
			}
			if got.key != tc.payload.Key {
				t.Errorf("key = %q, want %q", got.key, tc.payload.Key)
			}
			if string(got.body) != string(tc.payload.Content) {
				t.Errorf("body = %q, want %q", got.body, tc.payload.Content)
			}
		})
	}
}

func TestDeleteObjectRemovesCorrectKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload payloadDelete
	}{
		{
			name:    "simple delete",
			payload: payloadDelete{Bucket: "my-bucket", Key: "uploads/old.txt"},
		},
		{
			name:    "nested key delete",
			payload: payloadDelete{Bucket: "archive", Key: "2024/01/02/log.gz"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := &fakeClient{}
			outlet := newDeleteOutlet(c)
			if err := outlet.Sink(context.Background(), tc.payload); err != nil {
				t.Fatalf("Sink() error = %v", err)
			}
			if len(c.deletes) != 1 {
				t.Fatalf("DeleteObject calls = %d, want 1", len(c.deletes))
			}
			got := c.deletes[0]
			if got.bucket != tc.payload.Bucket {
				t.Errorf("bucket = %q, want %q", got.bucket, tc.payload.Bucket)
			}
			if got.key != tc.payload.Key {
				t.Errorf("key = %q, want %q", got.key, tc.payload.Key)
			}
		})
	}
}

func TestPutObjectWithPassesInputAsIs(t *testing.T) {
	t.Parallel()

	bucket := "custom-bucket"
	key := "custom/key.txt"
	c := &fakeClient{}
	reg := s3sink.NewRegistry()
	csink.Register(reg, func(_ context.Context, p payloadPut) csink.Op[s3sink.Client] {
		return s3sink.PutObjectWith(&awss3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
	})
	outlet := s3sink.New(c, reg)
	if err := outlet.Sink(context.Background(), payloadPut{}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.puts) != 1 || c.puts[0].bucket != bucket || c.puts[0].key != key {
		t.Fatalf("PutObjectWith did not forward input: %+v", c.puts)
	}
}

func TestDeleteObjectWithPassesInputAsIs(t *testing.T) {
	t.Parallel()

	bucket := "custom-bucket"
	key := "custom/key.txt"
	c := &fakeClient{}
	reg := s3sink.NewRegistry()
	csink.Register(reg, func(_ context.Context, p payloadDelete) csink.Op[s3sink.Client] {
		return s3sink.DeleteObjectWith(&awss3.DeleteObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
	})
	outlet := s3sink.New(c, reg)
	if err := outlet.Sink(context.Background(), payloadDelete{}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(c.deletes) != 1 || c.deletes[0].bucket != bucket || c.deletes[0].key != key {
		t.Fatalf("DeleteObjectWith did not forward input: %+v", c.deletes)
	}
}

func TestUnregisteredPayloadSkips(t *testing.T) {
	t.Parallel()

	type other struct{}
	c := &fakeClient{}
	err := newPutOutlet(c).Sink(context.Background(), other{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestPutObjectErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("access denied")
	c := &fakeClient{err: boom}
	err := newPutOutlet(c).Sink(context.Background(), payloadPut{Bucket: "b", Key: "k", Content: []byte("x")})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "s3" {
		t.Fatalf("recovered = %+v, want *sink.Error{Outlet:s3, Phase:apply}", se)
	}
}

func TestDeleteObjectErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("no such key")
	c := &fakeClient{err: boom}
	err := newDeleteOutlet(c).Sink(context.Background(), payloadDelete{Bucket: "b", Key: "k"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "s3" {
		t.Fatalf("recovered = %+v, want *sink.Error{Outlet:s3, Phase:apply}", se)
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newCombinedOutlet(&fakeClient{}) })
}
