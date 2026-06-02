// SPDX-License-Identifier: Apache-2.0

package gcppubsub

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/pubsub/v2"
)

// TestAdapterPassesMessageThrough verifies the adapter builds a Message from
// data and attributes and returns the bridged publish result. It fakes the
// publish seam so no live topic or result handle is required.
func TestAdapterPassesMessageThrough(t *testing.T) {
	t.Parallel()

	var got *pubsub.Message
	a := adapter{publish: func(_ context.Context, msg *pubsub.Message) (string, error) {
		got = msg
		return "srv-123", nil
	}}

	attrs := map[string]string{"k": "v"}
	id, err := a.Publish(context.Background(), []byte("hello"), attrs)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if id != "srv-123" {
		t.Fatalf("id = %q, want srv-123", id)
	}
	if got == nil || string(got.Data) != "hello" {
		t.Fatalf("message data = %v, want hello", got)
	}
	if got.Attributes["k"] != "v" {
		t.Fatalf("message attrs = %v, want k=v", got.Attributes)
	}
}

// TestAdapterPropagatesError verifies the adapter surfaces the bridged publish
// error unchanged so the Emitter can wrap it.
func TestAdapterPropagatesError(t *testing.T) {
	t.Parallel()

	boom := errors.New("publish failed")
	a := adapter{publish: func(_ context.Context, _ *pubsub.Message) (string, error) {
		return "", boom
	}}

	if _, err := a.Publish(context.Background(), nil, nil); !errors.Is(err, boom) {
		t.Fatalf("Publish() = %v, want %v", err, boom)
	}
}
