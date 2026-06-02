// SPDX-License-Identifier: Apache-2.0

package sink_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/sink"
)

type fakeClient struct {
	applied []string
}

func TestOpFuncAppliesThroughToClient(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	var op sink.Op[*fakeClient] = sink.OpFunc[*fakeClient](func(_ context.Context, client *fakeClient) error {
		client.applied = append(client.applied, "put")
		return nil
	})

	if err := op.Apply(context.Background(), c); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if len(c.applied) != 1 || c.applied[0] != "put" {
		t.Fatalf("client.applied = %v, want [put]", c.applied)
	}
}

func TestOpFuncPropagatesError(t *testing.T) {
	t.Parallel()

	want := errors.New("write failed")
	op := sink.OpFunc[*fakeClient](func(_ context.Context, _ *fakeClient) error { return want })
	if err := op.Apply(context.Background(), &fakeClient{}); !errors.Is(err, want) {
		t.Fatalf("Apply() = %v, want %v", err, want)
	}
}
