// SPDX-License-Identifier: Apache-2.0

package eventbridge_test

import (
	"context"
	"strings"
	"testing"

	awseb "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	csink "github.com/stablekernel/crucible/sink"
	ebsink "github.com/stablekernel/crucible/sink/eventbridge"
)

// TestPutEvents_NilErrorCodeSkipped verifies that entries with a nil ErrorCode
// are skipped when building the detail list, while the reported count still
// reflects the authoritative FailedEntryCount rather than the number of entries
// that happened to carry a code.
func TestPutEvents_NilErrorCodeSkipped(t *testing.T) {
	t.Parallel()

	c := &fakeClient{
		output: &awseb.PutEventsOutput{
			FailedEntryCount: 2,
			Entries: []types.PutEventsResultEntry{
				{EventId: ptr("ok-1")}, // succeeded: no ErrorCode
				{ErrorCode: ptr("ThrottlingException"), ErrorMessage: ptr("slow down")}, // failed
				{}, // failed but no code reported
			},
		},
	}
	reg := ebsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[ebsink.Client] {
		return ebsink.PutEvent("orders", "src", "OrderPlaced", `{}`)
	})
	outlet := ebsink.New(c, reg)

	err := outlet.Sink(context.Background(), orderPlaced{OrderID: "ORD-1"})
	if err == nil {
		t.Fatal("Sink() = nil, want partial failure error")
	}
	msg := err.Error()
	// The count comes from FailedEntryCount (2), not from the single entry that
	// carried an ErrorCode.
	if !strings.Contains(msg, "2 entries failed") {
		t.Errorf("error = %q, want the authoritative count %q", msg, "2 entries failed")
	}
	// The nil-code entry is skipped, so only the coded detail appears.
	if !strings.Contains(msg, "ThrottlingException") {
		t.Errorf("error = %q, want the coded entry detail", msg)
	}
	if strings.Contains(msg, "ok-1") {
		t.Errorf("error = %q, should not mention the succeeded entry", msg)
	}
}

// TestPutEvents_SingleFailureSingularPlural locks the singular plural arm so a
// single failed entry reads "1 entry failed".
func TestPutEvents_SingleFailureSingularPlural(t *testing.T) {
	t.Parallel()

	c := &fakeClient{
		output: &awseb.PutEventsOutput{
			FailedEntryCount: 1,
			Entries: []types.PutEventsResultEntry{
				{ErrorCode: ptr("InternalFailure"), ErrorMessage: ptr("retry")},
			},
		},
	}
	reg := ebsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[ebsink.Client] {
		return ebsink.PutEvent("orders", "src", "OrderPlaced", `{}`)
	})
	outlet := ebsink.New(c, reg)

	err := outlet.Sink(context.Background(), orderPlaced{OrderID: "ORD-2"})
	if err == nil {
		t.Fatal("Sink() = nil, want failure error")
	}
	if !strings.Contains(err.Error(), "1 entry failed") {
		t.Errorf("error = %q, want the singular form %q", err.Error(), "1 entry failed")
	}
}
