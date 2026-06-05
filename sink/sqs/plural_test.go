// SPDX-License-Identifier: Apache-2.0

package sqs_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// TestSendMessageBatchOp_MultiFailurePluralSuffix exercises the multi-failure
// branch of pluralSuffix: more than one failed entry uses the "ies" plural and
// lists every failed id/code pair.
func TestSendMessageBatchOp_MultiFailurePluralSuffix(t *testing.T) {
	t.Parallel()

	c := &fakeClient{
		batchFailed: []types.BatchResultErrorEntry{
			{Id: str("item-0"), Code: str("InvalidMessageContents"), SenderFault: true},
			{Id: str("item-1"), Code: str("ServiceUnavailable")},
		},
	}
	err := newOutlet(c).Sink(context.Background(), payloadC{Items: []string{"a", "b"}})
	if err == nil {
		t.Fatal("Sink() = nil, want a multi-entry batch failure error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "2 batch entries failed") {
		t.Errorf("error = %q, want the plural form %q", msg, "2 batch entries failed")
	}
	if !strings.Contains(msg, "item-0(InvalidMessageContents)") || !strings.Contains(msg, "item-1(ServiceUnavailable)") {
		t.Errorf("error = %q, want both failed entries listed", msg)
	}
}

// TestSendMessageBatchOp_SingleFailureSingularSuffix locks the singular branch
// so the two plural arms stay covered together.
func TestSendMessageBatchOp_SingleFailureSingularSuffix(t *testing.T) {
	t.Parallel()

	c := &fakeClient{
		batchFailed: []types.BatchResultErrorEntry{
			{Id: str("only"), Code: str("Throttled")},
		},
	}
	err := newOutlet(c).Sink(context.Background(), payloadC{Items: []string{"a"}})
	if err == nil {
		t.Fatal("Sink() = nil, want a single-entry batch failure error")
	}
	if !strings.Contains(err.Error(), "1 batch entry failed") {
		t.Errorf("error = %q, want the singular form %q", err.Error(), "1 batch entry failed")
	}
}
