// SPDX-License-Identifier: Apache-2.0

package sns_test

import (
	"context"
	"strings"
	"testing"

	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	snssink "github.com/stablekernel/crucible/sink/sns"
)

// batchFailureClient returns a configurable Failed list so the PublishBatch
// partial-failure path is exercised. SNS surfaces such a batch as HTTP 200, so
// only inspecting the response detects it.
type batchFailureClient struct {
	failed []types.BatchResultErrorEntry
}

func (b *batchFailureClient) Publish(context.Context, *awssns.PublishInput, ...func(*awssns.Options)) (*awssns.PublishOutput, error) {
	return &awssns.PublishOutput{}, nil
}

func (b *batchFailureClient) PublishBatch(context.Context, *awssns.PublishBatchInput, ...func(*awssns.Options)) (*awssns.PublishBatchOutput, error) {
	return &awssns.PublishBatchOutput{Failed: b.failed}, nil
}

func TestPublishBatch_PartialFailureReturnsError(t *testing.T) {
	t.Parallel()

	c := &batchFailureClient{
		failed: []types.BatchResultErrorEntry{
			{Id: ptr("1"), Code: ptr("InternalError"), SenderFault: false},
			{Id: ptr("2"), Code: ptr("InvalidParameter"), SenderFault: true},
		},
	}
	op := snssink.PublishBatch(&awssns.PublishBatchInput{
		TopicArn: ptr("arn:aws:sns:us-east-1:123456789012:orders"),
		PublishBatchRequestEntries: []types.PublishBatchRequestEntry{
			{Id: ptr("1"), Message: ptr("a")},
			{Id: ptr("2"), Message: ptr("b")},
		},
	})

	err := op.Apply(context.Background(), c)
	if err == nil {
		t.Fatal("Apply() = nil, want a partial-batch failure error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "2 batch entries failed") {
		t.Errorf("error = %q, want the plural count", msg)
	}
	if !strings.Contains(msg, "1(InternalError)") || !strings.Contains(msg, "2(InvalidParameter)") {
		t.Errorf("error = %q, want both failed ids/codes listed", msg)
	}
}

func TestPublishBatch_SingleFailureSingularPlural(t *testing.T) {
	t.Parallel()

	c := &batchFailureClient{
		failed: []types.BatchResultErrorEntry{{Id: ptr("only"), Code: ptr("Throttled")}},
	}
	op := snssink.PublishBatch(&awssns.PublishBatchInput{
		TopicArn:                   ptr("arn:aws:sns:us-east-1:123456789012:orders"),
		PublishBatchRequestEntries: []types.PublishBatchRequestEntry{{Id: ptr("only"), Message: ptr("x")}},
	})
	err := op.Apply(context.Background(), c)
	if err == nil {
		t.Fatal("Apply() = nil, want a single-entry failure error")
	}
	if !strings.Contains(err.Error(), "1 batch entry failed") {
		t.Errorf("error = %q, want the singular form", err.Error())
	}
}

func TestPublishBatch_NoFailuresSucceeds(t *testing.T) {
	t.Parallel()

	c := &batchFailureClient{}
	op := snssink.PublishBatch(&awssns.PublishBatchInput{
		TopicArn:                   ptr("arn:aws:sns:us-east-1:123456789012:orders"),
		PublishBatchRequestEntries: []types.PublishBatchRequestEntry{{Id: ptr("1"), Message: ptr("ok")}},
	})
	if err := op.Apply(context.Background(), c); err != nil {
		t.Fatalf("Apply() = %v, want nil when no entries fail", err)
	}
}
