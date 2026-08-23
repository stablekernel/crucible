// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/stablekernel/crucible/source"
)

// metaReqPoller embeds the fakePoller and dispatches Request by protocol type:
// a *kmsg.MetadataRequest is answered with metaResp and a
// *kmsg.ListOffsetsRequest with listResp, so the cold-group discovery path
// (metadata first, then list offsets) is exercised without a broker. Every
// request is recorded in requests for type-count assertions.
type metaReqPoller struct {
	fakePoller
	committedMap  map[string]map[int32]kgo.EpochOffset
	consumeTopics []string
	metaResp      *kmsg.MetadataResponse
	listResp      *kmsg.ListOffsetsResponse
	requests      []kmsg.Request
}

func (m *metaReqPoller) Request(_ context.Context, req kmsg.Request) (kmsg.Response, error) {
	m.requests = append(m.requests, req)
	switch req.(type) {
	case *kmsg.MetadataRequest:
		if m.metaResp == nil {
			return nil, errors.New("unexpected metadata request")
		}
		return m.metaResp, nil
	case *kmsg.ListOffsetsRequest:
		if m.listResp == nil {
			return nil, errors.New("unexpected list offsets request")
		}
		return m.listResp, nil
	default:
		return nil, errors.New("unexpected request type")
	}
}

func (m *metaReqPoller) CommittedOffsets() map[string]map[int32]kgo.EpochOffset {
	return m.committedMap
}

func (m *metaReqPoller) GetConsumeTopics() []string { return m.consumeTopics }

// countRequests tallies recorded requests by concrete type.
func countRequests(reqs []kmsg.Request) (meta, list int) {
	for _, r := range reqs {
		switch r.(type) {
		case *kmsg.MetadataRequest:
			meta++
		case *kmsg.ListOffsetsRequest:
			list++
		}
	}
	return meta, list
}

// metadataOrders builds a MetadataResponse declaring one topic with the given
// partition ids.
func metadataOrders(topic string, partitions ...int32) *kmsg.MetadataResponse {
	ps := make([]kmsg.MetadataResponseTopicPartition, 0, len(partitions))
	for _, p := range partitions {
		ps = append(ps, kmsg.MetadataResponseTopicPartition{Partition: p})
	}
	return &kmsg.MetadataResponse{
		Topics: []kmsg.MetadataResponseTopic{{
			Topic:      kmsg.StringPtr(topic),
			Partitions: ps,
		}},
	}
}

// TestSeekToStartColdGroupDiscoversPartitions pins the P1-4 fix: a group with
// nothing committed discovers its partitions from broker metadata instead of
// silently no-oping.
func TestSeekToStartColdGroupDiscoversPartitions(t *testing.T) {
	t.Parallel()

	rp := &metaReqPoller{
		consumeTopics: []string{"orders"},
		metaResp:      metadataOrders("orders", 0, 1, 2),
	}
	sub := newSub(rp)

	if err := sub.SeekToStart(context.Background()); err != nil {
		t.Fatalf("SeekToStart() error = %v", err)
	}
	if len(rp.setOffsets) != 1 {
		t.Fatalf("setOffsets calls = %d, want 1", len(rp.setOffsets))
	}
	set := rp.setOffsets[0]["orders"]
	if len(set) != 3 {
		t.Fatalf("partitions seeked = %d, want 3", len(set))
	}
	for _, p := range []int32{0, 1, 2} {
		got := set[p]
		if got.Offset != -2 || got.Epoch != -1 {
			t.Errorf("partition %d = %+v, want offset -2 epoch -1", p, got)
		}
	}
	meta, list := countRequests(rp.requests)
	if meta != 1 || list != 0 {
		t.Errorf("requests = (metadata %d, list offsets %d), want (1, 0)", meta, list)
	}
}

// TestSeekToEndUsesCommittedWhenPresent proves the committed-offsets fast path:
// no broker requests at all when every assigned topic has commits.
func TestSeekToEndUsesCommittedWhenPresent(t *testing.T) {
	t.Parallel()

	rp := &metaReqPoller{committedMap: map[string]map[int32]kgo.EpochOffset{
		"orders": {0: {Offset: 5}, 1: {Offset: 9}},
	}}
	sub := newSub(rp)

	if err := sub.SeekToEnd(context.Background()); err != nil {
		t.Fatalf("SeekToEnd() error = %v", err)
	}
	if len(rp.setOffsets) != 1 {
		t.Fatalf("setOffsets calls = %d, want 1", len(rp.setOffsets))
	}
	set := rp.setOffsets[0]["orders"]
	for _, p := range []int32{0, 1} {
		if set[p].Offset != -1 || set[p].Epoch != -1 {
			t.Errorf("partition %d = %+v, want offset -1 epoch -1", p, set[p])
		}
	}
	if meta, list := countRequests(rp.requests); meta != 0 || list != 0 {
		t.Errorf("requests = (metadata %d, list offsets %d), want none: committed partitions must be reused", meta, list)
	}
}

// TestSeekToTimeColdGroupMixedTopics covers the mixed case: one topic with a
// commit (reuse its committed partition set) and one cold topic (discover from
// metadata), then resolve timestamps across both via ListOffsets.
func TestSeekToTimeColdGroupMixedTopics(t *testing.T) {
	t.Parallel()

	rp := &metaReqPoller{
		committedMap:  map[string]map[int32]kgo.EpochOffset{"a": {0: {Offset: 3}}},
		consumeTopics: []string{"a", "b"},
		metaResp: &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{
			{Topic: kmsg.StringPtr("a"), Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0}}},
			{Topic: kmsg.StringPtr("b"), Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0}, {Partition: 1}}},
		}},
		listResp: &kmsg.ListOffsetsResponse{Topics: []kmsg.ListOffsetsResponseTopic{
			{Topic: "a", Partitions: []kmsg.ListOffsetsResponseTopicPartition{{Partition: 0, Offset: 10}}},
			{Topic: "b", Partitions: []kmsg.ListOffsetsResponseTopicPartition{
				{Partition: 0, Offset: 20}, {Partition: 1, Offset: 30},
			}},
		}},
	}
	sub := newSub(rp)

	if err := sub.SeekToTime(context.Background(), time.Unix(1000, 0)); err != nil {
		t.Fatalf("SeekToTime() error = %v", err)
	}
	if len(rp.setOffsets) != 1 {
		t.Fatalf("setOffsets calls = %d, want 1", len(rp.setOffsets))
	}
	set := rp.setOffsets[0]
	if got := set["a"][0].Offset; got != 10 {
		t.Errorf("a/0 offset = %d, want 10", got)
	}
	if got := set["b"][0].Offset; got != 20 {
		t.Errorf("b/0 offset = %d, want 20", got)
	}
	if got := set["b"][1].Offset; got != 30 {
		t.Errorf("b/1 offset = %d, want 30", got)
	}
	if meta, list := countRequests(rp.requests); meta != 1 || list != 1 {
		t.Errorf("requests = (metadata %d, list offsets %d), want (1, 1)", meta, list)
	}
}

// TestLagColdGroupErrorsWithSentinel pins the Lag half of P1-4: before the
// first commit there is no baseline, so Lag reports ErrNoCommittedOffsets
// rather than a misleading zero.
func TestLagColdGroupErrorsWithSentinel(t *testing.T) {
	t.Parallel()

	rp := &metaReqPoller{consumeTopics: []string{"orders"}}
	sub := newSub(rp)

	_, err := sub.Lag(context.Background())
	if !errors.Is(err, ErrNoCommittedOffsets) {
		t.Fatalf("Lag() error = %v, want errors.Is ErrNoCommittedOffsets", err)
	}
}

// TestLagPartialCommitsCountOnlyCommitted verifies lag counts only partitions
// with a committed baseline; an uncommitted consume topic contributes nothing
// and triggers no metadata discovery on the lag path.
func TestLagPartialCommitsCountOnlyCommitted(t *testing.T) {
	t.Parallel()

	rp := &metaReqPoller{
		committedMap:  map[string]map[int32]kgo.EpochOffset{"a": {0: {Offset: 10}}},
		consumeTopics: []string{"a", "b"},
		listResp: &kmsg.ListOffsetsResponse{Topics: []kmsg.ListOffsetsResponseTopic{
			{Topic: "a", Partitions: []kmsg.ListOffsetsResponseTopicPartition{{Partition: 0, Offset: 15}}},
		}},
	}
	sub := newSub(rp)

	lag, err := sub.Lag(context.Background())
	if err != nil {
		t.Fatalf("Lag() error = %v", err)
	}
	if lag != 5 {
		t.Errorf("lag = %d, want 5", lag)
	}
	if meta, list := countRequests(rp.requests); meta != 0 || list != 1 {
		t.Errorf("requests = (metadata %d, list offsets %d), want (0, 1)", meta, list)
	}
}

// TestTopicPartitionsErrorPropagates proves a metadata-level failure (unknown
// topic, error code 3) surfaces from SeekToStart instead of being swallowed.
func TestTopicPartitionsErrorPropagates(t *testing.T) {
	t.Parallel()

	rp := &metaReqPoller{
		consumeTopics: []string{"missing"},
		metaResp: &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{{
			Topic:     kmsg.StringPtr("missing"),
			ErrorCode: 3,
		}}},
	}
	sub := newSub(rp)

	err := sub.SeekToStart(context.Background())
	if err == nil {
		t.Fatal("SeekToStart() = nil, want a metadata error")
	}
	var ke *kmsgError
	if !errors.As(err, &ke) || ke.code != 3 {
		t.Fatalf("error = %v, want kmsgError code 3", err)
	}
}

// Compile-time reminder that Seekable/LagReporter stay satisfied by the
// subscription while the cold-group paths evolve.
var (
	_ source.Seekable    = (*subscription)(nil)
	_ source.LagReporter = (*subscription)(nil)
)
