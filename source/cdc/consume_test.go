// SPDX-License-Identifier: Apache-2.0

package cdc_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/cdc"
	"github.com/stablekernel/crucible/source/memsource"
)

// TestConsume_DebeziumTopic exercises the documented pattern end-to-end with no
// broker: a memsource inlet stands in for a Debezium topic, a Hopper drives a
// handler that decodes each change event with the codec, and the ledger
// confirms every message was acked. A tombstone is skipped rather than failed.
func TestConsume_DebeziumTopic(t *testing.T) {
	t.Parallel()

	type user struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}

	registry := source.NewRegistry().SetDefault(cdc.New())

	var applied []string // ordered record of what the handler did per key
	handler := func(_ context.Context, m source.Message) source.Result {
		ev, err := cdc.DecodeEvent(registry, m)
		if err != nil {
			return source.Term(err)
		}
		switch ev.Operation {
		case cdc.OpTombstone:
			return source.Skip()
		case cdc.OpDelete:
			applied = append(applied, "delete")
			return source.Ack()
		default:
			row, err := cdc.AfterAs[user](ev)
			if err != nil {
				return source.Term(err)
			}
			applied = append(applied, row.Name)
			return source.Ack()
		}
	}

	h := memsource.NewHarness(t, nil,
		memsource.Msg{Key: "1", Value: []byte(`{"op":"c","after":{"id":1,"name":"ada"}}`)},
		memsource.Msg{Key: "1", Value: []byte(`{"op":"u","after":{"id":1,"name":"ada lovelace"}}`)},
		memsource.Msg{Key: "1", Value: []byte(`{"op":"d","before":{"id":1,"name":"ada lovelace"}}`)},
		memsource.Msg{Key: "1", Value: nil}, // tombstone
	)
	h.Run(handler)

	h.AssertCounts(memsource.Counts{Acked: 3, Dropped: 1})

	want := []string{"ada", "ada lovelace", "delete"}
	if len(applied) != len(want) {
		t.Fatalf("applied = %v, want %v", applied, want)
	}
	for i := range want {
		if applied[i] != want[i] {
			t.Fatalf("applied[%d] = %q, want %q", i, applied[i], want[i])
		}
	}
}
