// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/memsource"
)

// FuzzOrderedSettle hammers the core ordering invariant: across any interleaving
// of keys and any concurrency bound, every message is handled exactly once, none
// is lost or duplicated, and within a single key the messages are processed in
// the order they were delivered.
func FuzzOrderedSettle(f *testing.F) {
	f.Add(uint8(10), uint8(3), uint8(4))
	f.Add(uint8(64), uint8(8), uint8(1))
	f.Add(uint8(1), uint8(1), uint8(1))
	f.Add(uint8(100), uint8(16), uint8(7))
	f.Add(uint8(0), uint8(4), uint8(2))

	f.Fuzz(func(t *testing.T, count, conc, keyspace uint8) {
		if conc == 0 {
			conc = 1
		}
		if keyspace == 0 {
			keyspace = 1
		}

		// Build count messages round-robin across keyspace keys; each value is a
		// per-key monotonic sequence number.
		perKeyCount := make(map[string]int)
		var msgs []memsource.Msg
		for i := 0; i < int(count); i++ {
			key := "k" + strconv.Itoa(i%int(keyspace))
			seq := perKeyCount[key]
			perKeyCount[key]++
			msgs = append(msgs, memsource.Msg{
				Key:   key,
				Value: []byte(strconv.Itoa(seq)),
			})
		}

		var mu sync.Mutex
		processed := make(map[string][]int) // key -> sequence numbers in process order

		h := memsource.NewHarness(
			t,
			[]source.Option{source.WithConcurrency(int(conc))},
			msgs...,
		)
		h.Run(func(_ context.Context, m source.Message) source.Result {
			seq, err := strconv.Atoi(string(m.Value()))
			if err != nil {
				return source.Term(err)
			}
			mu.Lock()
			processed[string(m.Key())] = append(processed[string(m.Key())], seq)
			mu.Unlock()
			return source.Ack()
		})

		if got := h.Ledger().Len(); got != int(count) {
			t.Fatalf("settled %d messages, want %d (no loss or duplication)", got, count)
		}

		// Per key, the processed sequence must be exactly 0,1,2,... in order.
		for key, want := range perKeyCount {
			got := processed[key]
			if len(got) != want {
				t.Fatalf("key %s processed %d messages, want %d", key, len(got), want)
			}
			for i, seq := range got {
				if seq != i {
					t.Fatalf("key %s out of order: position %d has seq %d", key, i, seq)
				}
			}
		}
	})
}

// FuzzBatchOrderedSettle hammers the batch-mode ordering invariant: across any
// interleaving of keys, any concurrency bound, and any batch size, every message
// is handled exactly once (no loss, no duplication) and within a single key the
// messages are processed in delivery order — the same contract as the per-message
// path, now through accumulated batches.
func FuzzBatchOrderedSettle(f *testing.F) {
	f.Add(uint8(10), uint8(3), uint8(4), uint8(2))
	f.Add(uint8(64), uint8(8), uint8(1), uint8(7))
	f.Add(uint8(1), uint8(1), uint8(1), uint8(1))
	f.Add(uint8(100), uint8(16), uint8(7), uint8(16))
	f.Add(uint8(0), uint8(4), uint8(2), uint8(3))

	f.Fuzz(func(t *testing.T, count, conc, keyspace, size uint8) {
		if conc == 0 {
			conc = 1
		}
		if keyspace == 0 {
			keyspace = 1
		}
		if size == 0 {
			size = 1
		}

		perKeyCount := make(map[string]int)
		var msgs []memsource.Msg
		for i := 0; i < int(count); i++ {
			key := "k" + strconv.Itoa(i%int(keyspace))
			seq := perKeyCount[key]
			perKeyCount[key]++
			msgs = append(msgs, memsource.Msg{
				Key:   key,
				Value: []byte(strconv.Itoa(seq)),
			})
		}

		var mu sync.Mutex
		processed := make(map[string][]int)

		h := memsource.NewHarness(
			t,
			[]source.Option{
				source.WithConcurrency(int(conc)),
				// A short max-wait guarantees forward progress regardless of how the
				// keys distribute across lanes (a lane that cannot fill still flushes).
				source.WithBatch(int(size), time.Millisecond),
			},
			msgs...,
		)
		h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
			res := make([]source.Result, len(ms))
			mu.Lock()
			for i, m := range ms {
				seq, err := strconv.Atoi(string(m.Value()))
				if err != nil {
					res[i] = source.Term(err)
					continue
				}
				processed[string(m.Key())] = append(processed[string(m.Key())], seq)
				res[i] = source.Ack()
			}
			mu.Unlock()
			return res
		})

		if got := h.Ledger().Len(); got != int(count) {
			t.Fatalf("settled %d messages, want %d (no loss or duplication)", got, count)
		}
		for key, want := range perKeyCount {
			got := processed[key]
			if len(got) != want {
				t.Fatalf("key %s processed %d messages, want %d", key, len(got), want)
			}
			for i, seq := range got {
				if seq != i {
					t.Fatalf("key %s out of order: position %d has seq %d", key, i, seq)
				}
			}
		}
	})
}

// FuzzCodecRoundTrip checks the JSON codec round-trip: any pair of fields
// marshals and decodes back to an equal value, and the registry classifies a
// non-JSON payload as a decode failure (poison) rather than panicking.
func FuzzCodecRoundTrip(f *testing.F) {
	f.Add("A-1", 0)
	f.Add("", -5)
	f.Add("x\"y", 1<<30)

	f.Fuzz(func(t *testing.T, id string, qty int) {
		// JSON is a UTF-8 wire format: encoding/json replaces invalid UTF-8 bytes
		// with the replacement rune, so only valid-UTF-8 strings round-trip
		// byte-for-byte. Restrict the invariant to that domain.
		if !utf8.ValidString(id) {
			t.Skip()
		}
		reg := source.NewRegistry().SetDefault(source.NewJSONCodec[order]())
		payload, err := json.Marshal(order{ID: id, Qty: qty})
		if err != nil {
			t.Skip()
		}
		got, err := source.DecodeTyped[order](reg, testMsg{value: payload})
		if err != nil {
			t.Fatalf("round-trip decode failed for {%q,%d}: %v", id, qty, err)
		}
		if got.ID != id || got.Qty != qty {
			t.Fatalf("round-trip = %+v, want {%q,%d}", got, id, qty)
		}

		// A payload that is never valid JSON must classify as poison, never panic.
		if _, derr := reg.Decode(testMsg{value: []byte("\x00not json")}); derr == nil {
			t.Fatal("expected a decode error for non-JSON payload")
		}
	})
}
