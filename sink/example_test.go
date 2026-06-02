// SPDX-License-Identifier: Apache-2.0

package sink_test

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/sink"
)

// orderPlaced is a sample domain event a service might emit.
type orderPlaced struct {
	ID string
}

func ExampleManifold_Sink() {
	bucket := sink.NewBucket()
	m := sink.NewManifold(sink.WithOutlets(bucket))

	m.Sink(context.Background(), orderPlaced{ID: "A-1"})
	m.Sink(context.Background(), orderPlaced{ID: "A-2"})

	for _, o := range sink.RecordsOf[orderPlaced](bucket) {
		fmt.Println(o.ID)
	}
	// Output:
	// A-1
	// A-2
}

// inMemoryStore is a stand-in destination client.
type inMemoryStore struct {
	saved []string
}

func (s *inMemoryStore) save(id string) { s.saved = append(s.saved, id) }

func ExampleNewEmitter() {
	store := &inMemoryStore{}
	reg := sink.NewRegistry[sink.Op[*inMemoryStore]]()
	sink.Register(reg, func(_ context.Context, o orderPlaced) sink.Op[*inMemoryStore] {
		return sink.OpFunc[*inMemoryStore](func(_ context.Context, s *inMemoryStore) error {
			s.save(o.ID)
			return nil
		})
	})

	emitter := sink.NewEmitter[*inMemoryStore](store, reg, sink.WithName("store"))
	_ = emitter.Sink(context.Background(), orderPlaced{ID: "B-1"})

	fmt.Println(store.saved)
	// Output: [B-1]
}

func ExampleReservoir() {
	bucket := sink.NewBucket()
	// Buffer until three payloads accumulate, then release as a batch.
	batched := sink.Reservoir(bucket, sink.WithBatchSize(3), sink.WithBatchInterval(0))

	for i := 1; i <= 3; i++ {
		_ = batched.Sink(context.Background(), i)
	}

	fmt.Println(len(bucket.All()))
	// Output: 3
}
