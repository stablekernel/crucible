// SPDX-License-Identifier: Apache-2.0

package sink_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stablekernel/crucible/sink"
)

// TestManifoldConcurrentSinkAndAttach exercises the copy-on-write snapshot path:
// Sink reads the outlet slice without copying it, so a concurrent Attach must
// replace the slice rather than mutate the one a fan-out is iterating. Run under
// -race, this fails if Attach ever mutates a slice a Sink is reading.
func TestManifoldConcurrentSinkAndAttach(t *testing.T) {
	t.Parallel()

	m := sink.NewManifold().Attach(sink.NewBucket())

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			m.Sink(context.Background(), "payload")
		}()
		go func() {
			defer wg.Done()
			m.Attach(sink.NewBucket())
		}()
	}
	wg.Wait()
}
