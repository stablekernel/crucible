// SPDX-License-Identifier: Apache-2.0

package bridge_test

import (
	"context"
	"fmt"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/bridge"
	"github.com/stablekernel/crucible/state"
)

func ExampleMiddleware() {
	bucket := csink.NewBucket()
	m := csink.NewManifold(csink.WithOutlets(bucket))

	// Install the bridge as middleware: every transition fans out through m.
	machine := state.ForgeFor[*bulb]("switch").
		Use(bridge.Middleware[string, string, *bulb](m)).
		State("off").State("on").
		Initial("off").
		CurrentStateFn(func(b *bulb) string { return b.cur() }).
		Transition("off").On("toggle").GoTo("on").
		Quench(state.Strict())

	machine.Cast(&bulb{}).Fire(context.Background(), "toggle")

	tr := csink.RecordsOf[bridge.Transition](bucket)[0]
	fmt.Printf("%s: %s -> %s\n", tr.Event, tr.From, tr.To)
	// Output: toggle: off -> on
}
