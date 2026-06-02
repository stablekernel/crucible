// SPDX-License-Identifier: Apache-2.0

package statsd

import dogstatsd "github.com/DataDog/datadog-go/v5/statsd"

// This file binds the destination to its one SDK dependency. The narrow Client
// interface is a structural subset of the SDK's client surface; the assertion
// below makes that a compile-time guarantee, so a future SDK signature change
// fails the build here rather than silently at a call site.
var _ Client = dogstatsd.ClientInterface(nil)

// Dial opens an SDK client to addr (for example "127.0.0.1:8125") and returns
// it as a Client ready to pass to NewAggregator or New. It is a thin
// convenience over the SDK constructor; callers that need SDK-specific options
// should construct the SDK client directly, since *dogstatsd.Client satisfies
// Client structurally.
func Dial(addr string) (Client, error) {
	c, err := dogstatsd.New(addr)
	if err != nil {
		return nil, err
	}
	return c, nil
}
