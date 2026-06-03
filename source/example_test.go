// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/memsource"
)

// orderPlaced is a sample domain event arriving on a stream.
type orderPlaced struct {
	ID  string `json:"id"`
	Qty int    `json:"qty"`
}

// ExampleHopper shows the core loop: an in-memory inlet feeds scripted messages
// to a Hopper, which decodes each with a JSON codec, runs the handler, and
// settles by the returned Result.
func ExampleHopper() {
	in := memsource.New(memsource.WithMessages(
		memsource.Msg{Key: "A", Value: []byte(`{"id":"A","qty":2}`)},
		memsource.Msg{Key: "B", Value: []byte(`{"id":"B","qty":5}`)},
	))

	hp := source.New(source.WithCodec(source.NewJSONCodec[orderPlaced]()))

	sub, _ := in.Subscribe(context.Background(), source.SubscribeConfig{Topics: []string{"orders"}})
	_ = sub.Close() // drain once the two queued messages settle

	_ = hp.Run(context.Background(), sub, func(_ context.Context, m source.Message) source.Result {
		v, _ := source.Decoded(m)
		o := v.(orderPlaced)
		fmt.Printf("order %s qty %d\n", o.ID, o.Qty)
		return source.Ack()
	})
	// Output:
	// order A qty 2
	// order B qty 5
}

// ExampleChain shows middleware composition: the first middleware listed is the
// outermost, so a message flows outer to inner and the result returns inner to
// outer.
func ExampleChain() {
	tag := func(label string) source.Middleware {
		return func(next source.Handler) source.Handler {
			return func(ctx context.Context, m source.Message) source.Result {
				fmt.Println("enter", label)
				return next(ctx, m)
			}
		}
	}
	base := func(context.Context, source.Message) source.Result {
		fmt.Println("handle")
		return source.Ack()
	}

	h := source.Chain(base, tag("outer"), tag("inner"))
	_ = h(context.Background(), nil)
	// Output:
	// enter outer
	// enter inner
	// handle
}

// ExampleRegistry_Decode shows content-type routing: a registry selects a codec
// by the message's content-type header, falling back to its default otherwise.
func ExampleRegistry_Decode() {
	reg := source.NewRegistry().
		Register("application/json", source.NewJSONCodec[orderPlaced]())

	in := memsource.New()
	in.Queue(memsource.Msg{
		Value:   []byte(`{"id":"Z","qty":1}`),
		Headers: source.Headers{{Key: "content-type", Value: "application/json"}},
	})
	sub, _ := in.Subscribe(context.Background(), source.SubscribeConfig{})
	m, _ := sub.Next(context.Background())

	v, _ := reg.Decode(m)
	fmt.Println(v.(orderPlaced).ID)
	// Output: Z
}
