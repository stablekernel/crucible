---
title: Effects and purity
description: Fire returns state, effects, and a trace as data — no IO. The host dispatches the effects.
sidebar:
  order: 4
---

`Fire` does not talk to the outside world. It takes the current instance and an
event, computes the next state, and returns everything that happened as **data**:

```go
type FireResult[S comparable] struct {
    NewState S
    Effects  []Effect
    Trace    Trace
    Err      error
}
```

No effect is *performed* inside `Fire`. If a transition's action says "publish an
`OrderPaid` message" or "charge the card", `Fire` records that intent as an
`Effect` in the result and returns. The **host** — your handler, consumer, or
test — inspects `res.Effects` and dispatches them:

```go
res := inst.Fire(ctx, Pay)
if res.Err != nil {
    return res.Err
}
for _, eff := range res.Effects {
    if err := dispatch(ctx, eff); err != nil { // publish / store / RPC
        return err
    }
}
// res.NewState is now committed; res.Trace explains how we got there.
```

## One machine, many consumers

Because the machine performs no IO, the *same* `*Machine` runs unchanged in
wildly different hosts:

- **A unit test** fires events and asserts on `NewState` and `Effects` — no
  brokers, no databases, no mocks of the kernel.
- **An HTTP handler** fires the event for an incoming request, then dispatches
  the effects to its real publishers and stores.
- **An event consumer** fires the event off the wire and dispatches effects back
  onto the bus.

The decision of *what* should happen lives in the machine; the decision of *how*
to make it happen lives in the host. That separation is what keeps the kernel
pure, the behavior testable, and the same definition portable across every place
it runs.

The `Trace` rounds this out: it is an ordered record of the transitions, guards,
and regions that `Fire` walked, so you can explain, log, or replay any step
after the fact — again, entirely as data.
