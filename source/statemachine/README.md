# source/statemachine

Binds an inbound [`crucible/source`](../) message to a
[`crucible/state`](../../state) statechart, so consuming a message *is* firing a
transition and the ack is tied to a durable transition. The ingress mirror of
[`sink/bridge`](../../sink/bridge): a separate module depending on both cores, so
neither imports the other.

```go
machine := state.Forge[S, E, C]("order"). /* ... */ Quench(state.Strict())
store := statemachine.NewMemStore[K, S, E, C]() // or a durable adapter

h := statemachine.Drive[K, E, C](machine, store,
    func(m source.Message) (K, E, error) {
        order, err := source.DecodeTyped[Order](registry, m)
        return order.ID, order.Event, err
    },
    statemachine.WithSink(sink), // hand emitted effects to a publisher
)

sub.Receive(ctx, h) // each message: route → load → Fire → emit → persist → ack
```

The handler runs one declared step:

    decode → route to (key, event) → load instance → Fire(event) →
        emit effects → persist new state → ack

The ack comes only after a successful durable `Store.Save`
(ack-after-durable-commit).

## Differentiators

- **Exactly-once into the machine.** The persisted instance carries a monotonic
  version and the id of the last applied message. A redelivered `(key, eventID)`
  already folded into the version returns `source.Skip` — acked, never re-fired —
  so redelivery is provably idempotent with no external dedup store. Make the id
  extractor `WithEventID`.
- **State-aware rejection.** A `Fire` rejected because the event is illegal for
  the current state (no transition, or a failing guard/`Assay`) returns
  `source.Reject` (Term, `InvalidForState`) carrying a `*source.GuardRejection` —
  distinct from a transient `Store`/infra error, which returns `source.Nak`
  (Retryable).
- **consume → transition → emit.** A transition's emitted effects are handed to
  an injected `Sink` in the same step, before the ack.
- **Analyzable consumption.** `CheckEvents` validates that a router's event union
  is exhaustive against the machine's event alphabet and reports inbound events no
  state can ever handle.

## Modes

- **Durable** — `Drive` loads and saves each instance through a `Store`,
  persisting the transition before acking. The exactly-once path.
- **Stateless** — `DriveFunc` fires each message against a caller-supplied
  function with no persistence, for a transient or externally-owned machine.

## Store coupling

The bridge depends only on the small `Store` interface, never on a concrete
durable backend; `crucible/durable` (or any store) can supply an adapter.
`NewMemStore` is an in-memory `Store` for tests and single-process use.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
