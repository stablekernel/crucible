# sourcedrive

The flagship example for `crucible/source`: **consume a Kafka topic, drive a
statechart, ack on the durable transition.**

Consuming a message *is* firing a transition. The ack is tied to the durable
state change, so the things only a state-machine-native ingress can offer fall
out for free:

- **Exactly-once into the machine.** Redelivery of an already-applied event is a
  no-op ack, keyed on the persisted state version. No external dedup store.
- **State-aware rejection.** An event that is illegal for the current state (a
  failing guard, or no declared transition) terminates as poison — not an
  endless retry loop.
- **Ack-after-durable-commit.** A transient store or broker error nak's for
  redelivery; the stream never advances past an unpersisted transition.

## The shipment lifecycle

```
pending --pay[funded]--> shipped --deliver--> delivered
```

`pay` is guarded by `funded`, so an unfunded `pay` is rejected as
invalid-for-state. `deliver` from `pending` has no transition. Both are
poison-by-state, distinct from a transient infrastructure error.

## How it is wired

| Piece | Role |
| --- | --- |
| `NewFulfillment` | Forges the statechart and binds it to a `statemachine.MemStore` through `statemachine.Drive`, yielding a `source.Handler`. |
| `Run` | Drives any `source.Inlet` through that handler with a `source.Hopper`. The unit test passes an in-memory `memsource.Inlet`. |
| `RunKafka` | Constructs a `source/kafka.Inlet` over real seed brokers and hands it to `Run`. |
| `cmd/sourcedrive` | The runnable program; calls `RunKafka`. |

The consume loop is split from the broker so the differentiator is unit-testable
with zero infra: `sourcedrive_test.go` exercises the happy path, idempotent
redelivery, state-aware rejection, and undecodable-payload poison entirely
in-process.

## Run it against a broker

```sh
go run ./cmd/sourcedrive -brokers localhost:9092 -topic fulfillment -group sourcedrive
```

Produce JSON commands keyed by shipment id, for example a record keyed `ship-1`
with body `{"op":"pay"}`. The program logs each applied transition.

## Tests

```sh
# In-process, no broker:
GOWORK=off go test -race ./...

# Against a throwaway RedPanda broker (Docker required):
GOWORK=off go test -tags integration -race ./...
```

This module lives outside the Go workspace (its own `go.mod` with `replace`
directives, built `GOWORK=off`) because it pulls the franz-go Kafka SDK in
through `source/kafka`, which the workspace keeps out of its dependency graph.
