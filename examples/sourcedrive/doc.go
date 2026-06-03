// SPDX-License-Identifier: Apache-2.0

// Package sourcedrive is the flagship example for crucible/source: consume a
// Kafka topic, decode each message into a domain event, drive a crucible
// statechart instance keyed by the message key, persist the transition, and ack
// only after the transition is durable.
//
// It is the killer feature made runnable. Consuming a message *is* firing a
// transition; the ack is tied to the durable state change, so:
//
//   - redelivery of an already-applied event is a no-op ack (exactly-once into
//     the machine, keyed on the persisted state version, no external dedup
//     store);
//   - an event that is illegal for the current state terminates as poison
//     (Term, InvalidForState) instead of looping on retry; and
//   - a transient store or broker error nak's for redelivery, never advancing
//     the stream past an unpersisted transition.
//
// # Layout
//
// The wiring is split so the differentiator is unit-testable with zero infra:
//
//   - [Fulfillment] forges the statechart and exposes the [source.Handler] the
//     source/statemachine bridge produces, over a statemachine.MemStore.
//   - [Run] drives any [source.Inlet] through that handler with a [source.Hopper].
//     The example test wires it to an in-memory memsource.Inlet, so the whole
//     consume → decode → Fire → persist → ack loop runs without a broker.
//   - [RunKafka] is the broker-touching entrypoint: it constructs a
//     source/kafka.Inlet over real seed brokers and hands it to [Run]. The
//     cmd/sourcedrive program calls it.
//
// # Running against a broker
//
// Point cmd/sourcedrive at a Kafka or RedPanda cluster:
//
//	go run ./cmd/sourcedrive -brokers localhost:9092 -topic fulfillment -group sourcedrive
//
// then produce JSON commands keyed by shipment id, for example a message keyed
// "ship-1" with body {"op":"pay"}. The program logs each transition and the ack
// that followed its durable persist.
package sourcedrive
