// SPDX-License-Identifier: Apache-2.0

// Package statemachine binds an inbound [source] message to a crucible/state
// statechart so that consuming a message *is* firing a transition, and the ack
// is tied to a durable transition. It is the ingress mirror of crucible/sink's
// bridge: a separate module depending on both crucible/source and
// crucible/state, so neither core imports the other.
//
// The binding is one declared step, not hand-wired plumbing:
//
//	decode → route to (instanceKey, event) → load instance → Fire(event) →
//	    hand emitted effects to a sink → persist new state → ack
//
// The ack comes only after a successful durable [Store.Save]
// (ack-after-durable-commit), so at-least-once delivery never advances the
// stream past an unpersisted transition.
//
// # Differentiators
//
// What only a state-machine-native ingress binding can own:
//
//   - Exactly-once into the machine. The persisted instance carries a
//     monotonic version, and [Drive] records the event id of the last applied
//     message. A redelivered (key, eventID) that was already applied returns
//     [source.Skip] — acked, never re-fired — so redelivery is provably
//     idempotent with no external dedup store. The machine's own state version
//     is the dedup key.
//   - State-aware rejection. A [state.Instance.Fire] that fails because the
//     event is illegal for the current state (no declared transition, or a
//     failing guard/[state.Machine.Verify]) returns [source.Reject]
//     (Term, classified InvalidForState) carrying a [*source.GuardRejection] —
//     distinct from a transient [Store] or infrastructure error, which returns
//     [source.Nak] (Retryable). "Wrong time" and "try again later" are
//     different first-class outcomes.
//   - consume → transition → emit. A transition's emitted effects
//     ([state.FireResult.Effects]) are handed to an injected [Sink] in the same
//     step, before the ack, so the statechart is the processor and emitted
//     effects are transition outputs.
//   - Analyzable consumption. [Conformance] validates that a router's event
//     union is exhaustive against the machine's event alphabet and reports
//     inbound events that no state can ever handle, at build or load time.
//
// # Modes
//
// Three binding modes share the same outcomes:
//
//   - Durable: [Drive] loads and saves each instance through a [Store],
//     persisting the transition before acking. Redelivery is deduplicated by the
//     persisted event id (exactly-once into the machine, at-least-once delivery).
//   - Transactional: [DriveTx] runs the durable path inside a
//     [source.Transactional] consume-process-produce transaction (Kafka EOS), so
//     the records a transition emits and the consumed offset commit as one atomic
//     unit. It is the exactly-once-into-a-sink path; use it only on a backend that
//     satisfies [source.Transactional].
//   - Stateless: [DriveFunc] fires each message against a caller-supplied
//     function with no persistence, for sources that drive a transient or
//     externally-owned machine.
//
// # Store coupling
//
// The bridge depends only on the small [Store] interface, never on a concrete
// durable backend; the crucible/durable module (or any store) can provide an
// adapter. [NewMemStore] is an in-memory [Store] for tests and single-process
// use.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package statemachine
