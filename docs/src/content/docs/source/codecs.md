---
title: Codecs and headers
description: An instance-scoped codec registry decodes bytes into typed events with no global registration; the cloudevents codec adds structured and binary content modes.
sidebar:
  order: 6
---

<!-- IMAGE-SLOT: source-codec-registry -->

A message arrives as bytes. A codec turns those bytes into the typed event a
handler wants, and resolves the generic `T` for a `TypedHandler[T]`.

## Instance-scoped, never global

The registry is **constructed and injected**, never a package-level singleton.
This deliberately avoids the global-registry anti-pattern: two consumers in the
same process never share decode state, and a codec is selected per message by its
content-type or header.

```go
reg := source.NewCodecRegistry()
source.RegisterCodec[OrderPlaced](reg, jsonCodec) // resolves T for the typed handler
```

JSON, proto, and Avro are the built-in shapes; CloudEvents lives in its own
module. Headers are **typed accessors over extension attributes**, not a
`map[string]string` of magic strings, so a missing or mistyped attribute is a
typed lookup, not a silent empty string.

## cloudevents

`crucible/source/cloudevents` is the CloudEvents codec. It supports both content
modes from the spec:

- **structured** mode, where the whole event (envelope plus data) is one encoded
  body, and
- **binary** mode, where the event attributes ride in headers and the data is the
  raw body.

The two content modes are the codec seam itself, so the same handler works
whichever mode a producer chose. Because the registry is instance-scoped, the
CloudEvents codec never registers itself globally, and a service can run a
CloudEvents consumer and a plain-JSON consumer side by side without interference.

The decoded type then flows into your handler, and for the
[state-machine binding](/crucible/source/with-state/) it becomes the event the
router fires. A decode failure is a typed `*DecodeError` the engine classifies
as poison and routes to the [DLQ](/crucible/source/reliability/#dlq).

## cdc

`crucible/source/cdc` is the change-data-capture codec. It decodes the standard
Debezium JSON change-event envelope (also the de-facto OpenCDC normalized record
shape) into a typed `ChangeEvent`:

- an `Operation` (create, snapshot read, update, delete, or tombstone),
- the `before` and `after` row images, kept as deferred JSON so one codec serves
  every table on a topic without binding to a row type at decode time,
- a decoded `Source` metadata block (connector, database, schema, table,
  snapshot marker, log position, transaction id), and
- the commit `Timestamp` the connector reported.

Recover the value with `DecodeEvent`, project a row image into a concrete type
with `BeforeAs[T]` / `AfterAs[T]`, and read the source metadata as typed
`source.Headers` through `SourceHeaders`. A log-compaction tombstone (an empty
payload) decodes to an `OpTombstone` event rather than a decode failure, so a
handler routes it (a delete-and-forget for the key) or skips it. A malformed
envelope is a typed `*DecodeError` the engine classifies as poison.

### Scope: envelope plus topic pattern, not a native connector

This codec covers the change-event **envelope** and the pattern for driving a
statechart from a change-event **topic**. The intended shape is to let an
existing connector (Debezium, or any producer emitting the same envelope) write
row changes to a topic, consume that topic through a backend inlet such as
[`source/kafka`](/crucible/source/inlets/), decode each message with this codec,
and drive a statechart per primary key through the
[state-machine binding](/crucible/source/with-state/). Because the codec is
instance-scoped, a service can run a CDC consumer alongside a plain-JSON or
CloudEvents consumer with no shared decode state.

A native database write-ahead-log connector (reading a Postgres logical
replication slot or a MySQL binlog directly, without a broker in between) is
deliberately out of scope and tracked as future work. The codec gives you the
typed change event; the connector that produces those events stays a separate,
operational concern.

```go
// Decode a Debezium change event, then route its after-image into a transition.
registry := source.NewRegistry().SetDefault(cdc.New())

router := func(m source.Message) (Key, Event, error) {
	change, err := cdc.DecodeEvent(registry, m)
	if err != nil {
		return zeroKey, zeroEvent, err
	}
	row, err := cdc.AfterAs[Row](change)
	if err != nil {
		return zeroKey, zeroEvent, err
	}
	return keyOf(row), eventOf(change.Operation, row), nil
}
```
