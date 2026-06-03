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
