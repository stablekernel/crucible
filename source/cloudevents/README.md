# source/cloudevents

A [`crucible/source`](../) codec that decodes inbound messages into
[CloudEvents](https://cloudevents.io). Runtime dependency:
[`github.com/cloudevents/sdk-go/v2`](https://github.com/cloudevents/sdk-go).

```go
reg := source.NewRegistry().
    SetDefault(cloudevents.New()).                                    // binary mode by content type
    Register(cloudevents.StructuredContentType, cloudevents.New())    // structured JSON

ev, order, err := cloudevents.DecodeData[Order](reg, m)
```

Construct a `Codec` with `New` and register it on a `source.Registry` under the
content types you accept (or as the registry default). There is no package-level
format registration: every codec is instance-scoped, so two codecs in one
process never share mutable state.

## Content modes

The CloudEvents spec defines two ways an event rides a transport, and this codec
accepts both, selecting by the message's content type (see `Detect`):

- **Structured** — the whole event (attributes and data) is one JSON document in
  the body, carried under `application/cloudevents+json`
  (`StructuredContentType`).
- **Binary** — the event's attributes ride as `ce-`-prefixed headers and the data
  is the raw body, with the body's own media type in the `datacontenttype`
  header (or the message's content type).

A content type whose media type begins with `application/cloudevents` decodes as
structured; anything else decodes as binary.

## Decoded value

`Decode` yields a `cloudevents.Event` (the SDK's canonical event). Recover it
from a handler with `EventOf`, or in one call with `DecodeEvent`. Decode the data
payload into a concrete type with `DataAs`, or use the generic `DecodeData[T]` to
decode and project in one step. Extension attributes are surfaced through the
core `source.Headers` (see `Extensions`, prefixed with `ExtensionHeaderPrefix`)
so a handler reads them the same way it reads any other inbound metadata.

## Stability

Experimental (pre-v1); the API may change until the suite locks v1.0.0.
