# crucible

A headless command-line tool for the crucible state-machine IR. It lints,
renders, diffs, validates, and ejects a machine's serialized IR JSON without
running any behavior. Every command reads an IR JSON file path, or `-` for
stdin.

## Behavior-free operation

A serialized IR carries behavior references by name only; the kernel binds those
names to real implementations when it assembles a machine. Commands that need an
assembled machine (`lint`, `render`, `validate`) do not have the host's real
guards, actions, reducers, and services. They register a deterministic no-op stub
for every referenced name first, so the machine assembles from its structure
alone. The stubs never run during these commands (no instance is cast and no
event is fired), so the structural view is exactly what the IR describes.

## Commands

### lint

```
crucible lint <ir.json> [-format text|json|sarif]
```

Runs every static analysis check and prints the findings. Exits non-zero when
the analysis reports any finding, so it can gate CI. `-format` selects the
output: human-readable `text` (the default), `json`, or `sarif` (SARIF 2.1.0)
for ingestion by code-scanning tools. SARIF findings carry the IR path as a
physical location unless the IR was read from stdin (`-`).

### render

```
crucible render <ir.json> [-format mermaid|dot]
```

Renders the machine as a Mermaid `stateDiagram-v2` (the default) or as Graphviz
DOT. Output is text. For an SVG, pipe the DOT through Graphviz (`crucible render
m.json -format dot | dot -Tsvg`); native SVG rendering is a future addition.

### diff

```
crucible diff <old.json> <new.json> [-format text|json] [-exit-code]
```

Classifies the changes between two serialized IRs, prints the recommended semver
bump (`major`, `minor`, or `patch`), and lists the breaking and additive changes
separately. `-format` selects human-readable `text` (the default) or `json`
(SARIF is not applicable to diffs). With `-exit-code`, the command exits non-zero
when the recommended bump is `major` (at least one breaking change), so a diff
can gate CI.

### validate

```
crucible validate <ir.json>
```

Confirms the IR loads and assembles cleanly. A malformed JSON document or a
structural defect the lint rejects exits non-zero with a message on stderr; a
clean machine exits zero.

### eject

```
crucible eject <ir.json> [-package name] [-o outfile]
```

Generates typed Go behavior stubs for every referenced guard, action, reducer,
and service, plus a `Provide` function that registers them. Writes to `outfile`
or, by default, to stdout.

### version

```
crucible version
crucible -version
```

Prints the CLI version.

## Exit codes

- `0` success
- `1` runtime or load error, lint findings, and `diff -exit-code` on a breaking change
- `2` usage error

## Versioning

The crucible CLI is versioned independently of the `state` module. It is not part
of the `state` v1 freeze, so it can move at its own pace.
