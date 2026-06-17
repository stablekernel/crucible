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
crucible render <ir.json> [-format mermaid|dot|svg] [-o outfile] \
  [-from state] [-to state] [-mode shortest|all|trace] \
  [-detail outline|guards|actions|lifecycle|full] \
  [-show dim]... [-hide dim]... [-theme file.json]
```

Renders the machine as a diagram. `-format` selects the output:

- `mermaid` (the default) — a Mermaid `stateDiagram-v2`, streamed to stdout.
- `dot` — Graphviz DOT, streamed to stdout (the historical
  `crucible render m.json -format dot | dot -Tsvg` pipeline still works for
  callers who prefer their own Graphviz).
- `svg` — a themed, scalable SVG rendered in-process by the embedded D2 engine
  (pure Go, no Chromium and no external Graphviz install). The SVG carries the
  Crucible forge palette.

There is no `png` format: `-format png` exits with a usage error pointing you
at the conversion path. SVG is the scalable raster-free output; for a PNG,
render `-format svg` and convert it, e.g.:

```
crucible render m.json -format svg -o m.svg
resvg m.svg m.png        # recommended
# or: rsvg-convert m.svg -o m.png
```

`-o` writes the output to a file instead of stdout; it is the norm for `svg`.

#### Scope and detail

The SVG pipeline projects the machine along two independent axes: **scope**
(how much of the graph to keep) and **detail** (how much of each
state/transition to show).

**Scope** is chosen from `-from`/`-to`/`-mode`:

- No `-from`: the **whole** machine.
- `-from A` only: the subgraph **reachable from A**.
- `-from A -to X`: a **path** from A to X. `-mode` shapes it:
  - `shortest` (default) keeps the whole reachable subgraph but highlights the
    single shortest A→X path (off-path elements stay, dimmed).
  - `all` keeps the union of all simple A→X paths, all highlighted.
  - `trace` keeps **only** the shortest A→X path, dropping everything else.

`-to` requires `-from`; a non-default `-mode` requires `-from`. Endpoints are
bare state names (composite names resolve to their region).

**Detail** is a cumulative ladder set by `-detail` (default `actions`); each
level implies all the levels below it:

| Level       | Adds                                              |
|-------------|---------------------------------------------------|
| `outline`   | states and transitions only                       |
| `guards`    | + transition guards                               |
| `actions`   | + effects and assigns (the default)               |
| `lifecycle` | + entry/exit actions and invocations              |
| `full`      | + delays, descriptions, data-flow, context schema, source |

`-show <dimension>` and `-hide <dimension>` (both repeatable) override the
ladder per dimension; `-show` wins when both name the same one. Dimensions:
`guards`, `effects`, `assigns`, `entry-exit`, `invoke`, `delays`,
`descriptions`, `data-flow`, `context-schema`, `source`.

#### Theme

`-theme file.json` overlays a JSON theme onto the embedded default forge
palette; fields you omit keep their defaults. Without `-theme`, the embedded
default theme is used.

#### Examples

```
# Whole machine, default detail, as Mermaid (to stdout):
crucible render m.json

# Just the shortest path from cart to done, nothing else, as SVG:
crucible render m.json -format svg -mode trace -from cart -to done -o path.svg

# Reachable-from-A view with full detail but guards suppressed:
crucible render m.json -format svg -from active -detail full -hide guards -o active.svg
```

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

### simulate

```
crucible simulate <ir.json> -events e1,e2 [-events-file f] [-initial S] [-guard name=bool] [-format text|json]
```

Fires a sequence of events against a machine assembled from the IR and prints the
resulting step trace (each event's from/to state, outcome, and emitted effects),
then the final state. The events come from `-events` (a comma-separated list) or
`-events-file` (a JSON file that is either a bare array of event names or a
conformance scenario object); exactly one is required. Since the IR carries no
real behavior, guards return seeded verdicts: `-guard name=bool` (repeatable)
sets a guard's result, and any unseeded guard defaults to `false`; actions,
reducers, and services are no-ops. `-initial` overrides the IR's declared start
state. `-format` selects human-readable `text` (the default) or `json`. A
guard-blocked or invalid transition is a normal observable outcome and exits
zero; an unknown event or an action failure exits non-zero.

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
