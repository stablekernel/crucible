# Third-Party Notices

The `crucible` command-line tool (`cmd/crucible`) embeds third-party software so
that `crucible render -format svg` can produce diagrams without requiring an
external Graphviz installation. The crucible `state` engine and its seams do
**not** depend on any of the software listed here; these notices apply only to
the CLI binary.

This file is informational. It is **not** a substitute for the licenses shipped
with each dependency, which remain authoritative.

---

## oss.terrastruct.com/d2

The D2 diagramming engine. Used by the CLI to lay out and render the machine
diagram to SVG in-process — pure Go, with no Chromium and no external Graphviz
install. The distributed `crucible` binary therefore contains D2.

- License: Mozilla Public License, Version 2.0 (MPL-2.0)
- Project: https://github.com/terrastruct/d2
- License text: https://www.mozilla.org/en-US/MPL/2.0/ (and the `LICENSE`
  shipped within the `oss.terrastruct.com/d2` module)

MPL-2.0 is a file-level copyleft license. The full license text is authoritative
and is shipped with the module in the Go module cache.

---

## Transitive dependencies

D2 pulls in supporting libraries during layout and SVG rendering. Their licenses
are reproduced in their respective module directories in the Go module cache and
recorded in `cmd/crucible/go.sum`. Notable entries:

- `github.com/dop251/goja` — MIT (JavaScript engine used by D2 layout)
- `oss.terrastruct.com/util-go` — MPL-2.0
- `github.com/alecthomas/chroma/v2` — MIT
- `github.com/PuerkitoBio/goquery` — BSD-3-Clause
- `github.com/lucasb-eyer/go-colorful` — MIT
- `github.com/golang/freetype` — FreeType License / GNU GPL (dual)
- `golang.org/x/image`, `golang.org/x/net`, `golang.org/x/text` — BSD-3-Clause
