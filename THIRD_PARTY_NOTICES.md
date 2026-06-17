# Third-Party Notices

The `crucible` command-line tool (`cmd/crucible`) embeds third-party software so
that `crucible render -format svg|png` can produce images without requiring an
external Graphviz installation. The crucible `state` engine and its seams do
**not** depend on any of the software listed here; these notices apply only to
the CLI binary.

This file is informational. It is **not** a substitute for the licenses shipped
with each dependency, which remain authoritative.

---

## github.com/goccy/go-graphviz

Pure-Go bindings that run Graphviz compiled to WebAssembly via
[wazero](https://github.com/tetratelabs/wazero). Used by the CLI to render DOT
to SVG/PNG in-process.

- License: MIT
- Project: https://github.com/goccy/go-graphviz

```
MIT License

Copyright (c) 2020 Masaaki Goshima

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## Graphviz (bundled as WebAssembly by go-graphviz)

`go-graphviz` embeds the [Graphviz](https://graphviz.org/) graph-layout and
rendering engine compiled to WebAssembly. The distributed `crucible` binary
therefore contains Graphviz.

- License: Eclipse Public License, Version 1.0 (EPL-1.0)
- Project: https://graphviz.org/
- License text: https://graphviz.org/license/ (and the `LICENSE` shipped within
  the go-graphviz module under `vendor`/embedded WebAssembly assets)

Graphviz is distributed under the Eclipse Public License, Version 1.0. The full
license text is available at https://www.eclipse.org/legal/epl-v10.html. The
program source for the embedded Graphviz is available from the Graphviz project
at https://gitlab.com/graphviz/graphviz.

---

## Transitive image-encoding dependencies

`go-graphviz` pulls in pure-Go image-encoding libraries used during PNG/JPEG
rendering. Their licenses are reproduced in their respective module directories
in the Go module cache and in `cmd/crucible/go.sum`:

- `github.com/tetratelabs/wazero` — Apache License 2.0 (WebAssembly runtime)
- `github.com/disintegration/imaging` — MIT
- `github.com/fogleman/gg` — MIT
- `github.com/golang/freetype` — FreeType License / GNU GPL (dual)
- `github.com/flopp/go-findfont` — MIT
- `golang.org/x/image`, `golang.org/x/text` — BSD-3-Clause
