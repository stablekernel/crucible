# Crucible documentation site

The Crucible docs are an [Astro] + [Starlight] site. Mermaid diagrams render
client-side from fenced ` ```mermaid ` blocks via [astro-mermaid].

## Generated content

Two parts of the site are **generated from the Go source at build time and are
not committed** (both paths are gitignored):

- `src/content/docs/reference/`: the API reference, rendered from each package's
  godoc with [gomarkdoc].
- `src/content/docs/_generated/`: Mermaid diagram partials rendered from the real
  example machines' `ToMermaid()` output.

The generator is the `tools/docsgen` Go module. It is run **from the repository
root** (not from `docs/`). CI and the Pages deploy workflow run it explicitly
before `astro build`, so the published reference and diagrams are always fresh
and can never drift from the source.

## Images

Brand and illustration art lives in `src/assets/` (logo, hero, mascot, and one
image per in-page slot) and `public/` (favicon, social card). Each in-page
illustration slot is marked in the content with an `IMAGE-SLOT: <slug>` comment,
and the matching `<slug>.png` sits alongside it in `src/assets/`. The prompt
manifest (the shared visual style guide and a ready-to-use prompt for each slot)
is maintained outside this repo; regenerate from it and replace the matching file
to refresh any image.

## Local development

From the repository root:

```sh
# 1. Generate the reference pages and diagram partials.
go run ./tools/docsgen

# 2. Start the dev server (from this directory).
cd docs
npm install   # first time only
npm run dev
```

`npm run build` deliberately does **not** depend on Go: the Node build works as
long as the generated content is already present (CI runs `docsgen` as a
separate step). If you build without running `docsgen` first, the Reference
section and the embedded order diagram will be missing or stale. Run the
generator, then rebuild.

> Tip: re-run `go run ./tools/docsgen` whenever you change a documented package's
> godoc comments or an example machine. It is idempotent: running it twice
> yields identical output.

[Astro]: https://astro.build
[Starlight]: https://starlight.astro.build
[astro-mermaid]: https://www.npmjs.com/package/astro-mermaid
[gomarkdoc]: https://github.com/princjef/gomarkdoc
