# Changelog

All notable changes to `crucible/sink/file` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- JSONL file sink destination: `New(io.Writer)` for wrapping any writer and
  `Open(path)` for owning an append-only file. Implements `sink.Outlet`,
  `sink.Flusher` (Sync on `*os.File`), and `sink.Shutdowner` (Close for
  owned files). Safe for concurrent use; accepts all payload types without a
  registry.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/file
