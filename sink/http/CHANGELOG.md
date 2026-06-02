# Changelog

All notable changes to `crucible/sink/http` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `net/http` sink destination: a narrow `Doer` interface (`Do`, satisfied by
  `*http.Client`), `Post` and `PostJSON` operation constructors, `NewRegistry`,
  and `New` building a `sink.Outlet` with no third-party dependency.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/http
