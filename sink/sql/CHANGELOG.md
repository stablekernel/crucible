# Changelog

All notable changes to `crucible/sink/sql` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `database/sql` sink destination: a narrow `Tx` interface (`ExecContext`,
  satisfied by `*sql.DB`/`*sql.Tx`/`*sql.Conn`), an `Exec` operation
  constructor, `NewRegistry`, and `New` building a `sink.Outlet` with no driver
  dependency.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/sql
