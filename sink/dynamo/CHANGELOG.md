# Changelog

All notable changes to `crucible/sink/dynamo` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- DynamoDB sink destination: a narrow `Client` interface (`PutItem`,
  `UpdateItem`, `DeleteItem`, `TransactWriteItems`, `BatchWriteItem`, satisfied
  by `*dynamodb.Client`), the `PutItem`, `UpdateItem`, `DeleteItem`,
  `TransactWrite`, and `BatchWrite` operation constructors, `NewRegistry`, and
  `New` building a `sink.Outlet`.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/dynamo
