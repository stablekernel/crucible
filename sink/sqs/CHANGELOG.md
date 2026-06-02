# Changelog

All notable changes to `crucible/sink/sqs` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Amazon SQS sink destination: a narrow `Client` interface (`SendMessage`,
  `SendMessageBatch`, satisfied by `*sqs.Client` from
  `github.com/aws/aws-sdk-go-v2/service/sqs`), three Op constructors
  (`SendMessage`, `SendMessageFrom`, `SendMessageBatchOp`), `NewRegistry`, and
  `New` building a `sink.Outlet`. `SendMessageBatchOp` surfaces partial-batch
  failures (SQS HTTP-200 with `out.Failed` entries) as errors so callers do
  not need to inspect the SDK output.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/sqs
