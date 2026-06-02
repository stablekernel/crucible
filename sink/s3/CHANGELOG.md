# Changelog

All notable changes to `crucible/sink/s3` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Amazon S3 sink destination: a narrow `Client` interface (`PutObject`,
  `DeleteObject`, satisfied by the real `*s3.Client`), four operation
  constructors (`PutObject`, `PutObjectWith`, `DeleteObject`,
  `DeleteObjectWith`), `NewRegistry`, and `New` building a `sink.Outlet`
  backed by the AWS SDK v2 (`github.com/aws/aws-sdk-go-v2/service/s3`).

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/s3
