module github.com/stablekernel/crucible/sink/kinesis

go 1.25.0

require (
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.43.9
	github.com/stablekernel/crucible/sink v0.0.0
)

require (
	github.com/aws/aws-sdk-go-v2 v1.41.9 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.25 // indirect
	github.com/aws/smithy-go v1.26.0 // indirect
	github.com/stablekernel/crucible/telemetry v0.0.0 // indirect
)

replace github.com/stablekernel/crucible/sink => ../

replace github.com/stablekernel/crucible/telemetry => ../../telemetry
