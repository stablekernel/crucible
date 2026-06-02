module github.com/stablekernel/crucible/sink/sns

go 1.25.0

replace github.com/stablekernel/crucible/sink => ../

replace github.com/stablekernel/crucible/telemetry => ../../telemetry

require (
	github.com/aws/aws-sdk-go-v2/service/sns v1.39.19
	github.com/stablekernel/crucible/sink v0.0.0-00010101000000-000000000000
)

require (
	github.com/aws/aws-sdk-go-v2 v1.41.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.25 // indirect
	github.com/aws/smithy-go v1.26.0 // indirect
	github.com/stablekernel/crucible/telemetry v0.0.0 // indirect
)
