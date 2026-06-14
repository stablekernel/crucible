module github.com/stablekernel/crucible/source/cloudevents

go 1.25.11
toolchain go1.26.4
require (
	github.com/cloudevents/sdk-go/v2 v2.16.2
	github.com/stablekernel/crucible/source v0.0.0
)

require (
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/stablekernel/crucible/telemetry v0.0.0 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
)

replace github.com/stablekernel/crucible/source => ../

replace github.com/stablekernel/crucible/telemetry => ../../telemetry
