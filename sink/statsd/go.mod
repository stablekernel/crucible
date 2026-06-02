module github.com/stablekernel/crucible/sink/statsd

go 1.25.0

require (
	github.com/DataDog/datadog-go/v5 v5.8.3
	github.com/stablekernel/crucible/sink v0.0.0
)

require (
	github.com/Microsoft/go-winio v0.5.0 // indirect
	github.com/stablekernel/crucible/telemetry v0.0.0 // indirect
	golang.org/x/sys v0.0.0-20210510120138-977fb7262007 // indirect
)

replace github.com/stablekernel/crucible/sink => ../

replace github.com/stablekernel/crucible/telemetry => ../../telemetry
