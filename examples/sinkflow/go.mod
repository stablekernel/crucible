module github.com/stablekernel/crucible/examples/sinkflow

go 1.25.0

require (
	github.com/stablekernel/crucible/sink v0.0.0
	github.com/stablekernel/crucible/sink/bridge v0.0.0
	github.com/stablekernel/crucible/state v0.0.0
	github.com/stablekernel/crucible/telemetry v0.0.0
)

replace github.com/stablekernel/crucible/sink => ../../sink

replace github.com/stablekernel/crucible/sink/bridge => ../../sink/bridge

replace github.com/stablekernel/crucible/state => ../../state

replace github.com/stablekernel/crucible/telemetry => ../../telemetry
